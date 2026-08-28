//! The Typed Effect Log's two stores: [`MemEffectLog`] in memory, [`DurableEffectLog`] on a disk.
//!
//! Design authority: DESIGN.md section 3 ("Log format"). `DISTRIBUTED.md` §F9 is why the durable
//! one exists: merges are computed *from* these frames, so a promoted leader that holds none of its
//! predecessor's effect log cannot recompute the merge its predecessor was evaluating, and no
//! follower can verify a merge it did not host.
//!
//! # The re-append contract, which both stores share and neither may weaken
//!
//! The log is keyed by `(branch, txn_id)`, and a second append under a key that is already
//! present is one of exactly three things. Telling them apart is the whole contract:
//!
//! - **A retry** — the identical frame arrives again. Stored once. `OpKind::Add` is not
//!   idempotent, so a log that keeps a replayed frame twice has already lost.
//! - **Growth** — the stored frame is a *prefix* of the new one. An agent task accumulates ops
//!   across several statements under one `TxnId` and re-appends the open frame after each
//!   (`agent_sql::AgentRuntime::stage_all` does exactly this, at one `append` per statement), so
//!   the frame legitimately grows. Accepted: the stored frame keeps what it holds and gains the
//!   new tail.
//! - **A contradiction** — anything else: ops that differ, ops that were dropped, a changed base
//!   or seq. That is two different transactions wearing one id. Hard error, never a silent
//!   overwrite; one of the two is a bug and picking a winner would hide it.
//!
//! Growth is defined as extension rather than "contents differ, take the newer" precisely so the
//! third case stays detectable. A truncating or rewriting append is not an extension and still
//! fails, which is what keeps this from degrading into last-writer-wins.
//!
//! [`classify`] is that decision, as one function. **Both stores call it**, so the rule cannot be
//! one thing in memory and another on the disk — which is not a tidiness point: the durable store
//! writes only what `classify` calls new, and if it disagreed with the store it indexes, the file
//! would hold a frame nobody ever accepted.
//!
//! Merge de-duplicates by `TxnId` again anyway ([`crate::tel::engine::dedup_by_txn`]) because
//! frames also arrive from elsewhere — a frame already merged once can reach a later merge from
//! both sides. Neither check makes the other redundant. **And the de-dup cannot stand in for this
//! one**: it keys on `TxnId`, so it collapses two frames wearing one id and is blind to *one*
//! frame carrying an op twice, which is exactly what a naive append-only file replays into.
//!
//! # `DurableEffectLog`'s file: growth is appended as a delta, not as a whole new frame
//!
//! ```text
//! header:  magic(4) | version(4) | crc32(4)                       -- 12 bytes, checksummed
//! record:  total_len(4) | tag(1) | body | crc32(4)                -- crc over everything before it
//!
//! tag 1  FrameOpen    key(20) | base(32) | seq(8) | schema_ver(4) | ops | guards | claims
//! tag 2  FrameExtend  key(20) | prior_ops(4) | prior_guards(4) | prior_claims(4)
//!                             | ops | guards | claims
//!
//! key = branch.id(8) | branch.generation(4) | txn_id(8)
//! ```
//!
//! The framing is [`crate::provenance::durable`]'s, which is [`crate::wal::log`]'s: a
//! length-prefixed record with a trailing CRC32, appended and fsynced one at a time, replayed from
//! the start on open, with the scan stopping at the first record that fails any of its checks. A
//! snapshot file would have to be rewritten whole on every statement and would lose the previous
//! copy to a torn write; an append that is torn loses only its own tail.
//!
//! **A `FrameExtend` carries only the new tail, and that is the load-bearing choice.** The naive
//! shape — re-append the whole frame each time and let the reader collapse the copies — is
//! quadratic in the file (a task of *n* statements writes *n(n+1)/2* op copies) and, far worse, it
//! puts two copies of every early op on the disk and makes correctness depend on a reader
//! collapsing them. Deltas remove both problems at once:
//!
//! * a **retry** writes *nothing at all* — `classify` says `Retry`, and there is no new tail, so
//!   the file never holds a second copy of the frame and no reader has to notice that it does;
//! * a **growth** writes only what grew, so the file is linear in the ops the task actually ran;
//! * every op reaches the file **exactly once**, so replaying the file exactly once reproduces
//!   every op exactly once. The Cassandra counter trap
//!   ([`crate::tel::ids::TxnId`]: two identical `qty -= 5` compose to −10) is not defended against
//!   here, it is unrepresentable — there is no second copy to double.
//!
//! `prior_ops`/`prior_guards`/`prior_claims` say how much the writer had already stored when it
//! wrote the delta. Replay checks them against what it has accumulated, so a record that is
//! **missing or duplicated in the middle of the file is visible by arithmetic** rather than being
//! silently concatenated into a frame with the wrong ops — the same reason `consensus::log` stamps
//! each frame with the round its position implies.
//!
//! **Stated gap: the directory entry is not fsynced.** The header and every record are, so a record
//! whose append returned `Ok` is on the device. But on a filesystem that can lose a newly created
//! file's directory entry across a power cut, the *first* append of a brand-new log can go with the
//! entry, and the reopened database then reports no frames rather than refusing. The repo's only
//! directory fsync is `storage::atomic_file::OsFileOps::sync_dir`, which is reachable from a
//! temp-then-rename and not from an append, and whose own documentation records the Windows arm as a
//! real gap — a directory handle there needs `FILE_FLAG_BACKUP_SEMANTICS`, which this crate has no
//! dependency to ask for (see `872a7d9`, where a hand-rolled directory fsync was `ERROR_ACCESS_
//! DENIED` on the Windows runner). `provenance::durable` and `wal::log` have the same gap. Named
//! here rather than left for a reader to discover.
//!
//! # What a torn tail does, and what damage elsewhere does
//!
//! A torn tail is truncated back to the last good byte and the number of discarded bytes is
//! **reported** through [`DurableEffectLog::discarded_tail_bytes`]. A store that silently swallowed
//! a partial write would answer with a frame short of the ops it had been told about, and a merge
//! computed from it would be confidently wrong.
//!
//! **The scan stops at the first record that fails, and everything after it is discarded too.**
//! That is the WAL's behaviour and it is deliberate rather than a shortcut: the record that failed
//! might have been a `FrameExtend`, and skipping it to keep what follows would replay a frame short
//! of the ops it ran — the counter bug, arrived at by a repair. So a garbled record in the middle of
//! the file costs the records after it, and the byte count is reported rather than swallowed so an
//! operator knows how much went.
//!
//! Damage that is *not* at the tail is refused rather than healed, because every repair would mean
//! inventing effects: a `FrameExtend` for a key the file never opened, a second `FrameOpen` for a
//! key it already did, a delta whose prior counts do not line up, an unknown tag, trailing bytes
//! after a record's fields, a header whose CRC does not hold. Each of those means the file
//! disagrees with itself, and guessing which half is right would produce a merge nobody can audit.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::branch::types::{BranchId, CommitHash};
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::storage::storage::Storage;
use crate::tel::frame::TxnFrame;
use crate::tel::guard::{ArithOp, CmpOp, Guard, GuardExpr};
use crate::tel::ids::{ColId, Dot, RowId, TableId, TxnId};
use crate::tel::op::{Delta, EscrowClaim, Op, OpKind};
use crate::tel::EffectLog;
use crate::wal::log::{
    crc32, pread_all, pwrite_all, take_array, take_str, take_u32, take_u64, take_u8,
    write_str,
};

/// An `EffectLog` held in memory. Durable enough for a branch that never outlives the process,
/// which — given non-cooperative lease reaping — is the common case for an agent task.
///
/// It is also [`DurableEffectLog`]'s index, which is why it is not merely a stand-in: every guard
/// it enforces applies there unchanged, and there is one implementation of the re-append rule
/// rather than two.
#[derive(Default)]
pub struct MemEffectLog {
    frames: Mutex<Vec<TxnFrame>>,
}

impl MemEffectLog {
    pub fn new() -> Self {
        MemEffectLog { frames: Mutex::new(Vec::new()) }
    }

    /// Every frame ever appended, in append order.
    pub fn all(&self) -> Vec<TxnFrame> {
        self.frames.lock().expect("effect log mutex poisoned").clone()
    }

    pub fn len(&self) -> usize {
        self.frames.lock().expect("effect log mutex poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The frame for one transaction on one branch, if it was ever appended.
    pub fn frame(&self, branch: BranchId, txn: TxnId) -> Option<TxnFrame> {
        self.frames
            .lock()
            .expect("effect log mutex poisoned")
            .iter()
            .find(|f| f.branch == branch && f.txn_id == txn)
            .cloned()
    }

    /// Classify `frame` against what is stored **without changing anything**.
    ///
    /// `None` means the key is absent, so the whole frame is new. `Some(..)` is a re-append, as
    /// [`classify`] describes it. An `Err` is the contradiction, carrying the one message.
    ///
    /// This exists so [`DurableEffectLog`] can learn *which bytes are new* before it mutates
    /// anything: the shape of the record it must write is decided by the same function that decides
    /// whether the append is legal at all, and a refused append must not reach the file.
    fn classify_append(&self, frame: &TxnFrame) -> Result<Option<Reappend>, FerroError> {
        let frames = self.frames.lock().expect("effect log mutex poisoned");
        match frames.iter().find(|f| f.branch == frame.branch && f.txn_id == frame.txn_id) {
            None => Ok(None),
            Some(existing) => classify(existing, frame).map(Some),
        }
    }
}

/// Whether two numeric deltas are the same delta, **with NaN equal to itself**.
///
/// `Delta` derives `PartialEq`, so `Delta::Float(NAN) != Delta::Float(NAN)` under IEEE rules.
/// `Value` deliberately avoids that trap — its `PartialEq` goes through `Ord`, which uses
/// `f64::total_cmp` — but `OpKind::Add` and `EscrowClaim::amount` carry a `Delta` and bypass `Value`
/// entirely. Left alone, a frame holding a NaN delta reports its own **byte-identical retry** as a
/// contradiction: `classify` finds `old != new`, `extends` finds the op is not a prefix of itself,
/// and the caller is told two transactions are wearing one id. And because `stage_all` re-appends
/// the open frame once per statement, that task's frame could then never grow again — every later
/// statement of it would fail, permanently.
///
/// Not hypothetical: `Delta::compose` turns `inf + -inf` into NaN (`op.rs`), so a composed delta
/// reaches this state without anyone writing `NAN`. It is unreachable from SQL today — the parser
/// yields `±inf` but never NaN — which is exactly why it needs a test rather than a comment.
fn delta_eq(a: &Delta, b: &Delta) -> bool {
    match (a, b) {
        (Delta::Float(x), Delta::Float(y)) => x.total_cmp(y) == std::cmp::Ordering::Equal,
        _ => a == b,
    }
}

/// Two ops are the same effect on the same cell. `Op`'s derived `PartialEq` everywhere except the
/// one field that carries a bare `f64`; see [`delta_eq`].
fn op_eq(a: &Op, b: &Op) -> bool {
    a.tbl == b.tbl
        && a.row == b.row
        && a.col == b.col
        && a.witness == b.witness
        && match (&a.kind, &b.kind) {
            (OpKind::Add(x), OpKind::Add(y)) => delta_eq(x, y),
            (x, y) => x == y,
        }
}

/// Likewise for a reservation, whose `amount` is a `Delta`.
fn claim_eq(a: &EscrowClaim, b: &EscrowClaim) -> bool {
    a.tbl == b.tbl
        && a.row == b.row
        && a.col == b.col
        && a.floor == b.floor
        && a.ceiling == b.ceiling
        && delta_eq(&a.amount, &b.amount)
}

/// Two frames are the same frame. `TxnFrame`'s derived `PartialEq` with [`op_eq`] and [`claim_eq`]
/// in place of `==` on the two vectors that can hold a bare `f64`. Guards need no special case:
/// every float inside one is a `Value`, whose comparison is already total.
fn frame_eq(a: &TxnFrame, b: &TxnFrame) -> bool {
    a.txn_id == b.txn_id
        && a.branch == b.branch
        && a.base == b.base
        && a.seq == b.seq
        && a.schema_ver == b.schema_ver
        && a.guards == b.guards
        && a.ops.len() == b.ops.len()
        && a.ops.iter().zip(&b.ops).all(|(x, y)| op_eq(x, y))
        && a.claims.len() == b.claims.len()
        && a.claims.iter().zip(&b.claims).all(|(x, y)| claim_eq(x, y))
}

/// Whether `new` is the same open frame as `old`, grown.
///
/// Everything identifying the transaction must match exactly, and every op, guard and claim
/// already stored must still be present, in the same order, at the same position. A frame that
/// reorders, rewrites or drops what was logged is not the same transaction continuing — it is a
/// different one reusing the id, and the caller needs to hear about it.
fn extends(old: &TxnFrame, new: &TxnFrame) -> bool {
    fn prefix<T, F: Fn(&T, &T) -> bool>(old: &[T], new: &[T], eq: F) -> bool {
        old.len() <= new.len() && old.iter().zip(new).all(|(a, b)| eq(a, b))
    }
    old.base == new.base
        && old.seq == new.seq
        && old.schema_ver == new.schema_ver
        && prefix(&old.ops, &new.ops, op_eq)
        && prefix(&old.guards, &new.guards, |a, b| a == b)
        && prefix(&old.claims, &new.claims, claim_eq)
}

/// What a second append under a key that is already present is. See the module header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reappend {
    /// The identical frame arrived again. Stored once, and **nothing is written** to a durable
    /// store: there is no new tail, so there is no second copy of any `Add` for a reader to have
    /// to collapse.
    Retry,
    /// The open frame grew. The counts are what was **already stored**, so the new tail is
    /// `frame.ops[ops..]`, `frame.guards[guards..]` and `frame.claims[claims..]`.
    Grew { ops: usize, guards: usize, claims: usize },
}

/// The re-append decision, in one place, for both stores.
fn classify(old: &TxnFrame, new: &TxnFrame) -> Result<Reappend, FerroError> {
    if frame_eq(old, new) {
        // A retry delivering the identical frame. Storing it again would double every Add it
        // carries.
        return Ok(Reappend::Retry);
    }
    if !extends(old, new) {
        return Err(FerroError::Merge(format!(
            "{} on branch {} was already logged with contents that the new frame does \
             not extend; refusing to overwrite it",
            new.txn_id, new.branch
        )));
    }
    Ok(Reappend::Grew {
        ops: old.ops.len(),
        guards: old.guards.len(),
        claims: old.claims.len(),
    })
}

impl EffectLog for MemEffectLog {
    fn append(&self, frame: &TxnFrame) -> Result<(), FerroError> {
        let mut frames = self.frames.lock().expect("effect log mutex poisoned");
        if let Some(i) = frames
            .iter()
            .position(|f| f.branch == frame.branch && f.txn_id == frame.txn_id)
        {
            match classify(&frames[i], frame)? {
                Reappend::Retry => return Ok(()),
                // The open frame grew. **Extended rather than replaced**, and the difference is
                // not performance (though it is also that: replacing cloned the whole frame on
                // every statement of a task, which is quadratic in the ops it ran).
                //
                // `extends` compares with `Value`'s `PartialEq`, which is *numeric* —
                // `Integer(1) == Float(1.0)`, `Decimal("1.50") == Decimal("1.5")`. Replacing
                // therefore let a re-append silently retype a value already stored, while
                // `DurableEffectLog` writes only the new tail and keeps the bytes it first
                // accepted. Extending here makes the two stores hold the identical frame for
                // every input rather than for the inputs a producer happens to send.
                Reappend::Grew { ops, guards, claims } => {
                    let stored = &mut frames[i];
                    stored.ops.extend_from_slice(&frame.ops[ops..]);
                    stored.guards.extend_from_slice(&frame.guards[guards..]);
                    stored.claims.extend_from_slice(&frame.claims[claims..]);
                    return Ok(());
                }
            }
        }
        frames.push(frame.clone());
        Ok(())
    }

    fn frames_for(&self, branch: BranchId, from_seq: u64) -> Result<Vec<TxnFrame>, FerroError> {
        let frames = self.frames.lock().expect("effect log mutex poisoned");
        let mut out: Vec<TxnFrame> = frames
            .iter()
            .filter(|f| f.branch == branch && f.seq >= from_seq)
            .cloned()
            .collect();
        out.sort_by_key(|f| (f.seq, f.txn_id.0));
        Ok(out)
    }
}

// =================================================================================================
// DurableEffectLog
// =================================================================================================

/// `0xF3EE_DB0*` is this codebase's file family: `01` is the WAL, `03` is the consensus round log,
/// and `04` is this one. `02` is unused and left alone rather than recycled, so a mistyped path is
/// refused instead of half-parsed.
const MAGIC: u32 = 0xF3EE_DB04;
const VERSION: u32 = 1;

/// `magic(4) | version(4) | crc32(4)`, the crc over the first eight bytes.
///
/// Checksummed, which the WAL's own header is not — `storage::sim`'s documentation names that a
/// gap, and it is affordable there because a lost WAL costs a recovery. It is not affordable here:
/// this file is the only record of the effects every merge on this node is computed from, so a
/// header that has been damaged is refused rather than read as a fresh log.
const HEADER_SIZE: u64 = 12;

/// `total_len(4) | tag(1) | crc32(4)`: the smallest record that can exist.
const MIN_RECORD: usize = 9;

/// The largest record this store will **write**.
///
/// **Reused rather than chosen.** It is [`crate::consensus::log::MAX_ENTRY_BYTES`], the consensus
/// log's own limit, for the reason `DISTRIBUTED.md` §F9 gives: `BEGIN AGENT SESSION ... DURABLE` is
/// the row that makes a session's TEL frames a replicated command, and a delta larger than one
/// replication frame is a delta that could never be shipped. Two constants with one derivation
/// would be two numbers to keep equal; this is the same number.
const MAX_APPEND_BYTES: usize = crate::consensus::log::MAX_ENTRY_BYTES;

/// The largest record this store will **read** from a disk — a constant of the *format*, and
/// deliberately not the same number as [`MAX_APPEND_BYTES`].
///
/// One number serving both would be a trap rather than a simplification. `MAX_APPEND_BYTES` is
/// derived from `replication::MAX_FRAME_BYTES`, which is a **transport tuning knob**; the scan
/// treats an over-long length prefix as a torn tail and truncates there. So if the read bound
/// tracked the transport, someone lowering that knob would silently truncate every effect log
/// already on a disk that held a record above the new value — a tuning change destroying committed
/// data, with the file reporting a healthy torn-tail heal. Fixing the read bound here decouples
/// them: lowering the transport constant then refuses new large records, which is a refusal the
/// caller sees, and reads every old one unchanged.
///
/// It is also the bound that makes a corrupt length field safe to allocate against — four bad bytes
/// must not be a request for memory — and it is checked *with* `offset + total > len`, so the real
/// ceiling is the file's own size.
const MAX_RECORD: usize = 64 * 1024 * 1024;

/// The write bound must fit inside the read bound, or this store could write a record it refuses to
/// read back. Checked at compile time so a change to either number fails the build rather than a
/// recovery.
const _: () = assert!(
    MAX_APPEND_BYTES <= MAX_RECORD,
    "MAX_APPEND_BYTES exceeds the format's read bound; raise MAX_RECORD in the same commit"
);

/// How deeply a [`GuardExpr`] may nest, on the way in and on the way out.
///
/// **Measured, not chosen.** `scratchpad/guard-depth-measurement.md` records the run: encoding,
/// fsyncing and replaying a guard through this store round-trips at depth 512 and **aborts the
/// process with a stack overflow at 768** in a debug build on a libtest thread. The pre-existing
/// guard operations abort in the same neighbourhood — `Guard::clone`, `Guard::check`,
/// `GuardExpr`'s `Display`, and `Drop` are all recursive, and the four of them together took the
/// process down at 1024.
///
/// That last fact is why a cap is the right mechanism rather than a workaround: `GuardExpr` recurses
/// in `Clone` and in `Drop`, so an iterative encoder and decoder would not remove the limit, only
/// move the abort to the moment the decoded frame is cloned or dropped. 256 is half the deepest
/// depth measured to work and about a third of where the guard subsystem itself dies, which leaves
/// room for a thread stack smaller than libtest's — `pgwire::serve` spawns one per connection.
///
/// Above it, a guard is **refused** and the refusal names the predicate; it is never truncated and
/// never stored in a shape that could not be read back. A predicate that deep is well past anything
/// `tel::capture::to_guard_expr` produces from real SQL: it emits `And`/`Or` as *binary* nodes, so
/// `a AND b AND c` is depth 3 and not depth 1, and even so a WHERE clause of 256 nested operators
/// has never been seen here.
///
/// The decoder enforces the same number, and that half is the one that matters for a file that
/// arrives damaged: a hand-crafted record nesting fifty thousand tags would otherwise be a process
/// abort triggered by a corrupt log, which is the one failure every other malformed record avoids.
const MAX_GUARD_DEPTH: u32 = 256;

const TAG_OPEN: u8 = 1;
const TAG_EXTEND: u8 = 2;

// ---- Value ------------------------------------------------------------------------------------
//
// The tags are `storage::index_page`'s `impl BTreeSerialize for Value`, byte for byte, so the
// codebase has one tag vocabulary for a `Value` on a disk. What is NOT reused is the code, and
// there are two reasons, both of which would be defects here:
//
//   * `Value::deserialize` indexes its input unchecked and **panics** on a short slice. These bytes
//     come off a disk that can stop working half way through a write, and a panic during replay is
//     a process abort caused by four bad bytes. `agent_sql::paged_rows::value_span` exists to
//     pre-validate exactly that, and is private to its module.
//   * `Value::serialize` writes `s.len() as u16` with a raw cast, and `Value::Decimal`'s own
//     documentation rejects a scaled integer *because text has no digit cap*. A 65536-byte decimal
//     would land with a length prefix of zero, inside a record with a valid CRC over exactly the
//     bytes intended: durable, and permanently undecodable. Refusing on the way in is the only
//     point at which the caller still has somewhere to put the error.
//
// Encode and decode live side by side here and are both exhaustive, which is the shape that
// matters: `paged_rows`' own comment records what the other arrangement cost — when the wide types
// were added to `index_page`, the separate span table was not, and BIGINT/DECIMAL/TIMESTAMP cells
// became *write-only* on page-backed branches. A `Value` variant added later fails to compile in
// `put_value` and is refused by `take_value`; neither can silently skip it.
//
// This codec is deletable the day `Value::deserialize` is bounds-checked and its length prefix is
// guarded.
const V_INTEGER: u8 = 0;
const V_VARCHAR: u8 = 1;
const V_FLOAT: u8 = 2;
const V_BOOLEAN: u8 = 3;
const V_NULL: u8 = 4;
const V_BIGINT: u8 = 5;
const V_DECIMAL: u8 = 6;
const V_TIMESTAMP: u8 = 7;

/// `write_str` with the guard [`crate::wal::log::write_str`] does not have.
///
/// The write itself is `wal::log`'s, deliberately: one string encoder means the WAL, the durable
/// provenance store and this file cannot disagree about what a length-prefixed string is.
fn put_str(out: &mut Vec<u8>, s: &str, what: &'static str) -> Result<(), FerroError> {
    if s.len() > u16::MAX as usize {
        return Err(FerroError::Merge(format!(
            "a {what} in this frame is {} bytes, over the {} a length prefix can hold; refusing to \
             write a record that could not be read back",
            s.len(),
            u16::MAX
        )));
    }
    write_str(out, s);
    Ok(())
}

fn put_u32_len(out: &mut Vec<u8>, n: usize, what: &'static str) -> Result<(), FerroError> {
    let n = u32::try_from(n).map_err(|_| {
        FerroError::Merge(format!("this frame holds {n} {what}, more than a u32 count can express"))
    })?;
    out.extend_from_slice(&n.to_be_bytes());
    Ok(())
}

/// A list length, refused when the rest of the record is too short to hold that many of an item
/// that small.
///
/// Never `Vec::with_capacity(n)` straight off a disk: four bad bytes would otherwise be a request
/// to allocate. `consensus::log::read_ids` proves the bytes are present before deriving a capacity;
/// the items here are variable-length, so the bound is the item's *minimum* width instead, and the
/// record's own [`MAX_RECORD`] cap bounds that in turn.
fn take_u32_len(
    body: &[u8],
    at: &mut usize,
    min_item: usize,
    what: &'static str,
) -> Result<usize, FerroError> {
    let n = take_u32(body, at)? as usize;
    let left = body.len().saturating_sub(*at);
    let room = left / min_item.max(1);
    if n > room {
        return Err(FerroError::Corruption(format!(
            "a typed effect record claims {n} {what}, but {left} bytes remain and each is at least \
             {min_item}, so at most {room} can be there"
        )));
    }
    Ok(n)
}

fn put_value(out: &mut Vec<u8>, v: &Value) -> Result<(), FerroError> {
    match v {
        Value::Integer(i) => {
            out.push(V_INTEGER);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Value::Varchar(s) => {
            out.push(V_VARCHAR);
            put_str(out, s, "string value")?;
        }
        // `to_be_bytes` and not `to_string`: `Value`'s ordering uses `f64::total_cmp`, so NaN's
        // payload and sign and the difference between `-0.0` and `0.0` are all part of the value.
        Value::Float(f) => {
            out.push(V_FLOAT);
            out.extend_from_slice(&f.to_be_bytes());
        }
        Value::Boolean(b) => {
            out.push(V_BOOLEAN);
            out.push(*b as u8);
        }
        Value::Null => out.push(V_NULL),
        Value::BigInt(v) => {
            out.push(V_BIGINT);
            out.extend_from_slice(&v.to_be_bytes());
        }
        // The digit text verbatim, scale included: `Value::Decimal` documents that `1.50` must not
        // become `1.5` because the trailing zero is information to whoever reads a price. Note that
        // a round-trip cannot be checked with `==` — `Decimal("1.50") == Decimal("1.5")` is true,
        // because comparison is numeric — which is why `tests_durable_log.rs` compares the text.
        Value::Decimal(d) => {
            out.push(V_DECIMAL);
            put_str(out, d, "decimal value")?;
        }
        Value::Timestamp(ms) => {
            out.push(V_TIMESTAMP);
            out.extend_from_slice(&ms.to_be_bytes());
        }
    }
    Ok(())
}

fn take_value(body: &[u8], at: &mut usize) -> Result<Value, FerroError> {
    Ok(match take_u8(body, at)? {
        V_INTEGER => Value::Integer(take_u32(body, at)? as i32),
        V_VARCHAR => Value::Varchar(take_str(body, at)?),
        V_FLOAT => Value::Float(f64::from_be_bytes(take_array::<8>(body, at)?)),
        // Not `!= 0`. Accepting 2..=255 as `true` would give one value 255 encodings, which is the
        // same defect the trailing-bytes check below refuses at the record level: two byte sequences
        // that decode identically mean two files can be byte-different and indistinguishable.
        V_BOOLEAN => match take_u8(body, at)? {
            0 => Value::Boolean(false),
            1 => Value::Boolean(true),
            other => {
                return Err(FerroError::Corruption(format!(
                    "a boolean in a typed effect record carries byte {other}, which is neither false \
                     (0) nor true (1)"
                )))
            }
        },
        V_NULL => Value::Null,
        V_BIGINT => Value::BigInt(take_u64(body, at)? as i64),
        V_DECIMAL => Value::Decimal(take_str(body, at)?),
        V_TIMESTAMP => Value::Timestamp(take_u64(body, at)? as i64),
        other => {
            return Err(FerroError::Corruption(format!(
                "unknown value tag {other} in a typed effect record"
            )))
        }
    })
}

fn put_opt_value(out: &mut Vec<u8>, v: &Option<Value>) -> Result<(), FerroError> {
    match v {
        None => out.push(0),
        Some(v) => {
            out.push(1);
            put_value(out, v)?;
        }
    }
    Ok(())
}

fn take_opt_value(body: &[u8], at: &mut usize) -> Result<Option<Value>, FerroError> {
    match take_u8(body, at)? {
        0 => Ok(None),
        1 => Ok(Some(take_value(body, at)?)),
        other => Err(FerroError::Corruption(format!(
            "an optional value in a typed effect record carries presence tag {other}, which is \
             neither absent (0) nor present (1)"
        ))),
    }
}

// ---- ids --------------------------------------------------------------------------------------

fn put_branch(out: &mut Vec<u8>, b: BranchId) {
    // `id` then `generation`, the layout `provenance::durable` already writes for a `BranchId`.
    out.extend_from_slice(&b.id.to_be_bytes());
    out.extend_from_slice(&b.generation.to_be_bytes());
}

fn take_branch(body: &[u8], at: &mut usize) -> Result<BranchId, FerroError> {
    let id = take_u64(body, at)?;
    let generation = take_u32(body, at)?;
    Ok(BranchId::new(id, generation))
}

fn put_dot(out: &mut Vec<u8>, d: &Dot) {
    put_branch(out, d.branch);
    out.extend_from_slice(&d.seq.to_be_bytes());
}

fn take_dot(body: &[u8], at: &mut usize) -> Result<Dot, FerroError> {
    let branch = take_branch(body, at)?;
    let seq = take_u64(body, at)?;
    Ok(Dot { branch, seq })
}

/// `branch.id(8) | branch.generation(4) | txn_id(8)` — the log's key, and the only thing a
/// `FrameExtend` needs in order to name the frame it grows.
///
/// The generation is in the key and not merely alongside it: a reaped `BranchId` comes back at the
/// next generation, and a key that ignored it would let a new branch grow the dead one's frames.
fn put_key(out: &mut Vec<u8>, branch: BranchId, txn: TxnId) {
    put_branch(out, branch);
    out.extend_from_slice(&txn.0.to_be_bytes());
}

fn take_key(body: &[u8], at: &mut usize) -> Result<(BranchId, TxnId), FerroError> {
    let branch = take_branch(body, at)?;
    Ok((branch, TxnId(take_u64(body, at)?)))
}

// ---- Delta, Op, OpKind ------------------------------------------------------------------------

fn put_delta(out: &mut Vec<u8>, d: &Delta) {
    match d {
        Delta::Int(i) => {
            out.push(0);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Delta::Float(f) => {
            out.push(1);
            out.extend_from_slice(&f.to_be_bytes());
        }
    }
}

fn take_delta(body: &[u8], at: &mut usize) -> Result<Delta, FerroError> {
    Ok(match take_u8(body, at)? {
        0 => Delta::Int(take_u64(body, at)? as i64),
        1 => Delta::Float(f64::from_be_bytes(take_array::<8>(body, at)?)),
        other => {
            return Err(FerroError::Corruption(format!(
                "unknown numeric-delta tag {other} in a typed effect record"
            )))
        }
    })
}

/// The smallest an `Op` can be: `tbl(4) | row(8) | col-absent(1) | kind(1) | witness-absent(1)`.
const OP_MIN: usize = 15;

fn put_op(out: &mut Vec<u8>, op: &Op) -> Result<(), FerroError> {
    out.extend_from_slice(&op.tbl.0.to_be_bytes());
    out.extend_from_slice(&op.row.0.to_be_bytes());
    match op.col {
        None => out.push(0),
        Some(c) => {
            out.push(1);
            out.extend_from_slice(&c.0.to_be_bytes());
        }
    }
    match &op.kind {
        OpKind::RowCreate(image) => {
            out.push(0);
            put_u32_len(out, image.len(), "columns in a row image")?;
            for v in image {
                put_value(out, v)?;
            }
        }
        OpKind::RowDelete => out.push(1),
        OpKind::Assign(v) => {
            out.push(2);
            put_value(out, v)?;
        }
        OpKind::Add(d) => {
            out.push(3);
            put_delta(out, d);
        }
        OpKind::Max(v) => {
            out.push(4);
            put_value(out, v)?;
        }
        OpKind::Min(v) => {
            out.push(5);
            put_value(out, v)?;
        }
        OpKind::SetInsert { elem, dot } => {
            out.push(6);
            put_value(out, elem)?;
            put_dot(out, dot);
        }
        OpKind::SetRemove { elem, dots } => {
            out.push(7);
            put_value(out, elem)?;
            put_u32_len(out, dots.len(), "observed dots")?;
            for d in dots {
                put_dot(out, d);
            }
        }
    }
    put_opt_value(out, &op.witness)
}

fn take_op(body: &[u8], at: &mut usize) -> Result<Op, FerroError> {
    let tbl = TableId(take_u32(body, at)?);
    let row = RowId(take_u64(body, at)?);
    let col = match take_u8(body, at)? {
        0 => None,
        1 => Some(ColId(take_u32(body, at)?)),
        other => {
            return Err(FerroError::Corruption(format!(
                "an op's column carries presence tag {other}, which is neither absent (0) nor \
                 present (1)"
            )))
        }
    };
    let kind = match take_u8(body, at)? {
        0 => {
            let n = take_u32_len(body, at, 1, "columns in a row image")?;
            let mut image = Vec::with_capacity(n.min(64));
            for _ in 0..n {
                image.push(take_value(body, at)?);
            }
            OpKind::RowCreate(image)
        }
        1 => OpKind::RowDelete,
        2 => OpKind::Assign(take_value(body, at)?),
        3 => OpKind::Add(take_delta(body, at)?),
        4 => OpKind::Max(take_value(body, at)?),
        5 => OpKind::Min(take_value(body, at)?),
        6 => OpKind::SetInsert { elem: take_value(body, at)?, dot: take_dot(body, at)? },
        7 => {
            let elem = take_value(body, at)?;
            let n = take_u32_len(body, at, 20, "observed dots")?;
            let mut dots = Vec::with_capacity(n.min(64));
            for _ in 0..n {
                dots.push(take_dot(body, at)?);
            }
            OpKind::SetRemove { elem, dots }
        }
        other => {
            return Err(FerroError::Corruption(format!(
                "unknown effect-kind tag {other} in a typed effect record"
            )))
        }
    };
    let witness = take_opt_value(body, at)?;
    Ok(Op { tbl, row, col, kind, witness })
}

// ---- Guard, GuardExpr -------------------------------------------------------------------------

/// The smallest a `Guard` can be, derived rather than eyeballed: the shortest expression is
/// `Literal(Null)` at **two** bytes — its own tag plus the value's — then a one-byte `Value::Null`
/// expectation and a one-byte absent `source_text`.
///
/// It was 3 here for one commit, from counting the expression as one byte. Too small only *weakens*
/// the allocation bound rather than breaking it, which is exactly why it is worth correcting: a
/// stated derivation that does not add up is a false claim in a comment the next reader will trust.
const GUARD_MIN: usize = 4;

fn put_guard(out: &mut Vec<u8>, g: &Guard) -> Result<(), FerroError> {
    put_guard_expr(out, &g.expr, 0, g)?;
    put_value(out, &g.expected)?;
    match &g.source_text {
        None => out.push(0),
        Some(t) => {
            out.push(1);
            put_str(out, t, "guard source text")?;
        }
    }
    Ok(())
}

fn take_guard(body: &[u8], at: &mut usize) -> Result<Guard, FerroError> {
    let expr = take_guard_expr(body, at, 0)?;
    let expected = take_value(body, at)?;
    let source_text = match take_u8(body, at)? {
        0 => None,
        1 => Some(take_str(body, at)?),
        other => {
            return Err(FerroError::Corruption(format!(
                "a guard's source text carries presence tag {other}, which is neither absent (0) \
                 nor present (1)"
            )))
        }
    };
    Ok(Guard { expr, expected, source_text })
}

fn cmp_tag(op: CmpOp) -> u8 {
    match op {
        CmpOp::Eq => 0,
        CmpOp::Ne => 1,
        CmpOp::Lt => 2,
        CmpOp::Le => 3,
        CmpOp::Gt => 4,
        CmpOp::Ge => 5,
    }
}

fn cmp_from(tag: u8) -> Result<CmpOp, FerroError> {
    Ok(match tag {
        0 => CmpOp::Eq,
        1 => CmpOp::Ne,
        2 => CmpOp::Lt,
        3 => CmpOp::Le,
        4 => CmpOp::Gt,
        5 => CmpOp::Ge,
        other => {
            return Err(FerroError::Corruption(format!(
                "unknown comparison-operator tag {other} in a guard"
            )))
        }
    })
}

fn arith_tag(op: ArithOp) -> u8 {
    match op {
        ArithOp::Add => 0,
        ArithOp::Sub => 1,
        ArithOp::Mul => 2,
        ArithOp::Div => 3,
    }
}

fn arith_from(tag: u8) -> Result<ArithOp, FerroError> {
    Ok(match tag {
        0 => ArithOp::Add,
        1 => ArithOp::Sub,
        2 => ArithOp::Mul,
        3 => ArithOp::Div,
        other => {
            return Err(FerroError::Corruption(format!(
                "unknown arithmetic-operator tag {other} in a guard"
            )))
        }
    })
}

fn put_guard_expr(
    out: &mut Vec<u8>,
    e: &GuardExpr,
    depth: u32,
    owner: &Guard,
) -> Result<(), FerroError> {
    if depth > MAX_GUARD_DEPTH {
        // `owner.source_text` and NOT `owner.violated_predicate()`. That accessor falls back to
        // `expr.to_string()`, whose `Display` recurses over the WHOLE tree — so on the one input
        // this guard exists to refuse, building the refusal message would overflow the stack the
        // refusal is protecting. Reading the source text cannot recurse at all.
        return Err(FerroError::Merge(format!(
            "a guard in this frame nests more than {MAX_GUARD_DEPTH} operators deep ({}); refusing to \
             write a record this store could not read back without overflowing its own stack",
            match &owner.source_text {
                Some(t) => format!("its source reads `{t}`"),
                None => "it was synthesised and carries no source text".to_string(),
            }
        )));
    }
    match e {
        GuardExpr::Literal(v) => {
            out.push(0);
            put_value(out, v)?;
        }
        GuardExpr::Col { tbl, row, col } => {
            out.push(1);
            out.extend_from_slice(&tbl.0.to_be_bytes());
            out.extend_from_slice(&row.0.to_be_bytes());
            out.extend_from_slice(&col.0.to_be_bytes());
        }
        GuardExpr::Compare { left, op, right } => {
            out.push(2);
            out.push(cmp_tag(*op));
            put_guard_expr(out, left, depth + 1, owner)?;
            put_guard_expr(out, right, depth + 1, owner)?;
        }
        GuardExpr::Arith { left, op, right } => {
            out.push(3);
            out.push(arith_tag(*op));
            put_guard_expr(out, left, depth + 1, owner)?;
            put_guard_expr(out, right, depth + 1, owner)?;
        }
        GuardExpr::And(parts) => {
            out.push(4);
            put_u32_len(out, parts.len(), "conjuncts in a guard")?;
            for p in parts {
                put_guard_expr(out, p, depth + 1, owner)?;
            }
        }
        GuardExpr::Or(parts) => {
            out.push(5);
            put_u32_len(out, parts.len(), "disjuncts in a guard")?;
            for p in parts {
                put_guard_expr(out, p, depth + 1, owner)?;
            }
        }
        GuardExpr::Not(inner) => {
            out.push(6);
            put_guard_expr(out, inner, depth + 1, owner)?;
        }
        GuardExpr::IsNull(inner) => {
            out.push(7);
            put_guard_expr(out, inner, depth + 1, owner)?;
        }
    }
    Ok(())
}

fn take_guard_expr(body: &[u8], at: &mut usize, depth: u32) -> Result<GuardExpr, FerroError> {
    if depth > MAX_GUARD_DEPTH {
        return Err(FerroError::Corruption(format!(
            "a guard in this typed effect record nests more than {MAX_GUARD_DEPTH} operators deep; \
             refusing it rather than recursing until the process dies"
        )));
    }
    Ok(match take_u8(body, at)? {
        0 => GuardExpr::Literal(take_value(body, at)?),
        1 => GuardExpr::Col {
            tbl: TableId(take_u32(body, at)?),
            row: RowId(take_u64(body, at)?),
            col: ColId(take_u32(body, at)?),
        },
        2 => {
            let op = cmp_from(take_u8(body, at)?)?;
            let left = Box::new(take_guard_expr(body, at, depth + 1)?);
            let right = Box::new(take_guard_expr(body, at, depth + 1)?);
            GuardExpr::Compare { left, op, right }
        }
        3 => {
            let op = arith_from(take_u8(body, at)?)?;
            let left = Box::new(take_guard_expr(body, at, depth + 1)?);
            let right = Box::new(take_guard_expr(body, at, depth + 1)?);
            GuardExpr::Arith { left, op, right }
        }
        4 => GuardExpr::And(take_guard_parts(body, at, depth, "conjuncts in a guard")?),
        5 => GuardExpr::Or(take_guard_parts(body, at, depth, "disjuncts in a guard")?),
        6 => GuardExpr::Not(Box::new(take_guard_expr(body, at, depth + 1)?)),
        7 => GuardExpr::IsNull(Box::new(take_guard_expr(body, at, depth + 1)?)),
        other => {
            return Err(FerroError::Corruption(format!(
                "unknown guard-expression tag {other} in a typed effect record"
            )))
        }
    })
}

fn take_guard_parts(
    body: &[u8],
    at: &mut usize,
    depth: u32,
    what: &'static str,
) -> Result<Vec<GuardExpr>, FerroError> {
    // 2, not 1: the shortest child is `Literal(Null)`, which is its tag plus the value's.
    let n = take_u32_len(body, at, 2, what)?;
    let mut parts = Vec::with_capacity(n.min(64));
    for _ in 0..n {
        parts.push(take_guard_expr(body, at, depth + 1)?);
    }
    Ok(parts)
}

// ---- EscrowClaim ------------------------------------------------------------------------------

/// `tbl(4) | row(8) | col(4) | delta-tag(1) + i64(8) | floor-absent(1) | ceiling-absent(1)`.
const CLAIM_MIN: usize = 27;

fn put_claim(out: &mut Vec<u8>, c: &EscrowClaim) -> Result<(), FerroError> {
    out.extend_from_slice(&c.tbl.0.to_be_bytes());
    out.extend_from_slice(&c.row.0.to_be_bytes());
    out.extend_from_slice(&c.col.0.to_be_bytes());
    put_delta(out, &c.amount);
    put_opt_value(out, &c.floor)?;
    put_opt_value(out, &c.ceiling)
}

fn take_claim(body: &[u8], at: &mut usize) -> Result<EscrowClaim, FerroError> {
    let tbl = TableId(take_u32(body, at)?);
    let row = RowId(take_u64(body, at)?);
    let col = ColId(take_u32(body, at)?);
    let amount = take_delta(body, at)?;
    let floor = take_opt_value(body, at)?;
    let ceiling = take_opt_value(body, at)?;
    Ok(EscrowClaim { tbl, row, col, amount, floor, ceiling })
}

// ---- the three lists a record carries ----------------------------------------------------------

/// The new tail of one frame: whatever grew since the last record for its key.
struct Tail {
    ops: Vec<Op>,
    guards: Vec<Guard>,
    claims: Vec<EscrowClaim>,
}

fn put_tail(
    out: &mut Vec<u8>,
    ops: &[Op],
    guards: &[Guard],
    claims: &[EscrowClaim],
) -> Result<(), FerroError> {
    put_u32_len(out, ops.len(), "ops")?;
    for op in ops {
        put_op(out, op)?;
    }
    put_u32_len(out, guards.len(), "guards")?;
    for g in guards {
        put_guard(out, g)?;
    }
    put_u32_len(out, claims.len(), "escrow claims")?;
    for c in claims {
        put_claim(out, c)?;
    }
    Ok(())
}

fn take_tail(body: &[u8], at: &mut usize) -> Result<Tail, FerroError> {
    let n = take_u32_len(body, at, OP_MIN, "ops")?;
    let mut ops = Vec::with_capacity(n.min(64));
    for _ in 0..n {
        ops.push(take_op(body, at)?);
    }
    let n = take_u32_len(body, at, GUARD_MIN, "guards")?;
    let mut guards = Vec::with_capacity(n.min(64));
    for _ in 0..n {
        guards.push(take_guard(body, at)?);
    }
    let n = take_u32_len(body, at, CLAIM_MIN, "escrow claims")?;
    let mut claims = Vec::with_capacity(n.min(64));
    for _ in 0..n {
        claims.push(take_claim(body, at)?);
    }
    Ok(Tail { ops, guards, claims })
}

// ---- records ----------------------------------------------------------------------------------

/// One record, decoded. Deliberately not a `TxnFrame`: a `FrameExtend` is *not* a frame, it is the
/// tail of one, and a type that could hold either would let a replay treat a delta as a whole frame
/// — which is the concatenation bug this format exists to make impossible.
enum Record {
    Open {
        branch: BranchId,
        txn: TxnId,
        base: CommitHash,
        seq: u64,
        schema_ver: u32,
        tail: Tail,
    },
    Extend {
        branch: BranchId,
        txn: TxnId,
        prior_ops: usize,
        prior_guards: usize,
        prior_claims: usize,
        tail: Tail,
    },
}

fn encode_open(frame: &TxnFrame) -> Result<Vec<u8>, FerroError> {
    let mut body = Vec::with_capacity(128);
    body.push(TAG_OPEN);
    put_key(&mut body, frame.branch, frame.txn_id);
    body.extend_from_slice(&frame.base.0);
    body.extend_from_slice(&frame.seq.to_be_bytes());
    body.extend_from_slice(&frame.schema_ver.to_be_bytes());
    put_tail(&mut body, &frame.ops, &frame.guards, &frame.claims)?;
    Ok(body)
}

fn encode_extend(
    frame: &TxnFrame,
    ops: usize,
    guards: usize,
    claims: usize,
) -> Result<Vec<u8>, FerroError> {
    let mut body = Vec::with_capacity(128);
    body.push(TAG_EXTEND);
    put_key(&mut body, frame.branch, frame.txn_id);
    put_u32_len(&mut body, ops, "already-stored ops")?;
    put_u32_len(&mut body, guards, "already-stored guards")?;
    put_u32_len(&mut body, claims, "already-stored escrow claims")?;
    put_tail(&mut body, &frame.ops[ops..], &frame.guards[guards..], &frame.claims[claims..])?;
    Ok(body)
}

fn decode_record(body: &[u8], at_offset: u64, name: &str) -> Result<Record, FerroError> {
    let mut at = 0usize;
    let tag = take_u8(body, &mut at)?;
    let rec = match tag {
        TAG_OPEN => {
            let (branch, txn) = take_key(body, &mut at)?;
            let base = CommitHash(take_array::<32>(body, &mut at)?);
            let seq = take_u64(body, &mut at)?;
            let schema_ver = take_u32(body, &mut at)?;
            let tail = take_tail(body, &mut at)?;
            Record::Open { branch, txn, base, seq, schema_ver, tail }
        }
        TAG_EXTEND => {
            let (branch, txn) = take_key(body, &mut at)?;
            let prior_ops = take_u32(body, &mut at)? as usize;
            let prior_guards = take_u32(body, &mut at)? as usize;
            let prior_claims = take_u32(body, &mut at)? as usize;
            let tail = take_tail(body, &mut at)?;
            Record::Extend { branch, txn, prior_ops, prior_guards, prior_claims, tail }
        }
        other => {
            return Err(FerroError::Corruption(format!(
                "{name}: unknown typed effect record tag {other} at offset {at_offset}"
            )))
        }
    };
    // Trailing bytes are refused, not ignored, and the reason is `consensus::log`'s: a decoder that
    // stops at the end of the fields it recognised accepts two encodings of one record, and two
    // encodings mean two files can be byte-different and decode identically — which is exactly the
    // kind of difference a merge would never surface.
    if at != body.len() {
        return Err(FerroError::Corruption(format!(
            "{name}: the record at offset {at_offset} decoded {at} of its {} body bytes; the {} \
             left over mean this is not a record this build wrote",
            body.len(),
            body.len() - at
        )));
    }
    Ok(rec)
}

/// `total_len(4) | body | crc32(4)`, the framing `wal::log` and `provenance::durable` both use.
fn frame_record(body: &[u8]) -> Result<Vec<u8>, FerroError> {
    let total = 4 + body.len() + 4;
    if total > MAX_APPEND_BYTES {
        return Err(FerroError::Merge(format!(
            "this frame's new effects encode to {total} bytes, over the {MAX_APPEND_BYTES}-byte limit \
             a record may occupy; refusing to store effects that could never be replicated"
        )));
    }
    let mut rec = Vec::with_capacity(total);
    rec.extend_from_slice(&(total as u32).to_be_bytes());
    rec.extend_from_slice(body);
    let crc = crc32(&rec);
    rec.extend_from_slice(&crc.to_be_bytes());
    Ok(rec)
}

/// **A failed read is a fault, not the end of the file**, and keeping those two apart is the whole
/// of this function.
///
/// Every call site has already proved its bytes are inside the length [`Storage::len`] reported, so
/// none of them can fail on end-of-data. The only way in is a real I/O error: a bad sector, a device
/// that went away mid-scan, a file another process truncated underneath us. An earlier version of
/// the scan wrote `if pread_all(..).is_err() { break offset }`, which made that error indistinguish-
/// able from a torn tail — so the heal would `set_len` away every **undamaged** record after the
/// unreadable one and `open` would return `Ok`, leaving a store short of effects it had reported
/// durable and the records themselves gone from the media. Refusing the open instead leaves every
/// byte where it is, which is recoverable; healing is not.
///
/// `consensus::log::scan_frames` propagates for exactly this reason. `provenance::durable` swallows
/// it (`durable.rs:221`, `:229`), and that is a defect there rather than a precedent to copy.
///
/// This is also the one fault class `storage::sim` cannot inject — it declares reads unfaultable
/// because "a failed read leaves the durable image untouched", which was true of every reader in the
/// tree until a reader started truncating on one. So the test for it hands the store a `Storage` of
/// its own rather than a `SimFabric`.
fn read_or_refuse(
    file: &dyn Storage,
    buf: &mut [u8],
    offset: u64,
    name: &str,
) -> Result<(), FerroError> {
    pread_all(file, buf, offset).map_err(|e| {
        FerroError::Io(format!(
            "{name}: reading {} byte(s) at offset {offset} failed: {e}. The scan cannot tell how much \
             of this file is intact, so it refuses to open rather than truncating away what it could \
             not read.",
            buf.len()
        ))
    })
}

fn header_bytes() -> [u8; HEADER_SIZE as usize] {
    let mut h = [0u8; HEADER_SIZE as usize];
    h[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    h[4..8].copy_from_slice(&VERSION.to_be_bytes());
    let crc = crc32(&h[0..8]);
    h[8..12].copy_from_slice(&crc.to_be_bytes());
    h
}

// ---- the store --------------------------------------------------------------------------------

/// What opening the store recovered from its file, and what it discarded.
///
/// Returned rather than logged: "the store opened" and "the store opened and threw away a partial
/// write" are different facts, and only the caller knows whether the second one matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryReport {
    /// Frames the file declared — one per `FrameOpen` record.
    pub frames: usize,
    /// Growth records replayed on top of them.
    pub extensions: usize,
    /// Bytes of a torn tail that were discarded. Non-zero means the process that wrote this file
    /// died mid-append.
    pub discarded_tail_bytes: u64,
}

/// Everything a write touches, under one lock.
struct Inner {
    file: Arc<dyn Storage>,
    /// Offset just past the last record that reached the file. Advanced **only** by a write that
    /// returned `Ok`, which is what keeps it in lockstep with [`DurableEffectLog::mem`].
    end: u64,
}

/// An [`EffectLog`] backed by a file, so the effects a merge is computed from outlive the process
/// that captured them.
pub struct DurableEffectLog {
    /// The in-memory index: deliberately the existing, tested implementation rather than a second
    /// one. Every guard [`MemEffectLog`] enforces applies here unchanged, `frames_for` is the same
    /// function with the same ordering, and the re-append rule is [`classify`] for both.
    mem: MemEffectLog,
    inner: Mutex<Inner>,
    /// What to call this store in an error. A path when [`DurableEffectLog::open`] made it; a label
    /// when a test handed it a [`Storage`] that is not a file.
    name: String,
    recovery: RecoveryReport,
}

impl std::fmt::Debug for DurableEffectLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let end = self.inner.lock().map(|i| i.end).unwrap_or(0);
        write!(
            f,
            "DurableEffectLog {{ name: {}, frames: {}, end: {end}, recovery: {:?} }}",
            self.name,
            self.mem.len(),
            self.recovery
        )
    }
}

impl DurableEffectLog {
    /// Open the log at `path`, creating it if absent and replaying whatever is there. The
    /// production entry point.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FerroError> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| FerroError::Io(format!("open {}: {e}", path.display())))?;
        Self::with_storage(path.display().to_string(), Arc::new(file))
    }

    /// The `EffectLog` a database gets when the layer that owns its name builds one: the durable
    /// store at `<db>.tel`.
    ///
    /// It exists so that an entry point wiring a runtime makes **no decision** — the naming
    /// convention and the choice of implementation live here, beside the format, rather than being
    /// spelled out at each call site the way `MemEffectLog::new()` is today. The same reasoning
    /// `provenance::durable` gives for `with_durable_provenance` living above the constructors: a
    /// constructor that takes page stores is never given the database's name, so the layer that
    /// owns the path applies it.
    pub fn default_for_database(db_path: &str) -> Result<Arc<dyn EffectLog>, FerroError> {
        Ok(Arc::new(Self::open(format!("{db_path}.tel"))?))
    }

    /// Open the log on any [`Storage`]. **This is the seam a fault is aimed through**: hand it a
    /// handle from a `SimFabric` and every byte of every durability decision becomes injectable,
    /// which is how `tests_durable_log.rs` sweeps a crash across each operation of an append
    /// instead of hand-editing a file after the fact.
    pub fn with_storage(name: impl Into<String>, file: Arc<dyn Storage>) -> Result<Self, FerroError> {
        let name = name.into();
        let len = file.len().map_err(|e| FerroError::Io(format!("{name}: {e}")))?;
        let mem = MemEffectLog::new();
        // **The boundary is `<=` and not `== 0`, and the reason is a torn first write.**
        //
        // The very first thing a brand-new log does is write its 12-byte header, and a crash that
        // tears that leaves a few bytes and no records at all. Refusing there would brick a log over
        // a file that has never held anything. The boundary is exact rather than generous: records
        // begin at `HEADER_SIZE`, so a file no longer than the header cannot contain a byte of any
        // frame, and initialising over it can lose nothing. A file *longer* than the header might,
        // and `replay` refuses a bad header there rather than reinitialising over effects a merge
        // was computed from. `consensus::log` draws the same line for the same reason.
        //
        // No `set_len` first, and that is provable rather than hopeful: this arm is only reached
        // when the file is at most `HEADER_SIZE` bytes and the write below is exactly `HEADER_SIZE`
        // bytes at offset 0, so every byte a torn earlier attempt left is overwritten.
        let (recovery, end) = if len <= HEADER_SIZE {
            // Written and fsynced before a single record may be appended. A header that is not
            // durable when the records above it are is a log that reopens as empty.
            pwrite_all(&*file, &header_bytes(), 0)?;
            file.sync_all().map_err(|e| FerroError::Io(format!("{name}: {e}")))?;
            (RecoveryReport::default(), HEADER_SIZE)
        } else {
            Self::replay(&*file, len, &mem, &name)?
        };
        Ok(DurableEffectLog { mem, inner: Mutex::new(Inner { file, end }), name, recovery })
    }

    /// Replay the file into `mem`, healing a torn tail and refusing damage anywhere else.
    fn replay(
        file: &dyn Storage,
        len: u64,
        mem: &MemEffectLog,
        name: &str,
    ) -> Result<(RecoveryReport, u64), FerroError> {
        debug_assert!(
            len > HEADER_SIZE,
            "with_storage initialises a file this short rather than replaying it"
        );
        let mut header = [0u8; HEADER_SIZE as usize];
        pread_all(file, &mut header, 0)?;
        if crc32(&header[0..8]) != u32::from_be_bytes(header[8..12].try_into().unwrap()) {
            return Err(FerroError::Corruption(format!(
                "{name}'s header fails its own checksum, and the file is {len} bytes — room for \
                 records. Refusing rather than reinitialising over them: an empty effect log and a \
                 damaged one both answer 'no frames' for every branch, and only one of those is a \
                 fact about this database."
            )));
        }
        if u32::from_be_bytes(header[0..4].try_into().unwrap()) != MAGIC {
            return Err(FerroError::Corruption(format!(
                "{name} does not begin with the typed effect log magic; refusing to read it as one"
            )));
        }
        let version = u32::from_be_bytes(header[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(FerroError::Corruption(format!(
                "{name} is typed effect log format version {version}; this build reads version \
                 {VERSION}"
            )));
        }

        // Reconstruct in FILE order. `order` keeps the declaration order so the index ends up
        // holding frames in the order a live store would have pushed them, which is what
        // `MemEffectLog::all` reports and what `frames_for` sorts from.
        let mut order: Vec<(BranchId, TxnId)> = Vec::new();
        let mut built: HashMap<(BranchId, TxnId), TxnFrame> = HashMap::new();
        let mut frames = 0usize;
        let mut extensions = 0usize;
        let mut offset = HEADER_SIZE;
        let good_end = loop {
            if offset + 4 > len {
                break offset;
            }
            let mut len_buf = [0u8; 4];
            read_or_refuse(file, &mut len_buf, offset, name)?;
            let total = u32::from_be_bytes(len_buf) as u64;
            if (total as usize) < MIN_RECORD || total as usize > MAX_RECORD || offset + total > len {
                break offset;
            }
            let mut rec = vec![0u8; total as usize];
            read_or_refuse(file, &mut rec, offset, name)?;
            let stored = u32::from_be_bytes(rec[total as usize - 4..].try_into().unwrap());
            if crc32(&rec[..total as usize - 4]) != stored {
                break offset;
            }
            // From here on the bytes are what was written, so anything wrong with them is the file
            // disagreeing with itself rather than a torn tail — and every branch below refuses.
            match decode_record(&rec[4..total as usize - 4], offset, name)? {
                Record::Open { branch, txn, base, seq, schema_ver, tail } => {
                    if built.contains_key(&(branch, txn)) {
                        return Err(FerroError::Corruption(format!(
                            "{name}: the record at offset {offset} opens {txn} on branch {branch}, \
                             which this file already declared. That is two transactions wearing one \
                             id, and merging their effects together would compose ops that never \
                             belonged to one transaction; refusing to open it."
                        )));
                    }
                    let mut frame = TxnFrame::new(txn, branch, base, seq, schema_ver);
                    frame.ops = tail.ops;
                    frame.guards = tail.guards;
                    frame.claims = tail.claims;
                    built.insert((branch, txn), frame);
                    order.push((branch, txn));
                    frames += 1;
                }
                Record::Extend { branch, txn, prior_ops, prior_guards, prior_claims, tail } => {
                    let frame = built.get_mut(&(branch, txn)).ok_or_else(|| {
                        FerroError::Corruption(format!(
                            "{name}: the record at offset {offset} grows {txn} on branch {branch}, \
                             which this file never declared. The frame's earlier effects are \
                             missing, so replaying this tail alone would produce a transaction that \
                             is short of the ops it ran; refusing to open it."
                        ))
                    })?;
                    // The hole check, by arithmetic. A record duplicated or missing in the middle of
                    // the file is otherwise concatenated silently, and for `OpKind::Add` that is
                    // the counter bug: the frame ends up holding an increment twice, or not at all,
                    // and nothing downstream can tell because every byte matches a checksum of
                    // itself.
                    if (prior_ops, prior_guards, prior_claims)
                        != (frame.ops.len(), frame.guards.len(), frame.claims.len())
                    {
                        return Err(FerroError::Corruption(format!(
                            "{name}: the record at offset {offset} grows {txn} on branch {branch} \
                             from {prior_ops} ops / {prior_guards} guards / {prior_claims} claims, \
                             but replay has {} / {} / {} of them. A record is missing or duplicated \
                             between here and this frame's declaration; refusing to open it.",
                            frame.ops.len(),
                            frame.guards.len(),
                            frame.claims.len()
                        )));
                    }
                    frame.ops.extend(tail.ops);
                    frame.guards.extend(tail.guards);
                    frame.claims.extend(tail.claims);
                    extensions += 1;
                }
            }
            offset += total;
        };

        let discarded_tail_bytes = len - good_end;
        if discarded_tail_bytes > 0 {
            // Healed the way the WAL heals a torn tail, so the next append does not write past
            // garbage and produce a file that stops being readable at the same point for ever.
            file.set_len(good_end)
                .map_err(|e| FerroError::Io(format!("{name}: truncate torn tail: {e}")))?;
            file.sync_all().map_err(|e| FerroError::Io(format!("{name}: {e}")))?;
        }

        for key in order {
            let frame = built.remove(&key).expect("every declared key was built");
            mem.append(&frame)?;
        }
        Ok((RecoveryReport { frames, extensions, discarded_tail_bytes }, good_end))
    }

    fn write_record(file: &dyn Storage, rec: &[u8], at: u64) -> Result<(), FerroError> {
        pwrite_all(file, rec, at)?;
        // Synchronous on every append, deliberately. The whole claim of this store is that a frame
        // survives the process that captured it; a buffered write that has not reached the disk
        // survives a clean exit and nothing else, and the difference is invisible until the crash
        // that matters. The cost is one fsync per statement of an agent task, which is the price of
        // the guarantee rather than an oversight — and it is one fsync of the *delta*, not of the
        // whole frame, which is the other reason the format appends tails.
        file.sync_data().map_err(|e| FerroError::Io(e.to_string()))
    }

    /// What opening this store recovered from the file, and what it discarded.
    pub fn recovery(&self) -> RecoveryReport {
        self.recovery
    }

    /// Bytes of a torn tail thrown away when this store was opened. Zero on a clean file.
    pub fn discarded_tail_bytes(&self) -> u64 {
        self.recovery.discarded_tail_bytes
    }

    /// The path, or the label a test opened this store under.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// How many frames the index holds. The same count [`MemEffectLog::len`] reports, because it is
    /// that store underneath.
    pub fn len(&self) -> usize {
        self.mem.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mem.is_empty()
    }

    /// The frame for one transaction on one branch, if it was ever appended.
    pub fn frame(&self, branch: BranchId, txn: TxnId) -> Option<TxnFrame> {
        self.mem.frame(branch, txn)
    }
}

impl EffectLog for DurableEffectLog {
    fn append(&self, frame: &TxnFrame) -> Result<(), FerroError> {
        let mut inner = self.inner.lock().expect("durable effect log mutex poisoned");

        // Classified, and encoded, BEFORE anything is mutated and before a byte is written.
        //
        // `classify_append` is a read, so every refusal this store owes the caller — a
        // contradiction under an id already used, a string too long for its length prefix, a guard
        // nested past what the decoder can survive, a delta too large to replicate — happens with
        // the index and the file both untouched. `provenance::durable` had to mutate first because
        // its guards live inside the mutation; this store's do not, and the difference is what lets
        // the order below be the safe one.
        let body = match self.mem.classify_append(frame)? {
            None => encode_open(frame)?,
            // A retry. There is no new tail, so there is nothing to write — which is stronger than
            // writing a second copy and collapsing it on read: the file simply never holds one.
            Some(Reappend::Retry) => return Ok(()),
            Some(Reappend::Grew { ops, guards, claims }) => {
                encode_extend(frame, ops, guards, claims)?
            }
        };
        let rec = frame_record(&body)?;

        // **The record lands first, and only then does the index accept it.**
        //
        // That order is what makes a failed append cost exactly one append. `end` and `mem` advance
        // together or not at all, so a write that fails leaves the store exactly as it was: the next
        // append re-derives the same prior counts and writes at the same offset, over whatever
        // partial bytes the failure left. Nothing has to be latched off.
        //
        // The other order is a trap here, and it is worth naming because `provenance::durable` uses
        // it: had the index accepted the growth first, a failed write would leave `mem` holding
        // effects the file does not, and the *next* growth would write a `FrameExtend` whose prior
        // counts describe the index rather than the file. Replay would then refuse the arithmetic
        // check and the whole file with it — one failed write costing every frame ever captured on
        // this node.
        let at = inner.end;
        Self::write_record(&*inner.file, &rec, at)?;
        self.mem.append(frame)?;
        inner.end = at + rec.len() as u64;
        Ok(())
    }

    fn frames_for(&self, branch: BranchId, from_seq: u64) -> Result<Vec<TxnFrame>, FerroError> {
        self.mem.frames_for(branch, from_seq)
    }
}

// The house style in `src/tel/` is an inline `#[cfg(test)] mod tests` per file, and the file below
// deviates from it for the reason every Phase F lane did: it is large enough that keeping it beside
// a 700-line implementation would bury both. `#[path]` on a module that is not inside an inline
// block resolves against this file's directory, so no edit to `mod.rs` is needed to compile it.
#[cfg(test)]
#[path = "tests_durable_log.rs"]
mod tests_durable_log;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::types::CommitHash;
    use crate::catalog::column::Value;
    use crate::tel::ids::{ColId, RowId, TableId};
    use crate::tel::op::{Delta, Op, OpKind};

    fn decrement(txn: u64, branch: u64, seq: u64, n: i64) -> TxnFrame {
        let mut f = TxnFrame::new(
            TxnId(txn),
            BranchId::new(branch, 0),
            CommitHash::ZERO,
            seq,
            1,
        );
        f.push_op(Op::new(
            TableId(1),
            RowId(1),
            Some(ColId(2)),
            OpKind::Add(Delta::Int(-n)),
        ));
        f
    }

    #[test]
    fn frames_come_back_in_sequence_order_per_branch() {
        let log = MemEffectLog::new();
        log.append(&decrement(2, 1, 5, 1)).unwrap();
        log.append(&decrement(1, 1, 2, 1)).unwrap();
        log.append(&decrement(3, 2, 1, 1)).unwrap();

        let b1 = log.frames_for(BranchId::new(1, 0), 0).unwrap();
        assert_eq!(b1.iter().map(|f| f.seq).collect::<Vec<_>>(), vec![2, 5]);
        assert_eq!(log.frames_for(BranchId::new(2, 0), 0).unwrap().len(), 1);
        assert_eq!(log.frames_for(BranchId::new(1, 0), 3).unwrap().len(), 1);
    }

    #[test]
    fn replaying_the_identical_frame_does_not_store_it_twice() {
        // The Cassandra counter trap, at the log level: a retried Add must not be stored twice.
        let log = MemEffectLog::new();
        let f = decrement(7, 1, 0, 5);
        log.append(&f).unwrap();
        log.append(&f).unwrap();
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn reusing_a_txn_id_for_different_contents_is_an_error_not_an_overwrite() {
        let log = MemEffectLog::new();
        log.append(&decrement(7, 1, 0, 5)).unwrap();
        assert!(log.append(&decrement(7, 1, 0, 9)).is_err());
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn the_same_txn_id_on_a_different_branch_is_a_different_frame() {
        let log = MemEffectLog::new();
        log.append(&decrement(7, 1, 0, 5)).unwrap();
        log.append(&decrement(7, 2, 0, 5)).unwrap();
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn guards_survive_the_round_trip_separately_from_ops() {
        use crate::tel::guard::{CmpOp, Guard, GuardExpr};
        let log = MemEffectLog::new();
        let mut f = decrement(1, 1, 0, 5);
        f.push_guard(
            Guard::holds(GuardExpr::cmp(
                GuardExpr::col(TableId(1), RowId(1), ColId(2)),
                CmpOp::Ge,
                GuardExpr::Literal(Value::Integer(0)),
            ))
            .with_source("qty >= 0"),
        );
        log.append(&f).unwrap();
        let back = &log.frames_for(BranchId::new(1, 0), 0).unwrap()[0];
        assert_eq!(back.guards.len(), 1);
        assert_eq!(back.guards[0].violated_predicate(), "qty >= 0");
        assert_eq!(back.ops.len(), 1);
    }
}
