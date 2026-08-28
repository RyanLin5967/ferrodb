//! F3 — carrying `Message`s between nodes over the existing replication framing.
//!
//! **OWNER: agent F3.** Reuses `tag | u32 len | body` and the `0xFEDB` handshake from
//! `crate::replication`; `REPL_VERSION` goes to 2 so a v1 peer is refused at the handshake rather
//! than misparsed several frames later. `std::net` and `std::thread` only — this crate carries zero
//! runtime dependencies and that is a product claim.
//!
//! # Two halves, and they are separable on purpose
//!
//! [`encode`]/[`decode`] are pure functions over bytes, and [`Transport`] is the thread-per-peer
//! socket carriage above them. The split is not tidiness: the codec is the part that has to be
//! exhaustively tested against hostile input, and a codec reachable only through a socket can only
//! be tested through one. Every rule below is stated as a property of `decode` and pinned by a test
//! that hands it bytes directly.
//!
//! # What this module does NOT do
//!
//! **It does not authenticate.** `Message::from` is the sender's *claim* and nothing here checks
//! it. An unauthenticated peer that can reach this port can assert any term and demote a healthy
//! leader, which is the attack F7 (`signing.rs`) exists to close. Naming it here rather than
//! leaving a reader to discover it: until `signing.rs` lands, a consensus port must not be exposed
//! to anything the operator does not already trust at the network layer.
//!
//! **It does not retry, order, or deduplicate.** Consensus is specified against a network that
//! drops, reorders and duplicates — that is why `Consensus` re-sends on a refusal and backs a peer
//! up by `hint` rather than assuming delivery. So this transport is allowed to drop, and
//! [`Transport::send`] never blocks the state machine: a leader that blocked writing to one
//! partitioned follower would stop heartbeating the healthy majority, turning one node's failure
//! into the cluster's. Drops are **counted** ([`Transport::dropped`]) rather than silent, because
//! an invisible drop is indistinguishable from a protocol bug.
//!
//! # The wire format
//!
//! One frame, one message:
//!
//! ```text
//! 'C' | u32 length-of-body | from:u32 to:u32 term:u64 kind:u8 <kind-specific>
//! ```
//!
//! The length covers the body only — the convention `replication` states and pins, and the one
//! pgwire does *not* use. All integers are big-endian, matching every other encoder in this crate.
//!
//! **One tag for the whole protocol, with the kind byte inside the body.** The reasoning lives
//! beside [`crate::replication::CONSENSUS_TAG`], where the tag registry is.
//!
//! ## The encoding is canonical, and that is a requirement rather than a nicety
//!
//! Exactly one byte string decodes to any given [`Message`], and `decode` refuses every other
//! spelling of it: a boolean that is not 0 or 1, a member list that is unsorted or repeats, a body
//! with bytes left over after its last field. The property those rules buy is
//! `encode(decode(b)) == b`, and F7 needs it — a MAC is taken over bytes, so a receiver that
//! re-encodes a message it accepted must reproduce the bytes it authenticated. A decoder that
//! quietly normalises is a decoder whose output cannot be signed.

use std::collections::{BTreeMap, VecDeque};
use std::io::{Cursor, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::error::FerroError;
use crate::replication::{
    read_handshake, write_handshake, CONSENSUS_TAG, MAX_FRAME_BYTES, REPL_VERSION,
};
use crate::wal::log::{take_u32, take_u64, take_u8, RecKind};

use super::config::Config;
use super::snapshot::SnapshotMeta;
use super::{Body, BranchOp, Command, Entry, Message, NodeId, Round, Term};

// ---------------------------------------------------------------------------------------------
// Kind bytes
//
// Numeric and append-only, exactly as `RecKind`'s tags are: a variant added later takes the next
// free number so that a peer built before it exists refuses the frame by name instead of decoding
// it as its neighbour.
// ---------------------------------------------------------------------------------------------

const K_PRE_VOTE: u8 = 0;
const K_PRE_VOTE_RESP: u8 = 1;
const K_REQUEST_VOTE: u8 = 2;
const K_REQUEST_VOTE_RESP: u8 = 3;
const K_APPEND: u8 = 4;
const K_APPEND_RESP: u8 = 5;
const K_INSTALL_SNAPSHOT: u8 = 6;
const K_INSTALL_SNAPSHOT_RESP: u8 = 7;

const C_WAL_BATCH: u8 = 0;
const C_CATALOG: u8 = 1;
const C_BRANCH: u8 = 2;
const C_ARENA_GRANT: u8 = 3;
const C_TXN_ID_RANGE: u8 = 4;
const C_LEASE_TICK: u8 = 5;
const C_CHECKPOINT: u8 = 6;
const C_MEMBERSHIP: u8 = 7;
const C_NO_OP: u8 = 8;

const B_FORK: u8 = 0;
const B_MERGE: u8 = 1;
const B_ABANDON: u8 = 2;
const B_REAP: u8 = 3;

/// `RecKind::Ddl`'s tag in `wal::log`. Named here because [`Command::Catalog`] is carried *as* one
/// of those records, and because the tag has to be checked before the record is handed to
/// `RecKind::deserialize` — see [`decode_catalog`].
const RECKIND_DDL_TAG: u8 = 9;

// ---------------------------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------------------------

/// The whole frame — tag, length and body — for one consensus message.
///
/// Fallible, and the failure is the point. `replication::Message::encode` writes
/// `(body.len() as u32)`, which silently truncates a body over 4 GiB into a small length and
/// produces a frame the peer will happily misparse. Nothing in this codec may do that, so the
/// running buffer is checked against [`MAX_FRAME_BYTES`] as it grows — before the allocation, not
/// after it — and an oversized message is refused to its *sender*. A leader whose batch does not
/// fit must make a smaller batch; that is a decision for `replicate.rs`, and it can only make it if
/// this returns an error instead of a corrupt frame.
pub fn encode(m: &Message) -> Result<Vec<u8>, FerroError> {
    let mut body = Vec::new();
    put_u32(&mut body, m.from.0);
    put_u32(&mut body, m.to.0);
    put_u64(&mut body, m.term);
    encode_body(&mut body, &m.body)?;

    // Belt as well as braces: every variable-length step above already refused to grow past the
    // limit, and this is the single statement a reader can check the claim against.
    if body.len() > MAX_FRAME_BYTES {
        return Err(too_big(body.len()));
    }

    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(CONSENSUS_TAG);
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

fn too_big(n: usize) -> FerroError {
    FerroError::Wal(format!(
        "a consensus message encodes to {n} bytes, over the {MAX_FRAME_BYTES}-byte frame limit; \
         it was refused rather than framed, because a length that does not fit the frame header \
         is a frame the peer misparses. Send fewer entries."
    ))
}

/// Refuse *before* growing the buffer, so an over-large message costs its sender nothing.
fn room(b: &[u8], adding: usize) -> Result<(), FerroError> {
    let want = b.len().saturating_add(adding);
    if want > MAX_FRAME_BYTES {
        return Err(too_big(want));
    }
    Ok(())
}

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_be_bytes());
}

fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_be_bytes());
}

/// `false` is 0 and `true` is 1, and [`take_bool`] refuses everything else. See the canonicality
/// note in the module header.
fn put_bool(b: &mut Vec<u8>, v: bool) {
    b.push(u8::from(v));
}

fn put_bytes(b: &mut Vec<u8>, v: &[u8]) -> Result<(), FerroError> {
    room(b, 4 + v.len())?;
    put_u32(b, v.len() as u32);
    b.extend_from_slice(v);
    Ok(())
}

fn encode_body(b: &mut Vec<u8>, body: &Body) -> Result<(), FerroError> {
    match body {
        Body::PreVote { last_term, last_round } => {
            b.push(K_PRE_VOTE);
            put_u64(b, *last_term);
            put_u64(b, *last_round);
        }
        Body::PreVoteResp { granted } => {
            b.push(K_PRE_VOTE_RESP);
            put_bool(b, *granted);
        }
        Body::RequestVote { last_term, last_round } => {
            b.push(K_REQUEST_VOTE);
            put_u64(b, *last_term);
            put_u64(b, *last_round);
        }
        Body::RequestVoteResp { granted } => {
            b.push(K_REQUEST_VOTE_RESP);
            put_bool(b, *granted);
        }
        Body::Append { prev_round, prev_term, entries, commit } => {
            b.push(K_APPEND);
            put_u64(b, *prev_round);
            put_u64(b, *prev_term);
            put_u64(b, *commit);
            // The count is written before the entries, and `decode` deliberately does NOT
            // pre-allocate from it — see `decode_append`.
            room(b, 4)?;
            put_u32(b, u32::try_from(entries.len()).map_err(|_| too_big(entries.len()))?);
            for e in entries {
                encode_entry(b, e)?;
            }
        }
        Body::AppendResp { success, matched, hint, digest } => {
            b.push(K_APPEND_RESP);
            put_bool(b, *success);
            put_u64(b, *matched);
            put_u64(b, *hint);
            put_u64(b, *digest);
        }
        Body::InstallSnapshot { meta, offset, data, done } => {
            b.push(K_INSTALL_SNAPSHOT);
            encode_snapshot_meta(b, meta)?;
            put_u64(b, *offset);
            put_bool(b, *done);
            put_bytes(b, data)?;
        }
        Body::InstallSnapshotResp { received_through } => {
            b.push(K_INSTALL_SNAPSHOT_RESP);
            put_u64(b, *received_through);
        }
    }
    Ok(())
}

fn encode_entry(b: &mut Vec<u8>, e: &Entry) -> Result<(), FerroError> {
    // Checked per entry rather than only at the end, so a list of a hundred million `NoOp`s is
    // refused after the first few bytes instead of after 1.7 GB of them.
    room(b, 17)?;
    put_u64(b, e.term);
    put_u64(b, e.round);
    encode_command(b, &e.command)
}

fn encode_command(b: &mut Vec<u8>, c: &Command) -> Result<(), FerroError> {
    match c {
        Command::WalBatch { start_lsn, bytes } => {
            b.push(C_WAL_BATCH);
            put_u64(b, *start_lsn);
            put_bytes(b, bytes)?;
        }
        Command::Catalog { op, table, columns } => {
            b.push(C_CATALOG);
            // **Encoded as the `RecKind::Ddl` record it describes, not by a second schema
            // encoder.** `Command::Catalog`'s own doc says its `columns` match `RecKind::Ddl`
            // "exactly so the two descriptions of one schema cannot drift" — and two hand-written
            // encoders is precisely how they would. Reusing `RecKind::serialize` also means a
            // `DataType` or `ColumnAlteration` variant added later crosses this wire correctly with
            // no edit here, because `wal::log::write_data_type` is the one definition of both.
            //
            // The two page-id fields are written as zero and **refused as anything else on the way
            // back in**. `Command::Catalog` carries no roots by design — a page id is node-local,
            // and shipping the leader's would make every follower's catalog a copy of the leader's
            // physical layout. Writing zeroes and checking them turns that design statement into a
            // guard that fires.
            let rec = RecKind::Ddl {
                op: op.clone(),
                table: table.clone(),
                dir_root: 0,
                time_travel_root: 0,
                columns: columns.clone(),
            };
            let mut rec_bytes = Vec::new();
            rec.serialize(&mut rec_bytes);
            put_bytes(b, &rec_bytes)?;
        }
        Command::Branch { op } => {
            b.push(C_BRANCH);
            encode_branch_op(b, op);
        }
        Command::ArenaGrant { node, first_page, page_count } => {
            b.push(C_ARENA_GRANT);
            put_u32(b, node.0);
            put_u32(b, *first_page);
            put_u32(b, *page_count);
        }
        Command::TxnIdRange { node, lo, hi } => {
            b.push(C_TXN_ID_RANGE);
            put_u32(b, node.0);
            put_u64(b, *lo);
            put_u64(b, *hi);
        }
        Command::LeaseTick { unix_millis } => {
            b.push(C_LEASE_TICK);
            put_u64(b, *unix_millis);
        }
        Command::Checkpoint => b.push(C_CHECKPOINT),
        Command::Membership { config } => {
            b.push(C_MEMBERSHIP);
            encode_config(b, config)?;
        }
        Command::NoOp => b.push(C_NO_OP),
    }
    Ok(())
}

fn encode_branch_op(b: &mut Vec<u8>, op: &BranchOp) {
    match op {
        BranchOp::Fork { child, parent, fork_epoch, lease_millis } => {
            b.push(B_FORK);
            put_u64(b, *child);
            put_u64(b, *parent);
            put_u64(b, *fork_epoch);
            put_u64(b, *lease_millis);
        }
        BranchOp::Merge { branch, base_round } => {
            b.push(B_MERGE);
            put_u64(b, *branch);
            put_u64(b, *base_round);
        }
        BranchOp::Abandon { branch } => {
            b.push(B_ABANDON);
            put_u64(b, *branch);
        }
        BranchOp::Reap { branch, generation } => {
            b.push(B_REAP);
            put_u64(b, *branch);
            put_u32(b, *generation);
        }
    }
}

fn encode_config(b: &mut Vec<u8>, cfg: &Config) -> Result<(), FerroError> {
    put_u64(b, cfg.version);
    put_u64(b, cfg.term);
    for list in [cfg.members(), cfg.learners()] {
        room(b, 4 + list.len() * 4)?;
        put_u32(b, u32::try_from(list.len()).map_err(|_| too_big(list.len()))?);
        for n in list {
            put_u32(b, n.0);
        }
    }
    Ok(())
}

fn encode_snapshot_meta(b: &mut Vec<u8>, meta: &SnapshotMeta) -> Result<(), FerroError> {
    put_u64(b, meta.last_round);
    put_u64(b, meta.last_term);
    put_u64(b, meta.total_bytes);
    // The configuration travels with the snapshot because a receiver that installs one without it
    // cannot count a majority — F6's rule, enforced here by there being no way to encode a
    // `SnapshotMeta` that omits it.
    encode_config(b, &meta.config)
}

// ---------------------------------------------------------------------------------------------
// Decoding
//
// Every failure is a `FerroError::Wal`, which is the class `replication::Message::read_from` and
// `wal::log::short` already use for "a frame on this wire is not well formed". One class for both
// framings, so an operator filtering logs sees consensus and log-shipping framing failures
// together rather than having to know which module produced them.
// ---------------------------------------------------------------------------------------------

/// One message from the **body** of a frame — tag and length already stripped.
///
/// Refuses trailing bytes. A decoder that stops at its last field and ignores the slack accepts an
/// unbounded family of byte strings for one message, which is the canonicality property in the
/// module header gone; it is also how a hand-rolled protocol hides a field the sender wrote and the
/// receiver forgot to read.
pub fn decode(body: &[u8]) -> Result<Message, FerroError> {
    let mut at = 0usize;
    let from = NodeId(take_u32(body, &mut at)?);
    let to = NodeId(take_u32(body, &mut at)?);
    let term: Term = take_u64(body, &mut at)?;
    let b = decode_body(body, &mut at)?;
    if at != body.len() {
        return Err(FerroError::Wal(format!(
            "a consensus frame has {} byte(s) left over after its last field; the sender and this \
             build disagree about the shape of a {:?} message, and decoding it anyway would accept \
             several spellings of one message",
            body.len() - at,
            std::mem::discriminant(&b)
        )));
    }
    Ok(Message { from, to, term, body: b })
}

fn take_bool(bytes: &[u8], at: &mut usize) -> Result<bool, FerroError> {
    match take_u8(bytes, at)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(FerroError::Wal(format!(
            "a consensus flag byte is {other}; only 0 and 1 spell a boolean. Reading it as \
             `!= 0` would make {other} and 1 decode to the same message, so a signature taken over \
             a re-encoding would not match the bytes that arrived — see the canonicality note in \
             this module's header."
        ))),
    }
}

fn take_bytes(bytes: &[u8], at: &mut usize) -> Result<Vec<u8>, FerroError> {
    let len = take_u32(bytes, at)? as usize;
    let end = at.checked_add(len).ok_or_else(|| {
        FerroError::Wal(format!("a consensus frame claims {len} bytes at offset {at}, which overflows"))
    })?;
    let slice = bytes
        .get(*at..end)
        .ok_or_else(|| crate::wal::log::short(*at, len, bytes.len()))?;
    *at = end;
    Ok(slice.to_vec())
}

fn decode_body(bytes: &[u8], at: &mut usize) -> Result<Body, FerroError> {
    Ok(match take_u8(bytes, at)? {
        K_PRE_VOTE => {
            Body::PreVote { last_term: take_u64(bytes, at)?, last_round: take_u64(bytes, at)? }
        }
        K_PRE_VOTE_RESP => Body::PreVoteResp { granted: take_bool(bytes, at)? },
        K_REQUEST_VOTE => {
            Body::RequestVote { last_term: take_u64(bytes, at)?, last_round: take_u64(bytes, at)? }
        }
        K_REQUEST_VOTE_RESP => Body::RequestVoteResp { granted: take_bool(bytes, at)? },
        K_APPEND => decode_append(bytes, at)?,
        K_APPEND_RESP => Body::AppendResp {
            success: take_bool(bytes, at)?,
            matched: take_u64(bytes, at)?,
            hint: take_u64(bytes, at)?,
            digest: take_u64(bytes, at)?,
        },
        K_INSTALL_SNAPSHOT => {
            let meta = decode_snapshot_meta(bytes, at)?;
            let offset = take_u64(bytes, at)?;
            let done = take_bool(bytes, at)?;
            let data = take_bytes(bytes, at)?;
            Body::InstallSnapshot { meta, offset, data, done }
        }
        K_INSTALL_SNAPSHOT_RESP => {
            Body::InstallSnapshotResp { received_through: take_u64(bytes, at)? }
        }
        other => {
            return Err(FerroError::Wal(format!(
                "unknown consensus message kind {other}; this build speaks kinds \
                 {K_PRE_VOTE}..={K_INSTALL_SNAPSHOT_RESP}. A peer sending a kind this build does \
                 not know is a peer built from a later protocol, and the frame is refused rather \
                 than guessed at."
            )))
        }
    })
}

fn decode_append(bytes: &[u8], at: &mut usize) -> Result<Body, FerroError> {
    let prev_round: Round = take_u64(bytes, at)?;
    let prev_term: Term = take_u64(bytes, at)?;
    let commit: Round = take_u64(bytes, at)?;
    let count = take_u32(bytes, at)? as usize;

    // **Nothing is reserved from `count`.** It is a number a peer chose, and `Vec::with_capacity`
    // on it is exactly the amplification the frame limit exists to prevent: 8 MB on the wire could
    // otherwise ask for gigabytes of address space. The loop grows the vector as entries actually
    // decode, and the first entry that runs off the end of the body ends it.
    let mut entries = Vec::new();
    for i in 0..count {
        entries.push(decode_entry(bytes, at).map_err(|e| {
            FerroError::Wal(format!("entry {i} of {count} in an Append frame: {e}"))
        })?);
    }
    Ok(Body::Append { prev_round, prev_term, entries, commit })
}

fn decode_entry(bytes: &[u8], at: &mut usize) -> Result<Entry, FerroError> {
    let term: Term = take_u64(bytes, at)?;
    let round: Round = take_u64(bytes, at)?;
    Ok(Entry { term, round, command: decode_command(bytes, at)? })
}

fn decode_command(bytes: &[u8], at: &mut usize) -> Result<Command, FerroError> {
    Ok(match take_u8(bytes, at)? {
        C_WAL_BATCH => {
            Command::WalBatch { start_lsn: take_u64(bytes, at)?, bytes: take_bytes(bytes, at)? }
        }
        C_CATALOG => decode_catalog(bytes, at)?,
        C_BRANCH => Command::Branch { op: decode_branch_op(bytes, at)? },
        C_ARENA_GRANT => Command::ArenaGrant {
            node: NodeId(take_u32(bytes, at)?),
            first_page: take_u32(bytes, at)?,
            page_count: take_u32(bytes, at)?,
        },
        C_TXN_ID_RANGE => Command::TxnIdRange {
            node: NodeId(take_u32(bytes, at)?),
            lo: take_u64(bytes, at)?,
            hi: take_u64(bytes, at)?,
        },
        C_LEASE_TICK => Command::LeaseTick { unix_millis: take_u64(bytes, at)? },
        C_CHECKPOINT => Command::Checkpoint,
        C_MEMBERSHIP => Command::Membership { config: decode_config(bytes, at)? },
        C_NO_OP => Command::NoOp,
        other => {
            return Err(FerroError::Wal(format!(
                "unknown consensus command tag {other}; this build speaks tags \
                 {C_WAL_BATCH}..={C_NO_OP}. Refused rather than skipped: a command this node \
                 cannot apply is a round it must not claim to hold."
            )))
        }
    })
}

/// [`Command::Catalog`], carried as the `RecKind::Ddl` record it describes.
///
/// # The tag is checked before `RecKind::deserialize` is called, and that is load-bearing
///
/// `RecKind::deserialize`'s arms for the heap records (tags 5, 6 and 7) index their slices
/// directly — `bytes[15..15 + length]` — so a short record with one of those tags **panics**. On a
/// disk that is a corrupt page; on a socket it is a one-frame remote denial of service. Nothing in
/// a `Catalog` command is ever anything but a `Ddl`, so the tag is checked here and every other
/// value is refused before those arms can be reached.
fn decode_catalog(bytes: &[u8], at: &mut usize) -> Result<Command, FerroError> {
    let rec_bytes = take_bytes(bytes, at)?;
    match rec_bytes.first() {
        Some(&RECKIND_DDL_TAG) => {}
        Some(other) => {
            return Err(FerroError::Wal(format!(
                "a Catalog command carries a log record of kind {other}, not a Ddl record \
                 (kind {RECKIND_DDL_TAG}). Refused before deserializing it: the heap-record arms \
                 of `RecKind::deserialize` index their slices unchecked and panic on a short \
                 record, so a peer choosing the kind byte would choose whether this process lives."
            )))
        }
        None => {
            return Err(FerroError::Wal(
                "a Catalog command carries an empty log record".to_string(),
            ))
        }
    }

    // **`RecKind::deserialize` accepts trailing bytes, and this codec must not.**
    //
    // Its `Ddl` arm returns without ever comparing its cursor to the buffer's length
    // (`wal/log.rs`), so any number of bytes may follow the record and be silently discarded. The
    // WAL never notices because `read_record` hands it an exactly-sized slice — `&frame[28..total-4]`
    // — and a socket does not. Measured before this check existed: eight bytes of `"smuggled"`
    // appended to a well-formed record decoded to a `Command::Catalog` with no complaint.
    //
    // The damage is not a misread schema, it is canonicality: two byte strings would decode to one
    // command, so a receiver re-encoding what it accepted would not reproduce the bytes F7 signed.
    // Re-serializing and comparing is the total check — it catches trailing bytes and every other
    // non-canonical spelling inside the record at once — and it costs one encode of a schema
    // description.
    let decoded = RecKind::deserialize(&rec_bytes)?;
    let mut reencoded = Vec::new();
    decoded.serialize(&mut reencoded);
    if reencoded != rec_bytes {
        return Err(FerroError::Wal(format!(
            "a Catalog command's log record did not re-encode to the bytes it arrived as ({} bytes \
             in, {} out). `RecKind::deserialize` stops at its last field and ignores whatever \
             follows, so those bytes would have been discarded silently — and two byte strings \
             decoding to one command is exactly what a signature over the wire cannot survive",
            rec_bytes.len(),
            reencoded.len()
        )));
    }

    let RecKind::Ddl { op, table, dir_root, time_travel_root, columns } = decoded else {
        // Unreachable given the tag check above; written as a refusal rather than a fallback so
        // that a future `RecKind` reusing tag 9 fails here instead of silently.
        return Err(FerroError::Wal(
            "a Catalog command's log record has the Ddl tag but did not deserialize as one"
                .to_string(),
        ));
    };

    // **A page id must not cross this wire.** `Command::Catalog` is deliberately logical: each node
    // applies the DDL and computes its own roots, because shipping the leader's would make every
    // follower's catalog a copy of the leader's physical layout — the same mistake as agreeing on
    // byte offsets. The encoder writes zeroes; anything else means the sender put a node-local page
    // id in a replicated decision, and the follower that applied it would point its catalog at a
    // page that means something else here.
    if dir_root != 0 || time_travel_root != 0 {
        return Err(FerroError::Wal(format!(
            "a Catalog command carries dir_root={dir_root} and time_travel_root={time_travel_root}; \
             a replicated schema change must carry no page ids at all, because a page id is \
             node-local and applying the leader's would point this node's catalog at one of its own \
             pages that holds something else entirely"
        )));
    }

    Ok(Command::Catalog { op, table, columns })
}

fn decode_branch_op(bytes: &[u8], at: &mut usize) -> Result<BranchOp, FerroError> {
    Ok(match take_u8(bytes, at)? {
        B_FORK => BranchOp::Fork {
            child: take_u64(bytes, at)?,
            parent: take_u64(bytes, at)?,
            fork_epoch: take_u64(bytes, at)?,
            lease_millis: take_u64(bytes, at)?,
        },
        B_MERGE => {
            BranchOp::Merge { branch: take_u64(bytes, at)?, base_round: take_u64(bytes, at)? }
        }
        B_ABANDON => BranchOp::Abandon { branch: take_u64(bytes, at)? },
        B_REAP => BranchOp::Reap { branch: take_u64(bytes, at)?, generation: take_u32(bytes, at)? },
        other => {
            return Err(FerroError::Wal(format!(
                "unknown branch op tag {other}; this build speaks tags {B_FORK}..={B_REAP}"
            )))
        }
    })
}

/// A node list, refused unless it is strictly ascending.
///
/// **Strictly**, so it is sorted *and* free of duplicates in one comparison. Both matter and for the
/// same reason: `Config::new` sorts and de-duplicates on construction, so no correct sender can
/// produce anything else, and a list that repeats a node would — in any decoder that counted before
/// de-duplicating — inflate `len()` and therefore the quorum. One node counted twice is one vote
/// counted twice, which is two leaders of one term arriving through the front door.
///
/// Refused rather than quietly canonicalised, because canonicalising means the bytes that arrived
/// are not the bytes this node would re-emit, and F7's MAC is taken over bytes.
fn decode_node_list(bytes: &[u8], at: &mut usize, what: &str) -> Result<Vec<NodeId>, FerroError> {
    let count = take_u32(bytes, at)? as usize;
    // Not pre-allocated from `count`, for the reason given in `decode_append`.
    let mut out: Vec<NodeId> = Vec::new();
    for _ in 0..count {
        let n = NodeId(take_u32(bytes, at)?);
        if let Some(prev) = out.last() {
            if n <= *prev {
                return Err(FerroError::Wal(format!(
                    "a configuration's {what} list is not strictly ascending: {prev} is followed \
                     by {n}. A correct sender's list is sorted and de-duplicated by \
                     `Config::new`, so this frame either was not written by one or was altered in \
                     flight; a repeated member would enlarge the quorum's denominator without \
                     enlarging the set that can answer it."
                )));
            }
        }
        out.push(n);
    }
    Ok(out)
}

fn decode_config(bytes: &[u8], at: &mut usize) -> Result<Config, FerroError> {
    let version = take_u64(bytes, at)?;
    let term = take_u64(bytes, at)?;
    let members = decode_node_list(bytes, at, "member")?;
    let learners = decode_node_list(bytes, at, "learner")?;

    // A node cannot be both. `with_learners` drops such a learner silently, which is the right
    // thing for a *builder* — the voter entry is the stronger statement — and the wrong thing for a
    // decoder, because the config this node would then hold is not the one the leader sent.
    if let Some(n) = learners.iter().find(|n| members.contains(n)) {
        return Err(FerroError::Wal(format!(
            "a configuration lists {n} as both a voter and a learner. Accepting it would mean this \
             node holds a different configuration from the one the leader replicated, while both \
             claim version {version}"
        )));
    }

    let cfg = Config::new(members.iter().copied(), version, term).with_learners(learners.clone());

    // **The constructor is the authority, and this is the check that it agreed.** `Config::new`
    // sorts and de-duplicates and `with_learners` drops voters from the learner list; the rules
    // above were written to make all three no-ops for well-formed bytes. Comparing the result back
    // against the wire is what keeps that true if `config.rs` ever changes its normalisation —
    // rather than this file silently disagreeing with it about what a configuration is.
    if cfg.members() != members.as_slice() || cfg.learners() != learners.as_slice() {
        return Err(FerroError::Wal(format!(
            "a configuration changed under `Config::new`: {:?}/{:?} on the wire became {:?}/{:?}. \
             The wire format and `config.rs` disagree about what a configuration is, and a node \
             that accepted this would be counting a majority against a different set from the one \
             the leader counted",
            members,
            learners,
            cfg.members(),
            cfg.learners()
        )));
    }
    Ok(cfg)
}

fn decode_snapshot_meta(bytes: &[u8], at: &mut usize) -> Result<SnapshotMeta, FerroError> {
    let last_round = take_u64(bytes, at)?;
    let last_term = take_u64(bytes, at)?;
    let total_bytes = take_u64(bytes, at)?;
    Ok(SnapshotMeta { last_round, last_term, total_bytes, config: decode_config(bytes, at)? })
}

// ---------------------------------------------------------------------------------------------
// Reading frames off a socket
// ---------------------------------------------------------------------------------------------

/// What one non-blocking poll of a socket produced.
#[derive(Debug)]
enum Poll {
    /// Nothing yet, or part of a frame. Call again.
    Pending,
    /// A complete frame: its tag and its body.
    Frame(u8, Vec<u8>),
    /// The peer closed cleanly, between frames.
    Eof,
}

/// A framed reader that survives a socket read timeout **without losing its place**.
///
/// # Why this exists rather than `read_exact`
///
/// `Read::read_exact` gives no way to find out how many bytes it consumed before it failed, and its
/// contract says the buffer contents are unspecified on error. On a `TcpStream` carrying a read
/// timeout that is not a theoretical problem: a frame that straddles the timeout leaves bytes
/// consumed from the kernel and unaccounted for in the reader, and every frame after it is parsed
/// from the wrong offset. The stream is then desynchronised in the one way a length-prefixed
/// protocol cannot detect — the next `tag` byte is whatever the middle of the last frame happened
/// to hold.
///
/// A read timeout is not optional here: it is how a per-connection thread notices that the
/// transport is shutting down while it is blocked in `read`. So the reader has to be resumable, and
/// this is it. Progress is accumulated in `self`; a timeout returns [`Poll::Pending`] with every
/// byte so far still held.
struct FrameReader {
    header: [u8; 5],
    header_got: usize,
    body: Vec<u8>,
    /// How long this frame's body is, from the header. Meaningful only once `header_got == 5`.
    body_want: usize,
    /// How much of it has actually arrived. **This is the field a timeout must not lose**, and the
    /// whole reason the reader is a struct rather than a `read_exact`.
    body_have: usize,
}

impl FrameReader {
    fn new() -> Self {
        FrameReader { header: [0u8; 5], header_got: 0, body: Vec::new(), body_want: 0, body_have: 0 }
    }

    /// One `read` call's worth of progress.
    fn poll(&mut self, r: &mut impl Read) -> Result<Poll, FerroError> {
        if self.header_got < 5 {
            match read_some(r, &mut self.header[self.header_got..])? {
                Some(0) => {
                    return if self.header_got == 0 {
                        // Closed between frames: the ordinary end of a connection.
                        Ok(Poll::Eof)
                    } else {
                        Err(FerroError::Wal(format!(
                            "the peer closed after {} byte(s) of a 5-byte frame header; a frame \
                             that stops inside its own length is refused rather than completed \
                             from whatever arrives next",
                            self.header_got
                        )))
                    };
                }
                Some(n) => {
                    self.header_got += n;
                    if self.header_got < 5 {
                        return Ok(Poll::Pending);
                    }
                    let len = u32::from_be_bytes(self.header[1..5].try_into().unwrap()) as usize;
                    // Checked **before** the allocation, exactly as `replication::Message::read_from`
                    // does: a peer must not get to choose this process's memory usage.
                    if len > MAX_FRAME_BYTES {
                        return Err(FerroError::Wal(format!(
                            "a consensus frame claims {len} bytes, over the {MAX_FRAME_BYTES} limit"
                        )));
                    }
                    // `len` is already known to be at most MAX_FRAME_BYTES, so this allocation is
                    // bounded by the limit and not by anything the peer chose.
                    self.body_want = len;
                    self.body_have = 0;
                    self.body = vec![0u8; len];
                }
                None => return Ok(Poll::Pending),
            }
        }

        if self.body_have < self.body_want {
            match read_some(r, &mut self.body[self.body_have..])? {
                Some(0) => {
                    return Err(FerroError::Wal(format!(
                        "the peer closed {} byte(s) into a {}-byte frame body",
                        self.body_have, self.body_want
                    )))
                }
                Some(n) => {
                    self.body_have += n;
                    if self.body_have < self.body_want {
                        return Ok(Poll::Pending);
                    }
                }
                None => return Ok(Poll::Pending),
            }
        }

        let tag = self.header[0];
        let body = std::mem::take(&mut self.body);
        self.header_got = 0;
        self.body_want = 0;
        self.body_have = 0;
        Ok(Poll::Frame(tag, body))
    }
}

/// One `read`, with the three retryable conditions folded into `Ok(None)`.
///
/// `WouldBlock` and `TimedOut` are the same event on different platforms — a socket read timeout
/// expiring — and `Interrupted` is a signal. None of the three means anything was lost, so all
/// three become "call again", and only a real error is an error.
fn read_some(r: &mut impl Read, into: &mut [u8]) -> Result<Option<usize>, FerroError> {
    match r.read(into) {
        Ok(n) => Ok(Some(n)),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(FerroError::Io(e.to_string())),
    }
}

// ---------------------------------------------------------------------------------------------
// The handshake, gathered timeout-safely and validated by `replication`'s own rule
// ---------------------------------------------------------------------------------------------

/// The six handshake bytes, read without `read_exact` for the reason [`FrameReader`] gives, then
/// handed to [`crate::replication::read_handshake`] to be judged.
///
/// The *gathering* is this module's problem because it owns the socket's timeout; the *rule* is
/// `replication`'s and there is exactly one copy of it. A second magic-and-version check here is a
/// second place for the version to be wrong.
fn recv_handshake(
    stream: &mut TcpStream,
    stop: &AtomicBool,
    deadline: Duration,
) -> Result<(), FerroError> {
    let mut buf = [0u8; 6];
    let mut got = 0usize;
    let started = Instant::now();
    while got < 6 {
        if stop.load(Ordering::SeqCst) {
            return Err(FerroError::Io("transport is shutting down".into()));
        }
        if started.elapsed() > deadline {
            return Err(FerroError::Wal(format!(
                "a peer connected and sent {got} of 6 handshake bytes before the handshake \
                 deadline; closed rather than held open, so a peer that never speaks cannot pin a \
                 thread"
            )));
        }
        match read_some(stream, &mut buf[got..])? {
            Some(0) => {
                return Err(FerroError::Io(
                    "the peer closed during the handshake".to_string(),
                ))
            }
            Some(n) => got += n,
            None => continue,
        }
    }
    read_handshake(&mut Cursor::new(&buf[..]))
}

fn send_handshake(stream: &mut TcpStream) -> Result<(), FerroError> {
    let mut buf = Vec::with_capacity(6);
    write_handshake(&mut buf)?;
    stream.write_all(&buf).map_err(|e| FerroError::Io(e.to_string()))?;
    stream.flush().map_err(|e| FerroError::Io(e.to_string()))
}

// ---------------------------------------------------------------------------------------------
// The transport
// ---------------------------------------------------------------------------------------------

/// Knobs, all of them with a defensible default.
///
/// Every one is a real parameter rather than a tuning guess: `queue_depth` is how much a peer may
/// fall behind before this node starts dropping to it, `poll_interval` is how long a thread may
/// stay blocked in `read` after a shutdown has been asked for, and `reconnect_delay` is how hard
/// this node retries a peer that is down.
#[derive(Debug, Clone)]
pub struct TransportOptions {
    /// Messages held for one peer before the **oldest** is dropped. See [`Transport::send`].
    pub queue_depth: usize,
    /// Socket read timeout, and the listener's accept poll. Bounds shutdown latency.
    pub poll_interval: Duration,
    /// How long a sender thread waits after a failed dial before trying that peer again.
    pub reconnect_delay: Duration,
    /// How long an accepted connection may take to finish its handshake before it is closed.
    pub handshake_deadline: Duration,
}

impl Default for TransportOptions {
    fn default() -> Self {
        TransportOptions {
            queue_depth: 1024,
            poll_interval: Duration::from_millis(50),
            reconnect_delay: Duration::from_millis(100),
            handshake_deadline: Duration::from_secs(5),
        }
    }
}

/// Everything this transport has counted. **Drops and refusals are counted, never silent** — a
/// message that vanished without a number attached is indistinguishable from a protocol bug, and
/// this transport is allowed to drop.
#[derive(Debug, Default)]
struct Counters {
    sent: AtomicU64,
    received: AtomicU64,
    misrouted: AtomicU64,
    refused_handshakes: AtomicU64,
    connect_failures: AtomicU64,
}

/// One peer's queue and its current connection.
struct Outbox {
    addr: SocketAddr,
    state: Mutex<OutboxState>,
    woken: Condvar,
    depth: usize,
    dropped: AtomicU64,
}

struct OutboxState {
    queue: VecDeque<Vec<u8>>,
    /// A clone of the live socket, held **only** so that [`Transport::shutdown`] can call
    /// `shutdown(Both)` on it. A sender thread parked in `write_all` against a peer whose receive
    /// window is full is otherwise unreachable, and joining it would hang for ever.
    live: Option<TcpStream>,
    stopped: bool,
}

impl Outbox {
    /// Enqueue, dropping the **oldest** if this peer is already `depth` behind.
    ///
    /// Oldest and not newest, deliberately. Consensus messages are cumulative: a later `Append`
    /// carries a higher `commit` and later entries, and a later heartbeat supersedes an earlier
    /// one, so a peer that can only be told one thing should be told the newest. Dropping the newest
    /// would leave the queue holding a stale view and never catch up.
    fn push(&self, frame: Vec<u8>) {
        let mut st = self.state.lock().unwrap();
        if st.stopped {
            return;
        }
        while st.queue.len() >= self.depth {
            st.queue.pop_front();
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
        st.queue.push_back(frame);
        self.woken.notify_all();
    }
}

/// Thread-per-peer carriage for consensus messages, over `std::net`.
///
/// One thread per *outbound* peer, holding that peer's queue and its connection, plus one accept
/// thread and one thread per *inbound* connection — the same shape as `pgwire::serve`, which spawns
/// a thread per connection and lets the connection's lifetime be the client's business.
///
/// Inbound messages arrive on a channel; the caller drains it and feeds
/// [`super::Event::Recv`] to its `Consensus`. The transport never touches the state machine, for
/// the same reason the state machine never touches a socket.
pub struct Transport {
    self_id: NodeId,
    local_addr: SocketAddr,
    outboxes: BTreeMap<NodeId, Arc<Outbox>>,
    /// Behind a `Mutex` so `Transport` is `Sync` and can live in an `Arc`: an `mpsc::Receiver` is
    /// `Send` but not `Sync`, and a driver that sends from one thread and receives on another is
    /// the ordinary arrangement.
    inbox: Mutex<mpsc::Receiver<Message>>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// Every **live** inbound socket, so shutdown can unblock their readers at once instead of
    /// waiting a `poll_interval` for each.
    ///
    /// Keyed, and each connection removes its own entry on the way out. A plain `Vec` pushed onto
    /// and never drained leaks a file descriptor per connection: `try_clone` dups the descriptor,
    /// and a clone left behind after its thread has returned keeps that descriptor allocated for
    /// the life of the process. Since a reconnect is how this transport recovers from any write
    /// failure, that is the ordinary path and not a pathological one — a long-lived node would
    /// reach `EMFILE` and start refusing connections for reasons nothing in the cluster explains.
    inbound_conns: Arc<Mutex<BTreeMap<u64, TcpStream>>>,
}

/// Removes a connection from the live registry however its thread leaves — including the early
/// return on a refused handshake.
///
/// A `Drop` guard rather than a call at the end of `conn_loop`, because "every exit path also
/// deregisters" is exactly the invariant a later edit breaks by adding one more `return`.
struct ConnRegistration {
    id: u64,
    conns: Arc<Mutex<BTreeMap<u64, TcpStream>>>,
}

impl Drop for ConnRegistration {
    fn drop(&mut self) {
        self.conns.lock().unwrap().remove(&self.id);
    }
}

impl Transport {
    /// Bind a listener and start one sender thread per peer.
    ///
    /// `peers` must not contain `self_id`: a node does not send itself messages, and an entry for
    /// itself would be a thread dialling its own listener for ever.
    pub fn bind(
        self_id: NodeId,
        listen: impl ToSocketAddrs,
        peers: BTreeMap<NodeId, SocketAddr>,
        opts: TransportOptions,
    ) -> Result<Transport, FerroError> {
        let listener = TcpListener::bind(listen).map_err(|e| FerroError::Io(e.to_string()))?;
        Transport::from_listener(self_id, listener, peers, opts)
    }

    /// The same, over a listener the caller already holds.
    ///
    /// The primitive, with [`Transport::bind`] as the convenience over it. Two reasons it is the
    /// primitive rather than an afterthought: a node whose socket came from elsewhere — socket
    /// activation, a supervisor — has one already; and **a cluster cannot be stood up race-free any
    /// other way.** Every node's peer map needs every other node's address, so binding node A to
    /// discover its port and then constructing node B is circular. Reserving the listeners first
    /// and handing them over closes it, with no window in which a port is published but unowned.
    pub fn from_listener(
        self_id: NodeId,
        listener: TcpListener,
        peers: BTreeMap<NodeId, SocketAddr>,
        opts: TransportOptions,
    ) -> Result<Transport, FerroError> {
        if peers.contains_key(&self_id) {
            return Err(FerroError::Internal(format!(
                "the peer map for {self_id} contains {self_id} itself; a node does not send to \
                 itself, and a sender thread for that entry would dial this process's own listener \
                 for ever"
            )));
        }
        for (name, d) in [
            ("poll_interval", opts.poll_interval),
            ("reconnect_delay", opts.reconnect_delay),
            ("handshake_deadline", opts.handshake_deadline),
        ] {
            // std documents a zero duration as an error for both `set_read_timeout` and
            // `connect_timeout`, so a zero here does not mean "no wait" — it means every socket
            // call fails with `invalid input`, and the node reports itself unable to reach anyone
            // for a reason that names nothing about the cluster. Refused where the value is set.
            if d.is_zero() {
                return Err(FerroError::Internal(format!(
                    "`{name}` is zero; std refuses a zero socket or connect timeout, so every \
                     connection this node made or accepted would fail at a call reporting \
                     `invalid input` rather than anything about the peer"
                )));
            }
        }
        if opts.queue_depth == 0 {
            return Err(FerroError::Internal(
                "a queue depth of 0 would drop every message on the way out, which is a \
                 partitioned node that reports itself healthy"
                    .to_string(),
            ));
        }

        let local_addr = listener.local_addr().map_err(|e| FerroError::Io(e.to_string()))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| FerroError::Io(e.to_string()))?;

        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(Counters::default());
        let inbound_conns: Arc<Mutex<BTreeMap<u64, TcpStream>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        // Lives only in the accept thread, which is the only place a connection id is minted.
        let next_conn_id = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel::<Message>();
        let mut threads = Vec::new();

        let mut outboxes = BTreeMap::new();
        for (peer, addr) in peers {
            let ob = Arc::new(Outbox {
                addr,
                state: Mutex::new(OutboxState {
                    queue: VecDeque::new(),
                    live: None,
                    stopped: false,
                }),
                woken: Condvar::new(),
                depth: opts.queue_depth,
                dropped: AtomicU64::new(0),
            });
            outboxes.insert(peer, Arc::clone(&ob));
            let stop_c = Arc::clone(&stop);
            let counters_c = Arc::clone(&counters);
            let opts_c = opts.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("consensus-out-{self_id}-to-{peer}"))
                    .spawn(move || sender_loop(ob, stop_c, counters_c, opts_c))
                    .map_err(|e| FerroError::Io(e.to_string()))?,
            );
        }

        // The accept thread owns the per-connection threads it spawns, and joins them before it
        // returns. Nothing here is detached: a transport that has been shut down must have no
        // thread still holding a socket, or a test that binds a fresh port after one is a test
        // racing the previous run.
        let stop_c = Arc::clone(&stop);
        let counters_c = Arc::clone(&counters);
        let conns_c = Arc::clone(&inbound_conns);
        let ids_c = next_conn_id;
        let opts_c = opts.clone();
        threads.push(
            std::thread::Builder::new()
                .name(format!("consensus-accept-{self_id}"))
                .spawn(move || {
                    accept_loop(listener, self_id, tx, stop_c, counters_c, conns_c, ids_c, opts_c)
                })
                .map_err(|e| FerroError::Io(e.to_string()))?,
        );

        Ok(Transport {
            self_id,
            local_addr,
            outboxes,
            inbox: Mutex::new(rx),
            counters,
            stop,
            threads: Mutex::new(threads),
            inbound_conns,
        })
    }

    /// The address this node is actually listening on — the resolved one, so a caller that bound
    /// port 0 can publish it.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn id(&self) -> NodeId {
        self.self_id
    }

    /// Queue one message for its addressee. **Never blocks**, and never blocks the state machine.
    ///
    /// The message is encoded here rather than on the sender thread, so a body that cannot be
    /// framed is refused to the caller — `replicate.rs` can then send fewer entries — instead of
    /// being swallowed on a background thread where nothing would ever see the error.
    ///
    /// Refuses rather than drops for the two cases that are configuration mistakes rather than
    /// network conditions: a message to this node itself, and a message to a node with no address.
    /// Both are silent partitions if they are dropped, and a silent partition is the failure this
    /// whole layer exists to make impossible.
    pub fn send(&self, m: &Message) -> Result<(), FerroError> {
        if m.to == self.self_id {
            return Err(FerroError::Internal(format!(
                "{} tried to send a consensus message to itself; the state machine addresses peers \
                 only, and a self-addressed message means a handler used the wrong id",
                self.self_id
            )));
        }
        let ob = self.outboxes.get(&m.to).ok_or_else(|| {
            FerroError::Internal(format!(
                "no address is configured for {}, so a message to it cannot be sent. This node \
                 holds addresses for {:?}. A configuration that names a node the transport cannot \
                 reach is a node permanently unreachable while every meter reads healthy — add it \
                 to the peer map",
                m.to,
                self.outboxes.keys().collect::<Vec<_>>()
            ))
        })?;
        let frame = encode(m)?;
        ob.push(frame);
        self.counters.sent.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// The next message that has arrived, or `None` if none has.
    pub fn try_recv(&self) -> Option<Message> {
        self.inbox.lock().unwrap().try_recv().ok()
    }

    /// The next message, waiting up to `d`. `None` means nothing arrived in that window — which is
    /// an ordinary and expected answer, not an error.
    pub fn recv_timeout(&self, d: Duration) -> Option<Message> {
        self.inbox.lock().unwrap().recv_timeout(d).ok()
    }

    /// Messages handed to a peer's queue.
    pub fn sent(&self) -> u64 {
        self.counters.sent.load(Ordering::SeqCst)
    }
    /// Messages dropped because a peer was `queue_depth` behind. See [`Transport::send`].
    pub fn dropped(&self) -> u64 {
        self.outboxes.values().map(|o| o.dropped.load(Ordering::SeqCst)).sum()
    }
    /// Dropped for one peer, so a caller can tell one slow follower from a cluster-wide problem.
    pub fn dropped_to(&self, peer: NodeId) -> u64 {
        self.outboxes.get(&peer).map_or(0, |o| o.dropped.load(Ordering::SeqCst))
    }
    /// Messages decoded and delivered to the inbox.
    pub fn received(&self) -> u64 {
        self.counters.received.load(Ordering::SeqCst)
    }
    /// Messages that decoded but were addressed to a different node, and were therefore refused.
    pub fn misrouted(&self) -> u64 {
        self.counters.misrouted.load(Ordering::SeqCst)
    }
    /// Whether [`Transport::shutdown`] has run.
    ///
    /// `recv_timeout` returning `None` means "nothing arrived in that window" while the transport
    /// is live and "nothing ever will" after it is stopped, and a caller looping on it needs to
    /// tell those apart — a quiet cluster and a closed transport are not the same fact.
    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// How many inbound connections are currently registered. The meter that makes the descriptor
    /// leak visible: it must fall back to zero as peers disconnect, not climb with every connection
    /// this node has ever accepted.
    pub fn live_inbound_conns(&self) -> usize {
        self.inbound_conns.lock().unwrap().len()
    }

    /// Connections closed at the handshake — a wrong magic, or a peer speaking another version.
    pub fn refused_handshakes(&self) -> u64 {
        self.counters.refused_handshakes.load(Ordering::SeqCst)
    }
    /// Failed dials. A peer that is down makes this climb steadily; it is the meter that says
    /// "unreachable" rather than "quiet".
    pub fn connect_failures(&self) -> u64 {
        self.counters.connect_failures.load(Ordering::SeqCst)
    }

    /// Stop every thread and close every socket. Idempotent, and called from [`Drop`].
    ///
    /// Sockets are shut down explicitly rather than left to time out: a thread parked in `read` or
    /// `write_all` does not notice a flag, and joining it would hang until the peer happened to
    /// send something. `shutdown(Both)` makes both calls return at once, and the `poll_interval`
    /// read timeout is the backstop for anything that slips between the flag and the shutdown.
    ///
    /// **One bound this cannot beat, stated rather than left to be found:** a sender thread already
    /// inside `TcpStream::connect_timeout` against a black-holed peer — one that neither accepts
    /// nor refuses — has no socket to shut down yet, so it is not interruptible and this call waits
    /// up to one `handshake_deadline` for it. That is bounded and it is the reason
    /// `handshake_deadline` is a knob rather than a constant. It is not shortened here, because the
    /// alternative is failing a legitimately slow connect, and a slow shutdown is the cheaper
    /// failure.
    pub fn shutdown(&self) {
        if self.stop.swap(true, Ordering::SeqCst) {
            // Already shut down. Still join below, so a second call is a barrier rather than a
            // no-op that returns while threads are alive.
        }
        for ob in self.outboxes.values() {
            let mut st = ob.state.lock().unwrap();
            st.stopped = true;
            st.queue.clear();
            if let Some(s) = st.live.take() {
                let _ = s.shutdown(Shutdown::Both);
            }
            ob.woken.notify_all();
        }
        for (_, s) in std::mem::take(&mut *self.inbound_conns.lock().unwrap()) {
            let _ = s.shutdown(Shutdown::Both);
        }
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *self.threads.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Hand-written rather than derived: the meters are what a reader of a log or a failing assertion
/// wants, and the queues, sockets and join handles are noise that would bury them.
impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("self_id", &self.self_id)
            .field("local_addr", &self.local_addr)
            .field("peers", &self.outboxes.keys().collect::<Vec<_>>())
            .field("sent", &self.sent())
            .field("dropped", &self.dropped())
            .field("received", &self.received())
            .field("misrouted", &self.misrouted())
            .field("refused_handshakes", &self.refused_handshakes())
            .field("connect_failures", &self.connect_failures())
            .field("stopped", &self.stop.load(Ordering::SeqCst))
            .finish()
    }
}

/// One peer's thread: keep a connection up, and write what the queue holds.
fn sender_loop(
    ob: Arc<Outbox>,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    opts: TransportOptions,
) {
    let mut conn: Option<TcpStream> = None;
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        if conn.is_none() {
            match dial(ob.addr, &opts) {
                Ok(s) => {
                    match s.try_clone() {
                        Ok(c) => {
                            let mut st = ob.state.lock().unwrap();
                            if st.stopped {
                                break;
                            }
                            st.live = Some(c);
                        }
                        Err(_) => {
                            counters.connect_failures.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                    }
                    conn = Some(s);
                }
                Err(_) => {
                    counters.connect_failures.fetch_add(1, Ordering::SeqCst);
                    // Wait on the condvar rather than sleeping, so a shutdown does not have to
                    // wait out a reconnect delay it has already made pointless.
                    let st = ob.state.lock().unwrap();
                    let _ = ob.woken.wait_timeout(st, opts.reconnect_delay);
                    continue;
                }
            }
        }

        // Take one frame, waiting a bounded time so the stop flag is checked regularly.
        let frame = {
            let mut st = ob.state.lock().unwrap();
            while st.queue.is_empty() && !st.stopped && !stop.load(Ordering::SeqCst) {
                let (g, timed_out) = ob.woken.wait_timeout(st, opts.poll_interval).unwrap();
                st = g;
                if timed_out.timed_out() {
                    break;
                }
            }
            if st.stopped {
                break;
            }
            st.queue.pop_front()
        };
        let Some(frame) = frame else { continue };

        let s = conn.as_mut().expect("connected above");
        // A write that fails part way through has left a partial frame on the wire, and there is no
        // way to resume it — the peer's reader is now inside a frame that will never finish. So the
        // connection is dropped, which is what makes the peer's reader see EOF and reset. The frame
        // is lost, and that is the documented policy of this transport: consensus re-sends.
        if s.write_all(&frame).and_then(|()| s.flush()).is_err() {
            let mut st = ob.state.lock().unwrap();
            if let Some(old) = st.live.take() {
                let _ = old.shutdown(Shutdown::Both);
            }
            drop(st);
            conn = None;
        }
    }

    let mut st = ob.state.lock().unwrap();
    if let Some(s) = st.live.take() {
        let _ = s.shutdown(Shutdown::Both);
    }
    drop(st);
    drop(conn);
}

/// Dial a peer and complete the handshake as the **connecting** side: write ours, then read theirs.
///
/// That ordering is `examples/repl_replica.rs`'s, and the accepting side's is
/// `examples/repl_primary.rs`'s — read then write. Keeping the two halves as they already are is
/// what stops this from being a third convention on one wire.
fn dial(addr: SocketAddr, opts: &TransportOptions) -> Result<TcpStream, FerroError> {
    let mut s = TcpStream::connect_timeout(&addr, opts.handshake_deadline)
        .map_err(|e| FerroError::Io(e.to_string()))?;
    s.set_read_timeout(Some(opts.poll_interval)).map_err(|e| FerroError::Io(e.to_string()))?;
    // Nagle off: consensus frames are small and latency-sensitive, and a delayed heartbeat is a
    // spurious election.
    let _ = s.set_nodelay(true);
    send_handshake(&mut s)?;

    let mut buf = [0u8; 6];
    let mut got = 0usize;
    let started = Instant::now();
    while got < 6 {
        if started.elapsed() > opts.handshake_deadline {
            return Err(FerroError::Wal(
                "a peer accepted the connection but did not answer the handshake".to_string(),
            ));
        }
        match read_some(&mut s, &mut buf[got..])? {
            Some(0) => {
                return Err(FerroError::Io("the peer closed during the handshake".to_string()))
            }
            Some(n) => got += n,
            None => continue,
        }
    }
    read_handshake(&mut Cursor::new(&buf[..]))?;
    Ok(s)
}

#[allow(clippy::too_many_arguments)]
fn accept_loop(
    listener: TcpListener,
    self_id: NodeId,
    tx: mpsc::Sender<Message>,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    conns: Arc<Mutex<BTreeMap<u64, TcpStream>>>,
    ids: Arc<AtomicU64>,
    opts: TransportOptions,
) {
    let mut conn_threads: Vec<JoinHandle<()>> = Vec::new();
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                // An accepted socket's blocking mode is not portably inherited from its listener,
                // so it is set explicitly rather than assumed. The read timeout is what lets the
                // connection thread notice a shutdown.
                if stream.set_nonblocking(false).is_err()
                    || stream.set_read_timeout(Some(opts.poll_interval)).is_err()
                {
                    continue;
                }
                let _ = stream.set_nodelay(true);
                let tx_c = tx.clone();
                let stop_c = Arc::clone(&stop);
                let counters_c = Arc::clone(&counters);
                let conns_c = Arc::clone(&conns);
                let opts_c = opts.clone();
                let id = ids.fetch_add(1, Ordering::SeqCst);
                match std::thread::Builder::new()
                    .name(format!("consensus-in-{self_id}"))
                    .spawn(move || {
                        conn_loop(stream, id, self_id, tx_c, stop_c, counters_c, conns_c, opts_c)
                    }) {
                    Ok(h) => conn_threads.push(h),
                    Err(_) => continue,
                }
                // Reap finished connection threads so a long-lived node does not accumulate
                // handles for every connection it has ever accepted.
                conn_threads.retain(|h| !h.is_finished());
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(opts.poll_interval);
            }
            Err(_) => std::thread::sleep(opts.poll_interval),
        }
    }
    // Joined, not detached: `Transport::shutdown` joins this thread, and it must not return while
    // a connection thread it spawned still holds a socket.
    for h in conn_threads {
        let _ = h.join();
    }
}

fn conn_loop(
    mut stream: TcpStream,
    id: u64,
    self_id: NodeId,
    tx: mpsc::Sender<Message>,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    conns: Arc<Mutex<BTreeMap<u64, TcpStream>>>,
    opts: TransportOptions,
) {
    // Registered before the handshake, so a peer that connects and then goes silent is still
    // reachable by `shutdown` rather than pinned until its deadline. The guard deregisters on every
    // exit path, including the early return below.
    let _registration = ConnRegistration { id, conns: Arc::clone(&conns) };
    if let Ok(c) = stream.try_clone() {
        conns.lock().unwrap().insert(id, c);
    }

    let verdict = recv_handshake(&mut stream, &stop, opts.handshake_deadline);

    // **Our handshake is written whatever the verdict, and that is the point of the version bump.**
    // A v1 peer is mid-`read_handshake` right now; giving it our six bytes is what makes its own
    // code say "replication protocol version 2; this build speaks 1" — the incompatibility named at
    // the handshake, by the peer, in its own words. Closing silently instead would give it
    // "failed to fill whole buffer", which describes nothing.
    let _ = send_handshake(&mut stream);

    if let Err(e) = verdict {
        counters.refused_handshakes.fetch_add(1, Ordering::SeqCst);
        // A courtesy for a peer that reads on: an `Error` frame, whose 'E' tag is a **v1** tag, so
        // even a peer that does not know version 2 can read it.
        let note = crate::replication::Message::Error {
            message: format!(
                "this node speaks ferrodb replication version {REPL_VERSION} (consensus); the \
                 handshake was refused: {e}"
            ),
        };
        let _ = note.write_to(&mut stream);
        let _ = stream.shutdown(Shutdown::Both);
        return;
    }

    let mut reader = FrameReader::new();
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        match reader.poll(&mut stream) {
            Ok(Poll::Pending) => continue,
            Ok(Poll::Eof) => break,
            Ok(Poll::Frame(tag, body)) => {
                if tag != CONSENSUS_TAG {
                    // A frame this listener cannot route. The connection is closed rather than
                    // skipped: a stream carrying a tag we do not know is a stream we cannot claim
                    // to be reading correctly.
                    break;
                }
                match decode(&body) {
                    Ok(m) => {
                        if m.to != self_id {
                            // Not authentication — `from` is still only a claim, and F7 owns that.
                            // This catches the configuration mistake where two nodes were given one
                            // address, which otherwise shows up as one node mysteriously voting
                            // twice.
                            counters.misrouted.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                        counters.received.fetch_add(1, Ordering::SeqCst);
                        if tx.send(m).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
}

#[cfg(test)]
#[path = "tests_transport.rs"]
mod tests;
