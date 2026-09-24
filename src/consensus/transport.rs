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
//! **It authenticates every frame, but only if it was given a key.** F7 landed, and
//! [`Transport::from_listener_with_key`] is the constructor that turns it on: every outbound frame
//! carries an HMAC-SHA256 tag over its own body, and every inbound frame is verified *before*
//! [`decode`] parses it, so a peer that cannot produce a tag never reaches the state machine. See
//! [`super::signing`] for what that proves — possession of the key — and, just as importantly, what
//! it does not: **freshness**. There is no replay protection.
//!
//! [`Transport::from_listener`] and [`Transport::bind`] construct an **unsigned** transport, and
//! that is still a real configuration: `Message::from` is then the sender's claim and nothing here
//! checks it, so an unauthenticated peer that can reach the port can assert any term and demote a
//! healthy leader. Two separate constructors rather than an option with a default, because a key is
//! not a knob with a defensible default — a `..Default::default()` that silently left signing off
//! is exactly how a security posture gets switched off by an unrelated edit.
//!
//! **Connection establishment is not authenticated either way.** The six-byte handshake carries no
//! tag, so anything that can reach the port can occupy an inbound connection slot; what bounds that
//! is `max_inbound_conns`, not the key. The first frame that fails to verify closes the connection.
//!
//! **It does not retry, order, or deduplicate.** Consensus is specified against a network that
//! drops, reorders and duplicates — that is why `Consensus` re-sends on a refusal and backs a peer
//! up by `hint` rather than assuming delivery. So this transport is allowed to drop, and
//! [`Transport::send`] never blocks the state machine: a leader that blocked writing to one
//! partitioned follower would stop heartbeating the healthy majority, turning one node's failure
//! into the cluster's.
//!
//! **Every loss this transport decides on has its own counter**, because an invisible drop is
//! indistinguishable from a protocol bug, and because the causes call for different actions:
//! [`Transport::dropped`] means a peer is too slow to keep up,
//! [`Transport::lost_in_flight`] means a connection broke mid-frame, and
//! [`Transport::inbound_dropped`] means *this* node is not draining its own inbox. A send after
//! shutdown is **refused** rather than dropped, because a caller still producing `Action::Send`
//! after stopping its transport has a bug rather than a slow peer. Every refused send is counted too
//! (D223): after shutdown, misaddressed, or unencodable, in [`Transport::refused_after_stop`],
//! [`Transport::unaddressable`] and [`Transport::unencodable`]. The caller in `node.rs` discards the
//! error, so the count is the only trace a refusal leaves.
//!
//! **What is not counted, because it cannot be.** This used to say *every* way a message could be
//! lost had a counter, and that was never true. A frame handed to the kernel is not tracked any
//! further: TCP gives no delivery receipt. When a connection dies, whatever sat in its buffers is
//! lost with no number attached, and only the frame whose own write failed reaches `lost_in_flight`.
//! That includes the first frame written after the peer closed its end, which is exactly what the
//! peer's `idle_deadline` does. The write succeeds locally, the peer answers with a reset, and it is
//! the next write that fails. Since D224 a sender probes a link it has left idle before writing to
//! it, and redials if the peer closed it ([`Transport::idle_probes`], [`Transport::idle_redials`]).
//! So an idle close costs frames (two, the first uncounted) only in the narrow race
//! `idle_probe_gap` describes. The same is
//! true of the receiving side. A connection closed on a frame with an
//! unknown tag, one `decode` refuses, a truncated or over-long frame, or the idle deadline discards
//! whatever the peer sends after it; of those closes, only the idle one is counted
//! ([`Transport::idle_closed`]). Consensus re-sends, so none of these is a correctness loss; they
//! are why `sent()` minus a peer's `received()` is not fully accounted for by the counters here.
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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::error::FerroError;
use crate::replication::{
    read_handshake, write_handshake, CONSENSUS_TAG, MAX_FRAME_BYTES, REPL_VERSION,
};
use crate::wal::log::{take_u32, take_u64, take_u8, RecKind};

use super::config::Config;
use super::signing::{self, Key};
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

/// Most voters, and most learners, a configuration may carry **on the wire**.
///
/// Not a limit on what a cluster may be — a limit on what a *frame* may claim it is. 1024 is far
/// above anything real: membership changes here are single-node by construction (`membership.rs`),
/// and quorum arithmetic makes a cluster of even fifty voters unusable, so a configuration naming a
/// thousand is already not a configuration but a malformed or hostile frame. Both halves of the
/// codec enforce it, so a sender is refused where the cause is visible rather than emitting a frame
/// every peer refuses.
///
/// **It is no longer what bounds the work a frame can buy, and why it once was is worth keeping.**
/// `Config::with_learners` used to drop voters from the learner list with a `Vec::contains` per
/// learner, O(learners x members): one 8 MiB frame of two million ids was on the order of 10^12
/// comparisons, and this cap was the first fix. It bounded one configuration and nothing bounded
/// how many a frame carried, so an `Append` of about 1018 maximal configurations still cost about
/// 1.07e9.
/// D207 removed the quadratic at its source instead (`config::retain_absent`, one binary search per
/// learner): a configuration now decodes in O(n log n), and a frame's decode work is linear in its
/// bytes up to that log, which [`MAX_FRAME_BYTES`] bounds. A per-frame budget on node ids was tried
/// in between and removed: it bounded one frame's latency rather than a peer's work, and it refused
/// catch-up `Append`s the leader has no way to split.
pub const MAX_CONFIG_NODES: usize = 1024;

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
    encode_signed(m, None)
}

/// The same frame, authenticated with `key` when there is one.
///
/// The tag goes **inside** the frame body, ahead of the message, so the length header still covers
/// everything after it and a reader needs no second length. What that costs is [`signing::MAC_LEN`]
/// bytes of the frame budget, which is why a message that fits unsigned can be refused signed —
/// see [`signing::sign_frame`], which is where that refusal is made and explained.
///
/// **The whole message is the authenticated region**, `(from, to, term, kind, fields)`, and the
/// term being in there is the point of the row: a tag over anything less would leave the one field
/// an attacker wants to change outside it.
pub fn encode_signed(m: &Message, key: Option<&Key>) -> Result<Vec<u8>, FerroError> {
    let body = message_body(m)?;
    let body = match key {
        Some(k) => signing::sign_frame(k, &body)?,
        None => body,
    };

    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(CONSENSUS_TAG);
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// The unsigned frame body for `m` — `from | to | term | kind | fields` — refused over the frame
/// limit. Everything [`encode_signed`] frames, and what [`append_entries_budget`] measures.
fn message_body(m: &Message) -> Result<Vec<u8>, FerroError> {
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
    Ok(body)
}

/// How many bytes of entries one `Append` may carry and still be framed, **signed or not**.
///
/// For the leader's batching (D220). An `Append` used to be capped by entry count alone, while one
/// entry may be up to `log::MAX_ENTRY_BYTES`, so 64 large entries made an `Append` this encoder
/// refused on every heartbeat — and `node.rs` discards a refused send, so that follower was stuck
/// with no meter moving.
///
/// Taken from the encoder rather than recomputed beside it: the frame limit, less the body of an
/// `Append` that carries no entries (encoded here to measure it), less the MAC a signing transport
/// adds. The state machine that builds a batch cannot know whether its transport signs, so it
/// always leaves room for one.
pub(crate) fn append_entries_budget() -> usize {
    let empty = Message {
        from: NodeId(0),
        to: NodeId(0),
        term: 0,
        body: Body::Append { prev_round: 0, prev_term: 0, entries: Vec::new(), commit: 0 },
    };
    // Every field around the entries is fixed-width, so these values do not change the length.
    // An empty `Append` cannot fail to encode; if it somehow did, a zero budget still sends one
    // entry per `Append` (see `replicate.rs`), which degrades rather than stalls.
    message_body(&empty)
        .map_or(0, |b| MAX_FRAME_BYTES.saturating_sub(b.len() + signing::MAC_LEN))
}

/// The bytes the encoder writes for one entry inside an `Append`, measured by writing it.
///
/// Measured rather than computed so that it cannot drift from [`encode_entry`]. An entry the encoder
/// cannot write at all reports `usize::MAX`: it fits nothing, which is the truth.
pub(crate) fn entry_wire_len(e: &Entry) -> usize {
    let mut b = Vec::new();
    match encode_entry(&mut b, e) {
        Ok(()) => b.len(),
        Err(_) => usize::MAX,
    }
}

/// **The one admission check** (D223): whether an entry can ever be carried, decided before it
/// reaches the leader's log, in memory or on disk.
///
/// Every limit here is the encoder's own, applied by running it rather than restated. An entry the
/// encoder cannot write at all — a configuration over [`MAX_CONFIG_NODES`], a name longer than its
/// u16 prefix — is refused with the encoder's own error. An entry it can write, but longer than
/// [`append_entries_budget`], is refused because it would not fit one signed `Append` alone.
///
/// Why at proposal. A round the wire cannot carry is a round no follower can ever receive, so it
/// never commits, and it blocks every round after it. Before this, `on_propose` appended any command,
/// and the only refusals came later and silently: from the disk, after the in-memory tail already
/// held the round, or from the encoder on every heartbeat, discarded by `node.rs`.
pub(crate) fn admit_entry(e: &Entry) -> Result<(), FerroError> {
    let mut b = Vec::new();
    encode_entry(&mut b, e)?;
    let budget = append_entries_budget();
    if b.len() > budget {
        return Err(FerroError::Wal(format!(
            "an entry of {} bytes on the wire cannot fit one Append frame, which carries at most \
             {budget} bytes of entries once its envelope and a signature are paid for. Refused at \
             proposal, before it reached the log: a round no frame can carry is a round no follower \
             can ever receive, and it would block every round after it",
            b.len()
        )));
    }
    Ok(())
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
            // D154: this `?` IS the refusal now. `wal::log::write_str` checks its own `u16`
            // prefix, so an over-long table or column name is refused here, at the encoder,
            // instead of being written truncated and detected afterwards.
            rec.serialize(&mut rec_bytes)?;

            // ⛔ A SEND-SIDE RE-ENCODE CHECK WAS REMOVED HERE BY D154, and it is worth saying why
            // rather than leaving a silent gap where a guard used to be.
            //
            // It round-tripped the record and compared bytes, to catch a truncation that reparsed
            // SUCCESSFULLY but to a different record — the case a length check could not see while
            // `write_str` was unchecked. It was one of THREE local workarounds around that one
            // unguarded primitive. With the guard moved into `write_str`, it became unfirable: the
            // `?` above refuses first, so a mutant deleting the round trip could not be caught,
            // and its own test had ALREADY needed a hand-built 65540-byte payload to make it fire
            // once. **A guard nobody can force to fire is the appearance of protection, not
            // protection.**
            //
            // Its forward-looking justification — "catches every present and future truncation in
            // an encoder this file does not own" — is discharged by `decode_catalog`, which runs
            // the byte-for-byte IDENTICAL round trip on the receive side and is the total check
            // for non-canonical spellings. The receiver also has the job the sender structurally
            // cannot do: trailing bytes arriving off a socket, which `RecKind::deserialize`
            // silently ignores because it stops at its last field. That one is independently
            // testable from hand-crafted bytes, and it is the case a signature cannot survive.
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
        // Refused to the SENDER as well, at the same limit the decoder uses. Otherwise this node
        // emits a frame every peer refuses and never learns why — a cluster that silently cannot
        // replicate its own configuration.
        if list.len() > MAX_CONFIG_NODES {
            return Err(FerroError::Wal(format!(
                "a configuration holds {} nodes, over the {MAX_CONFIG_NODES} the wire format \
                 allows; a peer would refuse this frame, so it is refused here where the cause is \
                 visible",
                list.len()
            )));
        }
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

/// Authenticate a frame body and then decode it — **in that order**, which is the whole hook.
///
/// `decode` is the code that has to survive hostile input; running it only on bytes that already
/// carried a valid tag means an unauthenticated peer cannot reach the parser, let alone the state
/// machine. With `key` at `None` this is exactly [`decode`], for a transport that was not given a
/// key.
pub fn decode_verified(body: &[u8], key: Option<&Key>) -> Result<Message, FerroError> {
    match key {
        Some(k) => decode(signing::verify_frame(k, body)?),
        None => decode(body),
    }
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
    decoded.serialize(&mut reencoded)?;
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
    // Refused before a single id is read. See [`MAX_CONFIG_NODES`]: the frame limit bounds the
    // bytes a peer can make this process hold, not the work it can make this process do.
    if count > MAX_CONFIG_NODES {
        return Err(FerroError::Wal(format!(
            "a configuration claims {count} {what}s, over the {MAX_CONFIG_NODES} limit. No cluster \
             that large can form a usable quorum, so a configuration this size is a malformed or \
             hostile frame rather than a cluster, and it is refused before a single id is read"
        )));
    }
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

/// The first node present in both lists, or `None`.
///
/// Linear, because both lists are already known to be strictly ascending — which
/// [`decode_node_list`] has just guaranteed. A two-pointer walk, not a nested scan.
fn first_common(a: &[NodeId], b: &[NodeId]) -> Option<NodeId> {
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => return Some(a[i]),
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    None
}

fn decode_config(bytes: &[u8], at: &mut usize) -> Result<Config, FerroError> {
    let version = take_u64(bytes, at)?;
    let term = take_u64(bytes, at)?;
    let members = decode_node_list(bytes, at, "member")?;
    let learners = decode_node_list(bytes, at, "learner")?;

    // A node cannot be both. `with_learners` drops such a learner silently, which is the right
    // thing for a *builder* — the voter entry is the stronger statement — and the wrong thing for a
    // decoder, because the config this node would then hold is not the one the leader sent.
    //
    // A linear merge rather than `find(|n| members.contains(n))`, which was O(learners × members).
    // [`MAX_CONFIG_NODES`] already bounds that product, so this is belt as well as braces — but a
    // linear check over two lists already known to be strictly ascending is both cheaper and
    // simpler than relying on the cap, and it does not become a defect again if the cap is raised.
    if let Some(n) = first_common(&members, &learners) {
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
/// Every one is a real parameter rather than a tuning guess: `queue_depth` and `queue_bytes` are how
/// far a peer may fall behind, in messages and in bytes, before this node starts dropping to it,
/// `poll_interval` is how long a thread may stay blocked in `read` after a shutdown has been asked
/// for, and `reconnect_delay` is how hard this node retries a peer that is down.
#[derive(Debug, Clone)]
pub struct TransportOptions {
    /// Messages held for one peer before the **oldest** is dropped. See [`Transport::send`].
    pub queue_depth: usize,
    /// Bytes of queued frames held for one peer before the **oldest** is dropped.
    ///
    /// A depth in messages is not a bound on memory: a message is anything from 18 bytes to 8 MiB, so
    /// `queue_depth` alone let one peer's queue hold 1024 x 8 MiB = 8.6 GB (D207, ported from
    /// F3-transport's `207d362`, which never merged). Both bounds apply and whichever binds first
    /// wins: the depth stops a flood of tiny heartbeats, and this stops a run of maximal `Append`s.
    ///
    /// A frame larger than the whole bound is still queued, alone — the oldest are dropped until
    /// the queue is empty and then it goes in — so no message is ever unsendable because of this
    /// knob, however small it is set.
    pub queue_bytes: usize,
    /// Socket read timeout, and the listener's accept poll. Bounds shutdown latency.
    pub poll_interval: Duration,
    /// How long a sender thread waits after a failed dial before trying that peer again.
    pub reconnect_delay: Duration,
    /// How long an accepted connection may take to finish its handshake before it is closed.
    pub handshake_deadline: Duration,
    /// Most inbound connections accepted at once.
    ///
    /// **Signing does not replace this cap.** F7 authenticates every *frame*, not the handshake, so
    /// anything that can reach this port can still open a connection, and each one costs a thread
    /// and two descriptors. Without a cap that is an unauthenticated peer choosing how many threads
    /// this process runs — the same class of hole as letting one choose how many bytes it
    /// allocates, which the frame limit already closes. A signed transport closes such a connection
    /// on its first unverifiable frame, which bounds how long the slot is held but not how many are
    /// opened.
    /// Beyond the cap a connection is closed immediately rather than queued, and counted.
    pub max_inbound_conns: usize,
    /// How long an established connection may stay silent before it is closed.
    ///
    /// A peer whose host vanishes without sending a FIN leaves a connection that is never readable
    /// and never errors, so its thread and descriptors are pinned for the life of the process.
    ///
    /// **What closing a live peer costs.** The leader heartbeats its followers, and they answer it,
    /// so those connections are never silent for long. **Followers send each other nothing** while
    /// a leader holds, so each follower's connection *to* another is closed by the receiver after
    /// this long. The sender does not read its socket, so it does not see the close.
    ///
    /// Until D224, a surviving follower's first campaign frame after the leader died went into that
    /// closed connection and was lost uncounted. Its second failed and was counted. Failover after a
    /// long stable term took an election round or two longer.
    ///
    /// Now the sender probes a link idle for half of this before its next write, and redials first
    /// if the link is closed (`idle_probe_gap`). For that to hold, no node's value may be below half
    /// of another's. Nothing enforces it; every node in this repo runs this default.
    pub idle_deadline: Duration,
    /// Most bytes of undelivered inbound messages held before further ones are refused.
    ///
    /// **The frame limit alone does not bound inbound memory.** It caps one frame; the channel to
    /// the caller is unbounded, so a peer that writes faster than the caller drains — the normal
    /// state whenever the state machine is applying or fsyncing — accumulates every frame it sends.
    /// Bounded in bytes rather than messages because a message is anything from 18 bytes to 8 MiB,
    /// so a depth in messages is not a bound on anything.
    pub inbox_bytes: usize,
}

impl Default for TransportOptions {
    fn default() -> Self {
        TransportOptions {
            queue_depth: 1024,
            queue_bytes: 64 * 1024 * 1024,
            poll_interval: Duration::from_millis(50),
            reconnect_delay: Duration::from_millis(100),
            handshake_deadline: Duration::from_secs(5),
            inbox_bytes: 32 * 1024 * 1024,
            max_inbound_conns: 256,
            idle_deadline: Duration::from_secs(60),
        }
    }
}

/// Everything this transport has counted. **Every drop and refusal it decides on is counted, never
/// silent** — a message that vanished without a number attached is indistinguishable from a protocol
/// bug, and this transport is allowed to drop. What TCP loses after a frame left this process is
/// not, and cannot be; the module header lists those cases.
#[derive(Debug, Default)]
struct Counters {
    sent: AtomicU64,
    received: AtomicU64,
    misrouted: AtomicU64,
    refused_handshakes: AtomicU64,
    connect_failures: AtomicU64,
    /// Inbound messages refused because the caller had not drained `inbox_bytes` worth yet.
    inbound_dropped: AtomicU64,
    /// Outbound frames already dequeued and then lost to a failed write. Counted separately from
    /// queue-overflow drops because the two say different things to an operator: overflow means a
    /// peer is slow, this means a connection broke.
    lost_in_flight: AtomicU64,
    /// Bytes of decoded messages sitting in the inbox, undelivered.
    inbox_bytes: AtomicUsize,
    /// Connections closed without being served: because `max_inbound_conns` were already
    /// established, because the accepted socket could not be duplicated or configured (D207), or
    /// because no thread could be started for it.
    refused_conns: AtomicU64,
    /// Connections closed for going silent longer than `idle_deadline`.
    idle_closed: AtomicU64,
    /// Outbound messages refused because the transport is stopped.
    refused_after_stop: AtomicU64,
    /// Outbound messages refused because the encoder cannot frame them (D223). After admission at
    /// proposal, a leader's own traffic should never land here, so a climbing number is a bug that
    /// this is the only meter able to show: `node.rs` discards the error by design.
    unencodable: AtomicU64,
    /// Outbound messages refused because they were addressed to this node itself, or to a node this
    /// transport holds no address for: configuration mistakes that are otherwise a silent partition.
    unaddressable: AtomicU64,
    /// Times a sender probed its link before the first write after an idle gap (D224). A link that
    /// consensus keeps busy never reaches the gate, so this should move only on links it leaves
    /// silent: follower to follower during a stable term.
    idle_probes: AtomicU64,
    /// Probes that found the peer had closed the link, so the sender redialled before writing
    /// instead of losing the frame to the closed connection (D224).
    idle_redials: AtomicU64,
    /// Inbound frames that did not authenticate against this node's signing key, and were refused
    /// **before** [`decode`] saw them.
    ///
    /// Its own counter and not folded into a general "bad frame": a frame that fails to decode is a
    /// version skew or a bug, and a frame that fails to authenticate is either a misconfigured key
    /// or somebody who should not be talking to this port at all. An operator seeing this number
    /// move is being told something no other counter here can tell them.
    unauthenticated: AtomicU64,
}

/// One peer's queue and its current connection.
struct Outbox {
    addr: SocketAddr,
    state: Mutex<OutboxState>,
    woken: Condvar,
    depth: usize,
    /// `TransportOptions::queue_bytes`.
    max_bytes: usize,
    dropped: AtomicU64,
}

struct OutboxState {
    queue: VecDeque<Vec<u8>>,
    /// The sum of `queue`'s frame lengths, kept beside it so the byte bound costs no walk. Every
    /// push, drop and pop of a frame moves it by that frame's length, under the same lock.
    bytes: usize,
    /// A clone of the live socket, held **only** so that [`Transport::shutdown`] can call
    /// `shutdown(Both)` on it. A sender thread parked in `write_all` against a peer whose receive
    /// window is full is otherwise unreachable, and joining it would hang for ever.
    live: Option<TcpStream>,
    stopped: bool,
}

impl Outbox {
    /// Enqueue, dropping the **oldest** while this peer is already `depth` frames behind, or while
    /// this frame would take it past `max_bytes`.
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
        // Both bounds, whichever binds first. The loop stops at an empty queue whatever the bytes
        // say, so a frame larger than the whole byte bound is still admitted, alone: no message is
        // made permanently unsendable by the bound that exists to keep sending possible.
        while !st.queue.is_empty()
            && (st.queue.len() >= self.depth
                || st.bytes.saturating_add(frame.len()) > self.max_bytes)
        {
            if let Some(old) = st.queue.pop_front() {
                st.bytes -= old.len();
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        st.bytes += frame.len();
        st.queue.push_back(frame);
        self.woken.notify_all();
    }

    /// Wait up to `delay` before the sender redials, returning early if woken — or at once if the
    /// transport is already stopping, in which case the sender's loop head sees the stop flag and
    /// leaves. (`stopped` is only ever set after the stop flag, so that check suffices; a returned
    /// flag here would be a second guard on the same exit, and nothing could tell if it broke.)
    ///
    /// **The stop flags are read under the lock BEFORE waiting.** `shutdown` notifies once, and a
    /// notify that lands while the sender is still inside `dial` finds nobody waiting; a wait begun
    /// after it used to sleep out the whole delay before anything looked at the flags again — the
    /// lost wakeup (D207, from F3-transport's `207d362`). `stopped` is set under this same lock, so
    /// it lands either before this check or while the wait below is parked, never between them.
    /// Both redial waits in `sender_loop` go through here, so there is one copy to get right.
    fn wait_before_redial(&self, stop: &AtomicBool, delay: Duration) {
        let st = self.state.lock().unwrap();
        if st.stopped || stop.load(Ordering::SeqCst) {
            return;
        }
        let _ = self.woken.wait_timeout(st, delay);
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
    /// Carries the message and its encoded size, so `recv` can give the size back to the byte
    /// budget the connection thread charged it against.
    inbox: Mutex<mpsc::Receiver<(Message, usize)>>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// Held for the whole of [`Transport::shutdown`], so a concurrent second call waits for the
    /// first to finish joining rather than taking an empty thread list and returning early. Without
    /// it "a second call is a barrier" was true only for a sequential caller — and a `Drop` racing
    /// an explicit `shutdown` is exactly the concurrent case.
    shutdown_lock: Mutex<()>,
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
    /// The cluster signing key, when this transport was built with one. `None` is an unsigned
    /// transport: see the module header for why that is a separate constructor rather than a
    /// defaulted field.
    key: Option<Arc<Key>>,
}

/// Removes a connection from the live registry however it leaves — including the early return on a
/// refused handshake, and a connection that never got a thread at all.
///
/// A `Drop` guard rather than a call at the end of `conn_loop`, because "every exit path also
/// deregisters" is exactly the invariant a later edit breaks by adding one more `return`.
///
/// **Made in the same critical section that reserves the slot**, and moved into the connection
/// thread from there. It used to be made at the top of `conn_loop`, which left the two exits in
/// `accept_loop` between the reservation and the spawn outside it: the spawn failure released the
/// slot by hand, and the failed socket setup did not — so each failed setup leaked a slot and a
/// descriptor, and `max_inbound_conns` of them made the node refuse every real peer (D207, ported
/// from F3-transport's `207d362`, which never merged). With the guard owning the slot from the
/// moment it exists, no exit can be added that forgets it.
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

    /// The same, signing every frame it sends and refusing every frame it cannot verify.
    ///
    /// A separate constructor rather than a field on [`TransportOptions`], and that is deliberate:
    /// the options struct is a bag of knobs that all have defensible defaults, and a key does not
    /// have one. Put there, `..Default::default()` in some later caller would leave a cluster
    /// unsigned while reading as if it had been configured. Here the choice is a function name.
    pub fn bind_with_key(
        self_id: NodeId,
        listen: impl ToSocketAddrs,
        peers: BTreeMap<NodeId, SocketAddr>,
        opts: TransportOptions,
        key: Arc<Key>,
    ) -> Result<Transport, FerroError> {
        let listener = TcpListener::bind(listen).map_err(|e| FerroError::Io(e.to_string()))?;
        Transport::from_listener_with_key(self_id, listener, peers, opts, key)
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
        Transport::start(self_id, listener, peers, opts, None)
    }

    /// The same, over a listener the caller already holds, signing every frame.
    ///
    /// `Arc` because the key outlives this call in three places at once — this handle, the sender
    /// threads, and every connection thread — and because a key is the one value in this process
    /// that should exist exactly once. See [`Transport::bind_with_key`] for why this is a
    /// constructor and not an option.
    pub fn from_listener_with_key(
        self_id: NodeId,
        listener: TcpListener,
        peers: BTreeMap<NodeId, SocketAddr>,
        opts: TransportOptions,
        key: Arc<Key>,
    ) -> Result<Transport, FerroError> {
        Transport::start(self_id, listener, peers, opts, Some(key))
    }

    /// The one body behind all four constructors, so the signed and unsigned paths cannot drift.
    fn start(
        self_id: NodeId,
        listener: TcpListener,
        peers: BTreeMap<NodeId, SocketAddr>,
        opts: TransportOptions,
        key: Option<Arc<Key>>,
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
        // Two more knobs whose zero is a partition that reads as health, not a smaller setting
        // (D207, ported from F3-transport's `207d362`). Not folded into the loop above: its message
        // blames std's socket calls, and neither of these ever reaches one.
        if opts.idle_deadline.is_zero() {
            return Err(FerroError::Internal(
                "`idle_deadline` is zero. It does not mean \"never idle\": a connection is closed \
                 once it has been silent longer than this, which a zero makes true on the first \
                 poll after the handshake — a node that accepts every peer and at once hangs up on \
                 each"
                    .to_string(),
            ));
        }
        if opts.max_inbound_conns == 0 {
            return Err(FerroError::Internal(
                "`max_inbound_conns` is zero, so every inbound connection would be refused at the \
                 cap: a node deaf to the whole cluster while its outbound meters read healthy"
                    .to_string(),
            ));
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
        // Every `?` from here on would otherwise return with `threads` and `stop` still local.
        // Dropping a `JoinHandle` DETACHES its thread, and dropping the only other `Arc<AtomicBool>`
        // leaves it with no way to be told to stop — so a spawn failure part way through this
        // function would leak a sender thread per peer already started, for the life of the
        // process. `started` collects them so the teardown below can reach them.
        let mut started: Vec<JoinHandle<()>> = Vec::new();
        let mut outbox_list: Vec<Arc<Outbox>> = Vec::new();
        let inbound_conns: Arc<Mutex<BTreeMap<u64, TcpStream>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        // Lives only in the accept thread, which is the only place a connection id is minted.
        let next_conn_id = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel::<(Message, usize)>();
        let mut threads = Vec::new();

        let mut outboxes = BTreeMap::new();
        for (peer, addr) in peers {
            let ob = Arc::new(Outbox {
                addr,
                state: Mutex::new(OutboxState {
                    queue: VecDeque::new(),
                    bytes: 0,
                    live: None,
                    stopped: false,
                }),
                woken: Condvar::new(),
                depth: opts.queue_depth,
                max_bytes: opts.queue_bytes,
                dropped: AtomicU64::new(0),
            });
            outboxes.insert(peer, Arc::clone(&ob));
            outbox_list.push(Arc::clone(&ob));
            let stop_c = Arc::clone(&stop);
            let counters_c = Arc::clone(&counters);
            let opts_c = opts.clone();
            match std::thread::Builder::new()
                .name(format!("consensus-out-{self_id}-to-{peer}"))
                .spawn(move || sender_loop(ob, stop_c, counters_c, opts_c))
            {
                Ok(h) => started.push(h),
                Err(e) => {
                    stop_started(&stop, &outbox_list, started);
                    return Err(FerroError::Io(format!(
                        "could not start the sender thread for {peer}: {e}. The {} thread(s) \
                         already started were stopped and joined rather than detached",
                        outbox_list.len() - 1
                    )));
                }
            }
        }
        threads.append(&mut started);

        // The accept thread owns the per-connection threads it spawns, and joins them before it
        // returns. Nothing here is detached: a transport that has been shut down must have no
        // thread still holding a socket, or a test that binds a fresh port after one is a test
        // racing the previous run.
        let stop_c = Arc::clone(&stop);
        let counters_c = Arc::clone(&counters);
        let conns_c = Arc::clone(&inbound_conns);
        let ids_c = next_conn_id;
        let opts_c = opts.clone();
        let key_c = key.clone();
        match std::thread::Builder::new()
            .name(format!("consensus-accept-{self_id}"))
            .spawn(move || {
                accept_loop(listener, self_id, tx, stop_c, counters_c, conns_c, ids_c, opts_c, key_c)
            }) {
            Ok(h) => threads.push(h),
            Err(e) => {
                stop_started(&stop, &outbox_list, threads);
                return Err(FerroError::Io(format!(
                    "could not start the accept thread for {self_id}: {e}. Every sender thread \
                     already started was stopped and joined rather than detached"
                )));
            }
        }

        Ok(Transport {
            self_id,
            local_addr,
            outboxes,
            inbox: Mutex::new(rx),
            counters,
            stop,
            threads: Mutex::new(threads),
            shutdown_lock: Mutex::new(()),
            inbound_conns,
            key,
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
    ///
    /// **Every refusal is also counted** (D223): [`Transport::refused_after_stop`],
    /// [`Transport::unaddressable`] and [`Transport::unencodable`]. The caller in `node.rs` discards
    /// the error by design, so the count is the only trace a refused send leaves.
    pub fn send(&self, m: &Message) -> Result<(), FerroError> {
        // A stopped transport discards, and a discard with no error and no counter is the silent
        // loss this module claims not to have. Refused, because a caller still stepping its state
        // machine after shutting down its transport has a bug that this is the only chance to name.
        if self.stop.load(Ordering::SeqCst) {
            self.counters.refused_after_stop.fetch_add(1, Ordering::SeqCst);
            return Err(FerroError::Internal(format!(
                "this transport has been shut down, so the message to {} was refused rather than \
                 dropped. A node that keeps producing `Action::Send` after its transport stopped is \
                 stepping a state machine whose output goes nowhere",
                m.to
            )));
        }
        if m.to == self.self_id {
            self.counters.unaddressable.fetch_add(1, Ordering::SeqCst);
            return Err(FerroError::Internal(format!(
                "{} tried to send a consensus message to itself; the state machine addresses peers \
                 only, and a self-addressed message means a handler used the wrong id",
                self.self_id
            )));
        }
        let ob = self.outboxes.get(&m.to).ok_or_else(|| {
            self.counters.unaddressable.fetch_add(1, Ordering::SeqCst);
            FerroError::Internal(format!(
                "no address is configured for {}, so a message to it cannot be sent. This node \
                 holds addresses for {:?}. A configuration that names a node the transport cannot \
                 reach is a node permanently unreachable, whose only meter is `unaddressable` — add \
                 it to the peer map",
                m.to,
                self.outboxes.keys().collect::<Vec<_>>()
            ))
        })?;
        // Counted as well as returned: `node.rs` discards this error by design, so without the count
        // a message the encoder refuses vanishes with no number attached (D223).
        let frame = encode_signed(m, self.key.as_deref()).inspect_err(|_| {
            self.counters.unencodable.fetch_add(1, Ordering::SeqCst);
        })?;
        ob.push(frame);
        self.counters.sent.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// The next message that has arrived, or `None` if none has.
    pub fn try_recv(&self) -> Option<Message> {
        let got = self.inbox.lock().unwrap().try_recv().ok();
        got.map(|(m, n)| self.credit(m, n))
    }

    /// The next message, waiting up to `d`. `None` means nothing arrived in that window — which is
    /// an ordinary and expected answer, not an error. See [`Transport::is_stopped`] for telling a
    /// quiet window from a closed transport.
    pub fn recv_timeout(&self, d: Duration) -> Option<Message> {
        let got = self.inbox.lock().unwrap().recv_timeout(d).ok();
        got.map(|(m, n)| self.credit(m, n))
    }

    /// Return a delivered message's bytes to the inbound budget.
    fn credit(&self, m: Message, n: usize) -> Message {
        // `fetch_sub` cannot underflow here: every message in the channel was charged before it was
        // sent, and each is credited exactly once on the way out. Saturating anyway, because an
        // underflowing byte counter would silently disable the bound it exists to enforce.
        let _ = self.counters.inbox_bytes.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |cur| Some(cur.saturating_sub(n)),
        );
        m
    }

    /// Messages handed to a peer's queue.
    pub fn sent(&self) -> u64 {
        self.counters.sent.load(Ordering::SeqCst)
    }
    /// Messages dropped because a peer was `queue_depth` messages or `queue_bytes` bytes behind.
    /// See [`Transport::send`].
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
    /// Inbound messages refused because the caller had not drained `inbox_bytes` worth yet.
    ///
    /// Not a silent loss: consensus re-sends, but a climbing number here means the caller is not
    /// draining fast enough and the cluster is losing traffic to this node's own back pressure.
    pub fn inbound_dropped(&self) -> u64 {
        self.counters.inbound_dropped.load(Ordering::SeqCst)
    }

    /// Outbound frames dequeued and then lost to a failed write — one per broken connection.
    pub fn lost_in_flight(&self) -> u64 {
        self.counters.lost_in_flight.load(Ordering::SeqCst)
    }

    /// Bytes of decoded inbound messages waiting for the caller.
    pub fn inbox_bytes(&self) -> usize {
        self.counters.inbox_bytes.load(Ordering::SeqCst)
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

    /// Inbound frames refused because they did not authenticate. Always zero on a transport built
    /// without a key, which is a fact about the configuration and not about the network.
    pub fn unauthenticated(&self) -> u64 {
        self.counters.unauthenticated.load(Ordering::SeqCst)
    }

    /// Whether this transport was given a key, and therefore signs what it sends and refuses what
    /// it cannot verify.
    ///
    /// Worth being able to ask: "the cluster is signed" is otherwise a claim about how four
    /// constructors were called in four processes, with nothing able to answer it.
    pub fn signs_its_traffic(&self) -> bool {
        self.key.is_some()
    }
    /// Inbound connections closed without being served: the cap was full, the accepted socket could
    /// not be duplicated or configured, or no thread could be started for it. F3-transport's
    /// `207d362` recorded a failed configuration as what a peer that connects and resets before
    /// `accept` leaves behind on macOS (D207), so on that platform this meter climbing while
    /// `live_inbound_conns` stays low reads as resets, not as a busy node.
    pub fn refused_conns(&self) -> u64 {
        self.counters.refused_conns.load(Ordering::SeqCst)
    }
    /// Connections closed for exceeding `idle_deadline` without a frame.
    pub fn idle_closed(&self) -> u64 {
        self.counters.idle_closed.load(Ordering::SeqCst)
    }
    /// Sends refused because the transport is stopped.
    pub fn refused_after_stop(&self) -> u64 {
        self.counters.refused_after_stop.load(Ordering::SeqCst)
    }
    /// Sends refused because the encoder could not frame the message. See `Counters::unencodable`.
    pub fn unencodable(&self) -> u64 {
        self.counters.unencodable.load(Ordering::SeqCst)
    }
    /// Sends refused because they were addressed to this node or to a node with no address.
    pub fn unaddressable(&self) -> u64 {
        self.counters.unaddressable.load(Ordering::SeqCst)
    }
    /// Links probed before the first write after an idle gap. See `Counters::idle_probes`.
    pub fn idle_probes(&self) -> u64 {
        self.counters.idle_probes.load(Ordering::SeqCst)
    }
    /// Probes that found the link closed by the peer and redialled first.
    pub fn idle_redials(&self) -> u64 {
        self.counters.idle_redials.load(Ordering::SeqCst)
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
        // Taken first and held throughout: a second caller blocks here until the first has finished
        // joining, which is what makes the barrier claim true for a concurrent caller and not just
        // a sequential one. `Drop` racing an explicit `shutdown` is that concurrent case.
        let _barrier = self.shutdown_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.stop.store(true, Ordering::SeqCst);
        for ob in self.outboxes.values() {
            let mut st = ob.state.lock().unwrap();
            st.stopped = true;
            st.queue.clear();
            st.bytes = 0;
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

/// Stop and join threads started before a failure in [`Transport::from_listener`].
///
/// The same teardown [`Transport::shutdown`] performs, reachable before a `Transport` exists to
/// call it on. Written as a function rather than inlined twice because the failure it handles is
/// the one nobody tests by hand, so it must not have two implementations.
fn stop_started(stop: &Arc<AtomicBool>, outboxes: &[Arc<Outbox>], threads: Vec<JoinHandle<()>>) {
    stop.store(true, Ordering::SeqCst);
    for ob in outboxes {
        let mut st = ob.state.lock().unwrap();
        st.stopped = true;
        st.queue.clear();
        st.bytes = 0;
        if let Some(s) = st.live.take() {
            let _ = s.shutdown(Shutdown::Both);
        }
        ob.woken.notify_all();
    }
    for h in threads {
        let _ = h.join();
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Every meter a transport keeps, as one read-only copy (D223 review F3).
///
/// A value rather than `&Transport`: [`Transport::send`] and [`Transport::shutdown`] take `&self`,
/// so a reference handed out for reading meters would also let its holder send on, or stop, the
/// node's transport. `Node::transport_counters` hands this out, so the meters that are the only
/// trace of a refused send can be read from a running node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransportCounters {
    pub sent: u64,
    pub received: u64,
    pub dropped: u64,
    pub lost_in_flight: u64,
    pub inbound_dropped: u64,
    pub misrouted: u64,
    pub refused_handshakes: u64,
    pub refused_conns: u64,
    pub idle_closed: u64,
    pub refused_after_stop: u64,
    pub connect_failures: u64,
    pub unauthenticated: u64,
    pub unencodable: u64,
    pub unaddressable: u64,
    pub idle_probes: u64,
    pub idle_redials: u64,
    pub live_inbound_conns: usize,
    pub inbox_bytes: usize,
}

impl Transport {
    /// A copy of every meter, taken now. See [`TransportCounters`].
    pub fn counters(&self) -> TransportCounters {
        TransportCounters {
            sent: self.sent(),
            received: self.received(),
            dropped: self.dropped(),
            lost_in_flight: self.lost_in_flight(),
            inbound_dropped: self.inbound_dropped(),
            misrouted: self.misrouted(),
            refused_handshakes: self.refused_handshakes(),
            refused_conns: self.refused_conns(),
            idle_closed: self.idle_closed(),
            refused_after_stop: self.refused_after_stop(),
            connect_failures: self.connect_failures(),
            unauthenticated: self.unauthenticated(),
            unencodable: self.unencodable(),
            unaddressable: self.unaddressable(),
            idle_probes: self.idle_probes(),
            idle_redials: self.idle_redials(),
            live_inbound_conns: self.live_inbound_conns(),
            inbox_bytes: self.inbox_bytes(),
        }
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
            .field("refused_conns", &self.refused_conns())
            .field("idle_closed", &self.idle_closed())
            .field("inbound_dropped", &self.inbound_dropped())
            .field("lost_in_flight", &self.lost_in_flight())
            .field("connect_failures", &self.connect_failures())
            .field("refused_after_stop", &self.refused_after_stop())
            .field("unencodable", &self.unencodable())
            .field("unaddressable", &self.unaddressable())
            .field("idle_probes", &self.idle_probes())
            .field("idle_redials", &self.idle_redials())
            // The key itself is never printed: `signing::Key`'s own `Debug` redacts it, and this
            // reports only whether there is one.
            .field("signed", &self.signs_its_traffic())
            .field("unauthenticated", &self.unauthenticated())
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
    // When the current connection last carried a frame, or was dialled. The gap since then decides
    // whether the link is probed before the next write (D224; the gate is just before the write).
    let mut last_used = Instant::now();
    // A frame already taken from the queue whose link the probe found closed. It is written first on
    // the next connection, so finding the close costs the frame nothing. It sits outside the queue's
    // drop-oldest bound while the peer is down, so on return the stalest frame goes first, against
    // `push`'s policy. Consensus refuses a stale term, so one such frame is harmless.
    let mut carried: Option<Vec<u8>> = None;
    // Half the idle deadline; the reasons for both bounds are at `idle_probe_gap`.
    let probe_gap = idle_probe_gap(&opts);
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        if conn.is_none() {
            match dial(ob.addr, &stop, &opts) {
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
                            // Wait before retrying, exactly as a failed dial does. Retrying
                            // immediately is a 100%-CPU loop and a connection storm against a peer
                            // that has done nothing wrong.
                            counters.connect_failures.fetch_add(1, Ordering::SeqCst);
                            ob.wait_before_redial(&stop, opts.reconnect_delay);
                            continue;
                        }
                    }
                    conn = Some(s);
                    last_used = Instant::now();
                }
                Err(_) => {
                    counters.connect_failures.fetch_add(1, Ordering::SeqCst);
                    // Wait on the condvar rather than sleeping, so a shutdown does not have to
                    // wait out a reconnect delay it has already made pointless.
                    ob.wait_before_redial(&stop, opts.reconnect_delay);
                    continue;
                }
            }
        }

        // Take one frame — the one a probe carried over, if there is one — waiting a bounded time so
        // the stop flag is checked regularly.
        let frame = if let Some(f) = carried.take() {
            Some(f)
        } else {
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
            let taken = st.queue.pop_front();
            if let Some(f) = taken.as_ref() {
                st.bytes -= f.len();
            }
            taken
        };
        let Some(frame) = frame else { continue };

        let s = conn.as_mut().expect("connected above");
        // **Probe a link that has been idle before writing to it** (D224). The receiver closes a
        // connection silent past its `idle_deadline`, and this thread never reads its socket, so it
        // would not know: the frame would go into the closed connection and be lost with no number
        // attached, and only the next write would fail. Consensus leaves follower-to-follower links
        // silent for a whole term, so that lost frame was a survivor's first campaign frame after the
        // leader died. A link consensus keeps busy never reaches this gate, so all it pays is two clock
        // reads per frame: this one and the refresh after the write.
        if last_used.elapsed() >= probe_gap {
            counters.idle_probes.fetch_add(1, Ordering::SeqCst);
            if peer_has_closed(s) {
                // A shutdown closes `st.live`, which shares this socket, so the probe sees its close
                // too. Both shutdown paths set `stop` before they close it, so that close is always
                // seen here with `stop` set: it is not a redial, and it is not counted as one. The
                // frame is dropped uncounted, as the queue is at shutdown.
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                counters.idle_redials.fetch_add(1, Ordering::SeqCst);
                let mut st = ob.state.lock().unwrap();
                if let Some(old) = st.live.take() {
                    let _ = old.shutdown(Shutdown::Both);
                }
                drop(st);
                conn = None;
                carried = Some(frame);
                continue;
            }
        }
        // A write that fails part way through has left a partial frame on the wire, and there is no
        // way to resume it — the peer's reader is now inside a frame that will never finish. So the
        // connection is dropped, which is what makes the peer's reader see EOF and reset. The frame
        // is lost, and that is the documented policy of this transport: consensus re-sends.
        if s.write_all(&frame).and_then(|()| s.flush()).is_err() {
            // The frame was already dequeued, and a partial write cannot be resumed — the peer's
            // reader is inside a frame that will never finish. So it is lost, and it is COUNTED:
            // this happens at least once on every reconnect, which is this transport's only
            // recovery path, and an uncounted loss here is the difference between "a connection
            // broke" and an unexplained gap between `sent()` and the peer's `received()`.
            counters.lost_in_flight.fetch_add(1, Ordering::SeqCst);
            let mut st = ob.state.lock().unwrap();
            if let Some(old) = st.live.take() {
                let _ = old.shutdown(Shutdown::Both);
            }
            drop(st);
            conn = None;
        } else {
            last_used = Instant::now();
        }
    }

    let mut st = ob.state.lock().unwrap();
    if let Some(s) = st.live.take() {
        let _ = s.shutdown(Shutdown::Both);
    }
    drop(st);
    drop(conn);
}

/// The idle gap after which a sender probes its link before writing (D224): half `idle_deadline`.
///
/// **Above the longest gap on any link consensus keeps busy.** A leader heartbeats every
/// `heartbeat` ticks (3, at a 50 ms `NodeOptions::tick`: 150 ms), and every heartbeat is an `Append`
/// each follower answers at once. So leader↔follower links see gaps of about one heartbeat, and at
/// the default deadline this gate is 200 heartbeats away. A link that missed that many has lost its
/// leader several election timeouts ago.
///
/// **At most the peer's `idle_deadline`, so every link the peer may have closed is probed.** The peer
/// closes a link only after hearing nothing for longer than its `idle_deadline`, counted from its
/// last read, which is no earlier than this sender's last write. So a gap under this cannot have
/// been closed, as long as the peer's deadline is at least this gate.
///
/// **Premise, stated and not enforced: no node's `idle_deadline` is below half of another's.** That
/// is the receiver's `D_r ≥ D_s / 2`, where `D_s` is this sender's; equal deadlines are not needed.
/// The handshake carries no options, so nothing checks it, and in this repo every node runs the
/// default. If it is violated, gaps between `D_r` and this gate are closed but never probed, and the
/// pre-D224 loss returns for that band only. There is no new failure mode.
///
/// **Why half: tolerance, not a narrower race.** Half is what lets a peer's deadline be as low as half
/// this one's. It does not narrow the race below, which sits at the peer's close wherever the gate
/// is.
///
/// **The residual race.** A gap that lands within about one `poll_interval` and a round trip of the
/// peer's close can be probed just before the close. The write then succeeds locally and is lost
/// uncounted. It also refreshes the gap, so the next frame is not probed: it fails against the reset
/// and is counted. That is two frames, the whole pre-D224 cost, at a small probability. A
/// follower-to-follower link idle for a whole term is far outside that window.
///
/// **A restarted peer is caught only across a gap of at least this**, which in practice means an idle
/// follower-to-follower link. It is not caught on a busy link, where it costs two heartbeats, nor
/// when the peer restarts mid-election, when campaign frames go out under a second apart. A rebooted
/// host sends nothing to find until this side writes.
fn idle_probe_gap(opts: &TransportOptions) -> Duration {
    opts.idle_deadline / 2
}

/// Whether the peer has closed this connection: asked without blocking and without consuming
/// anything (D224).
///
/// This side only ever writes to the link. The accepting side writes after the handshake on one
/// path only: a refused handshake sends its own handshake, then an `Error` frame, then closes
/// (`conn_loop`). `dial` reads just the six handshake bytes, so that frame stays unread for good, in
/// front of the FIN. So every readable state means the link is closed or closing: a FIN (`peek`
/// returns 0), a reset (an error), or unread bytes. Reading unread bytes as "alive" left the probe
/// blind on such a link for good (the D224 review's F4). Nothing to read means the link is up. A
/// socket that cannot be put back into blocking mode is treated as closed too: the sender redials
/// rather than write through a socket in the wrong mode.
fn peer_has_closed(s: &TcpStream) -> bool {
    if s.set_nonblocking(true).is_err() {
        return true;
    }
    let mut probe = [0u8; 1];
    let closed = match s.peek(&mut probe) {
        Ok(0) => true,
        // A refusal left in front of the peer's FIN, or bytes the protocol never sends on an open
        // link. Either way the frame is carried to a redial, which costs it nothing.
        Ok(_) => true,
        Err(e) => !matches!(
            e.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
        ),
    };
    s.set_nonblocking(false).is_err() || closed
}

/// Dial a peer and complete the handshake as the **connecting** side: write ours, then read theirs.
///
/// That ordering is `examples/repl_replica.rs`'s, and the accepting side's is
/// `examples/repl_primary.rs`'s — read then write. Keeping the two halves as they already are is
/// what stops this from being a third convention on one wire.
fn dial(
    addr: SocketAddr,
    stop: &AtomicBool,
    opts: &TransportOptions,
) -> Result<TcpStream, FerroError> {
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
        // `stop` is checked here as well as by the caller. Without it a shutdown that arrives while
        // a peer is accepting-but-silent waits out a whole `handshake_deadline` on top of the one
        // `connect_timeout` may already have spent — twice the bound `shutdown`'s own doc states.
        if stop.load(Ordering::SeqCst) {
            return Err(FerroError::Io("transport is shutting down".into()));
        }
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
#[allow(clippy::too_many_arguments)]
fn accept_loop(
    listener: TcpListener,
    self_id: NodeId,
    tx: mpsc::Sender<(Message, usize)>,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    conns: Arc<Mutex<BTreeMap<u64, TcpStream>>>,
    ids: Arc<AtomicU64>,
    opts: TransportOptions,
    key: Option<Arc<Key>>,
) {
    let mut conn_threads: Vec<JoinHandle<()>> = Vec::new();
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                // **Refused before a thread is spawned for it.** This transport does not
                // authenticate, so an unbounded accept loop is an unauthenticated peer choosing how
                // many threads this process runs.
                //
                // The check and the reservation happen under ONE lock, and the slot is taken HERE
                // rather than inside the spawned thread. Checking here and registering there was
                // measured as not a cap at all: a test opening 24 connections against a cap of 4
                // established 6, because several accepts each passed a check that only the first
                // should have — the connections were accepted faster than the threads could
                // register them.
                let id = ids.fetch_add(1, Ordering::SeqCst);
                let Ok(mine) = stream.try_clone() else {
                    // Before any slot is reserved, so there is nothing to release; but it is still
                    // a connection closed unserved, and a failed `dup` is usually EMFILE — the
                    // descriptor exhaustion these meters exist to show. Counted with the others.
                    counters.refused_conns.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                };
                // The reservation and the guard that releases it are made together, so from here on
                // EVERY way out — the two `continue`s below, a panic, and every return from
                // `conn_loop` — gives the slot back. See [`ConnRegistration`] for the exit that did
                // not (D207).
                let registration = {
                    let mut map = conns.lock().unwrap();
                    if map.len() >= opts.max_inbound_conns {
                        counters.refused_conns.fetch_add(1, Ordering::SeqCst);
                        drop(map);
                        let _ = stream.shutdown(Shutdown::Both);
                        // **Paced.** Refused back to back, a peer looping `connect()` against a
                        // full cap had this thread spin accept-refuse-accept, a core of this process
                        // for free. One poll per refusal bounds that, and delays a stop by at most
                        // the poll the stop flag is already allowed (D207, from `207d362`).
                        std::thread::sleep(opts.poll_interval);
                        continue;
                    }
                    map.insert(id, mine);
                    ConnRegistration { id, conns: Arc::clone(&conns) }
                };
                // An accepted socket's blocking mode is not portably inherited from its listener,
                // so it is set explicitly rather than assumed. The read timeout is what lets the
                // connection thread notice a shutdown.
                if stream.set_nonblocking(false).is_err()
                    || stream.set_read_timeout(Some(opts.poll_interval)).is_err()
                {
                    // Not hypothetical: F3-transport's `207d362` recorded that on macOS a peer which
                    // connects and resets before `accept` leaves a socket on which `SO_RCVTIMEO`
                    // fails with EINVAL. `registration` is dropped by this `continue`, which
                    // releases the slot; the refusal is counted, because an unserved connection
                    // with no number attached is the silent loss this module does not allow.
                    counters.refused_conns.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }
                let _ = stream.set_nodelay(true);
                let tx_c = tx.clone();
                let stop_c = Arc::clone(&stop);
                let counters_c = Arc::clone(&counters);
                let opts_c = opts.clone();
                let key_c = key.clone();
                match std::thread::Builder::new()
                    .name(format!("consensus-in-{self_id}"))
                    .spawn(move || {
                        conn_loop(
                            stream,
                            registration,
                            self_id,
                            tx_c,
                            stop_c,
                            counters_c,
                            opts_c,
                            key_c,
                        )
                    }) {
                    Ok(h) => conn_threads.push(h),
                    Err(_) => {
                        // A failed spawn drops the closure it was given, and `registration` with
                        // it, so the slot is already released: no thread will ever run for it, and
                        // no second release is needed here. Counted like the failed setup above,
                        // for the same reason — it is a connection this node closed unserved.
                        counters.refused_conns.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
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

#[allow(clippy::too_many_arguments)]
fn conn_loop(
    mut stream: TcpStream,
    // Held, never read: dropping it at any return below is what releases the slot.
    _registration: ConnRegistration,
    self_id: NodeId,
    tx: mpsc::Sender<(Message, usize)>,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    opts: TransportOptions,
    key: Option<Arc<Key>>,
) {
    // The accept thread already reserved this connection's slot and registered its socket, so
    // `shutdown` can reach a peer that connects and then goes silent, and it handed over the guard
    // that releases the slot. Owning it here is what makes every exit path below release it,
    // including the refused-handshake return.

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
    let mut last_heard = Instant::now();
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        // A peer whose host vanished without a FIN leaves a socket that never becomes readable and
        // never errors. Without this, its thread and both its descriptors are held for the life of
        // the process.
        //
        // **Silence is not proof the peer is gone.** This used to say it was, because consensus
        // heartbeats every few ticks. That is true of a leader's links and false of
        // follower-to-follower links, which carry nothing for a whole stable term. Those are closed
        // here too. The cost is paid by the sender, which probes a link it has left idle before
        // writing to it, and redials if the link is closed (`idle_probe_gap`, `peer_has_closed`;
        // D224).
        if last_heard.elapsed() > opts.idle_deadline {
            counters.idle_closed.fetch_add(1, Ordering::SeqCst);
            break;
        }
        match reader.poll(&mut stream) {
            Ok(Poll::Pending) => continue,
            Ok(Poll::Eof) => break,
            Ok(Poll::Frame(tag, body)) => {
                last_heard = Instant::now();
                if tag != CONSENSUS_TAG {
                    // A frame this listener cannot route. The connection is closed rather than
                    // skipped: a stream carrying a tag we do not know is a stream we cannot claim
                    // to be reading correctly.
                    break;
                }
                // **F7's hook, and it is here rather than after `decode` on purpose.** A frame
                // that does not authenticate is refused before the parser runs on it, so an
                // unauthenticated peer reaches neither `decode` nor the state machine. The
                // connection is then closed rather than the frame skipped: a peer that cannot
                // produce a tag is not a peer having a bad moment, and leaving the connection open
                // would let it hold a slot and keep trying.
                let verified = match key.as_deref() {
                    None => &body[..],
                    Some(k) => match signing::verify_frame(k, &body) {
                        Ok(inner) => inner,
                        Err(_) => {
                            counters.unauthenticated.fetch_add(1, Ordering::SeqCst);
                            break;
                        }
                    },
                };
                match decode(verified) {
                    Ok(m) => {
                        if m.to != self_id {
                            // On a signed transport `to` has been authenticated, so this is no
                            // longer a peer's unchecked claim — but it is still not a security
                            // check. It catches the configuration mistake where two nodes were
                            // given one address, which otherwise shows up as one node mysteriously
                            // voting twice.
                            counters.misrouted.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                        // **Charged against a byte budget before it is queued.** The frame limit
                        // caps one frame and the channel to the caller is unbounded, so without
                        // this a peer outrunning the caller's drain chooses this process's memory
                        // however small each frame is. Refused rather than blocked: blocking here
                        // would park a connection thread where `shutdown` cannot reach it.
                        let charge = body.len();
                        let fits = counters
                            .inbox_bytes
                            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                                (cur + charge <= opts.inbox_bytes).then_some(cur + charge)
                            })
                            .is_ok();
                        if !fits {
                            counters.inbound_dropped.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                        counters.received.fetch_add(1, Ordering::SeqCst);
                        if tx.send((m, charge)).is_err() {
                            counters.inbox_bytes.fetch_sub(charge, Ordering::SeqCst);
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
