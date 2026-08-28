//! F6 — state transfer for a follower whose rounds have been checkpointed away.
//!
//! A leader can only send a follower *entries*. Once it has checkpointed, the rounds below its
//! floor are gone, and a follower asking for one of them is asking for something that no longer
//! exists anywhere on that node. The answer is not a longer log; it is **state** — this node's
//! database as of a round, streamed across, installed whole.
//!
//! # What a ferrodb snapshot is, and why it is unusually cheap to name
//!
//! A branch *is* a root pointer (`README.md` §1: forking "sets `child.root_page_id =
//! parent.root_page_id`… zero data pages read, written, or refcounted"). So the whole of this
//! node's state is nameable as
//!
//! ```text
//! { last_round, last_term, config, root_page_id, live arenas, branch catalog, catalog image }
//! ```
//!
//! [`SnapshotMeta`] carries the first three plus the transfer's length, and **must** carry the
//! configuration: a receiver that installed a snapshot without one would hold a database and no
//! idea what a majority of the cluster is, so it could neither vote nor count a vote. The rest
//! rides the streamed payload, described by [`PayloadHeader`].
//!
//! `SnapshotMeta`'s field set is deliberately not extended to hold them. It is a **wire format**
//! (`transport.rs::encode_snapshot_meta`), frozen alongside `Body::InstallSnapshot`; the content
//! belongs in the bytes the meta describes, not in the envelope that announces them.
//!
//! # The base-backup path is reused, not rewritten — and there was nothing to consolidate
//!
//! The brief for this row asked which of `replication/snapshot.rs` and `replication/backup.rs` is
//! "the better copy". **Neither: they are not copies.** Measured by reading both:
//!
//! * [`crate::replication::backup`] (E8) is the *physical* base backup — every page of the page
//!   file copied atomically through the buffer pool, plus the LSN window it corresponds to. It is
//!   what a replica is seeded from.
//! * [`crate::replication::snapshot`] (E12) is a *logical* CDC backfill — rows read through the
//!   engine's own MVCC and written as JSON Lines, plus the handoff point a stream resumes from. It
//!   holds no page and reads no page; its own header says so.
//!
//! They share no function and neither references the other, so there is no survivor to choose and
//! nothing to merge. The one an `InstallSnapshot` must stream is `backup`, and this module calls
//! [`crate::replication::backup::take`] and [`crate::replication::backup::restore`] rather than
//! walking pages a second time — including `restore`'s check that the image's byte length matches
//! the page count its label claims, which is exactly the truncated-transfer guard this row needs.
//!
//! `backup.rs`'s own header states the gap this row closes: *"The backup is transferred out of
//! band. `take` writes a directory; the replica reads one. Streaming it over the replication socket
//! is a separate concern and is not implemented."* This is that transfer.
//!
//! # Who holds the bytes, and why the two directions are not symmetric
//!
//! **A sender holds its snapshot in memory; a receiver never holds one.**
//!
//! That asymmetry is the point. A sender's snapshot size is a fact about its own database, which it
//! chose. A receiver's is a *claim by a remote peer*, and "how much memory does this process use"
//! must never be a number someone else picks — the same rule `transport.rs` applies to
//! `MAX_FRAME_BYTES` and `log.rs` to `MAX_ENTRY_BYTES`. So:
//!
//! * the state machine on the receiving side keeps [`RecvCursor`], which is **O(1)**: the parsed
//!   header, a byte count, and a resumable digest. Nothing is ever allocated from `total_bytes`;
//! * the bytes themselves go straight from the arriving frame to the driver's spool file, and the
//!   driver installs from that file. [`SnapshotStore::install`] takes a path, never a buffer.
//!
//! # What this module does NOT defend against, said plainly
//!
//! [`MAX_SNAPSHOT_BYTES`] is derived from the format's own field widths — a claim above it cannot
//! describe any image this build can produce, because `page_count` and the two section lengths are
//! `u32`s. It is an arithmetic impossibility check, and it is **not** a resource budget: a peer
//! that is permitted to speak this protocol at all can still ask this node to spool a large file.
//! What stops an *unauthorised* peer from doing that is F7's authentication, which is the layer
//! that owns the question; what stops an authorised one is the operator. Named here rather than
//! left for a reader to discover.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::config::Config;
use super::replicate::fnv64_update;
use super::{Action, Body, Consensus, Message, NodeId, Role, Round, Term};
use crate::error::FerroError;

#[cfg(test)]
#[path = "tests_snapshot.rs"]
mod tests_snapshot;

// ---------------------------------------------------------------------------------------------
// What a snapshot claims to be.
// ---------------------------------------------------------------------------------------------

/// What a snapshot claims to be, sent ahead of its bytes.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SnapshotMeta {
    /// The last round included. The receiver's log begins after this.
    pub last_round: Round,
    /// The term of `last_round`, needed for the log-matching check on the entries that follow.
    pub last_term: Term,
    /// The configuration in force at that round — a snapshot that did not carry it would leave the
    /// receiver unable to count a majority.
    pub config: Config,
    /// Total bytes, so a receiver can refuse an implausible transfer before allocating for it.
    pub total_bytes: u64,
}

impl SnapshotMeta {
    /// Everything a receiver can decide about a snapshot **from the envelope alone**, before a
    /// single byte of it has been written anywhere.
    ///
    /// Every check here is about the claim, never about the claimant, so it is the same on the
    /// sender (where it catches a caller building an impossible snapshot) and on the receiver
    /// (where it catches a peer describing one).
    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.last_round == 0 {
            // Round 0 means "before the log begins". A snapshot of it covers nothing, and
            // installing it would move a receiver's floor to a round that is not a round.
            return Err(SnapshotError::Refused(
                "a snapshot at round 0 covers nothing: round 0 is 'before the log begins', not a \
                 round, so there is no state to be the state of"
                    .into(),
            ));
        }
        if self.config.is_empty() {
            return Err(SnapshotError::Refused(format!(
                "the snapshot at round {} carries an empty voter set. A receiver that installed it \
                 would hold the database and no idea what a majority is, so it could neither vote \
                 nor count one — and nothing later in the protocol can detect a node counting \
                 against the wrong number.",
                self.last_round
            )));
        }
        if self.total_bytes < PayloadHeader::BYTES as u64 {
            return Err(SnapshotError::Refused(format!(
                "the snapshot at round {} claims {} bytes, which is smaller than the {}-byte header \
                 every payload begins with, so it cannot be one",
                self.last_round,
                self.total_bytes,
                PayloadHeader::BYTES
            )));
        }
        if self.total_bytes > MAX_SNAPSHOT_BYTES {
            return Err(SnapshotError::Refused(format!(
                "the snapshot at round {} claims {} bytes, above the {MAX_SNAPSHOT_BYTES}-byte \
                 ceiling this build's own format can describe (`page_count` and both section \
                 lengths are u32). Refused before anything was written or allocated for it.",
                self.last_round, self.total_bytes
            )));
        }
        Ok(())
    }
}

/// Why a snapshot was refused.
///
/// One variant and a string, because every caller does the same thing with it — refuse, and say
/// why — and a taxonomy nobody branches on is a taxonomy that drifts. The distinction that *is*
/// load-bearing lives in the type: this is not a [`FerroError`], so a `?` cannot quietly turn a
/// refused snapshot into a generic failure of the round it was serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    Refused(String),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::Refused(s) => write!(f, "snapshot refused: {s}"),
        }
    }
}

impl SnapshotError {
    pub fn into_ferro(self) -> FerroError {
        match self {
            SnapshotError::Refused(s) => FerroError::Internal(format!("snapshot refused: {s}")),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Limits, all of them derived rather than chosen.
// ---------------------------------------------------------------------------------------------

/// Bytes of payload in one `InstallSnapshot` frame.
///
/// Well under [`crate::replication::MAX_FRAME_BYTES`] (8 MiB), which the envelope, the meta and the
/// configuration also have to fit inside, and at least [`PayloadHeader::BYTES`] so the **first**
/// chunk always carries the whole header — which is what lets a receiver check the payload's own
/// self-description before accepting a second chunk. Both facts are asserted in `tests_snapshot`.
pub const SNAPSHOT_CHUNK_BYTES: usize = 1 << 20;

/// The largest `total_bytes` that could describe an image **this build's format can produce**.
///
/// Derived, not chosen: the payload is a header, an arena image and a branch-catalog image (each
/// length a `u32`), and a page image of `page_count` (`u32`) pages. Nothing larger is a ferrodb
/// snapshot, whatever a peer says. See the module header for what this is *not*.
pub const MAX_SNAPSHOT_BYTES: u64 = PayloadHeader::BYTES as u64
    + u32::MAX as u64
    + u32::MAX as u64
    + (u32::MAX as u64 * crate::storage::disk_manager::PAGE_SIZE as u64);

// ---------------------------------------------------------------------------------------------
// The payload: the ferrodb state a snapshot is of.
// ---------------------------------------------------------------------------------------------

/// The fixed-size self-description every snapshot payload begins with.
///
/// Fixed size on purpose. A receiver that keeps no bytes still has to answer "is this shape even
/// possible" on the first chunk, and it can only do that if the answer is entirely inside a prefix
/// whose length is known before anything is read.
///
/// Every field is fixed width and big-endian. There is deliberately **no string anywhere in this
/// format**: `wal::log::write_str` writes `s.len() as u16` unchecked, which silently truncates a
/// 65536-byte value into a length prefix of zero and produces a frame that is durable, correctly
/// checksummed, and permanently undecodable (`consensus/log.rs::Unrepresentable` names the same
/// hazard). A format with no strings cannot have that bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PayloadHeader {
    /// The sender's rolling log digest **at `last_round`**.
    ///
    /// The single most load-bearing field here, and the least obvious. `AppendResp.digest` is a
    /// rolling hash chained entry by entry from the digest at the log's floor, so a receiver that
    /// installed a snapshot and then anchored its chain at zero would compute a *different* digest
    /// from its leader at every round above the floor — and the leader's divergence detector would
    /// latch a perfectly healthy follower out of the quorum, permanently, with a message blaming
    /// byte-level corruption. `LogTail::base_digest`'s own doc says this value is "supplied by a
    /// snapshot install"; this is where it is supplied from.
    pub base_digest: u64,
    /// Restated from the meta so the payload is self-describing off the wire: a spool file found on
    /// disk after a crash says which round it is of, without the message that announced it.
    pub last_round: Round,
    pub last_term: Term,
    /// The trunk's root pointer — a branch *is* a root pointer, so this is the database.
    pub root_page_id: u32,
    /// `Catalog::first_catalog_page_id`. The catalog lives in ordinary data pages, so its *bytes*
    /// ride the page image; this is where to start reading them.
    pub catalog_page_id: u32,
    /// The LSN window the page image sits in — [`crate::replication::backup::BackupLabel`], carried
    /// verbatim so the install can hand it back to the code that wrote it.
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub page_count: u32,
    /// `ArenaPageStore::state_bytes()`.
    pub arena_len: u32,
    /// The branch catalog's own file format: `u32 length | BranchRecord::serialize()`, repeated.
    pub branches_len: u32,
    /// `page_count * PAGE_SIZE`, restated so a truncated transfer is arithmetic rather than a guess.
    pub image_len: u64,
    /// A resumable digest over everything after this header.
    pub body_digest: u64,
}

/// `b"FDBSNAP"` plus a format generation. A payload that does not begin with these bytes is not
/// refused for a *field* being wrong; it is refused for not being one of ours at all.
const MAGIC: [u8; 8] = *b"FDBSNAP\x01";
const FORMAT_VERSION: u32 = 1;

impl PayloadHeader {
    /// magic 8 | version 4 | base_digest 8 | last_round 8 | last_term 8 | root 4 | catalog 4 |
    /// start_lsn 8 | end_lsn 8 | page_count 4 | arena_len 4 | branches_len 4 | image_len 8 |
    /// body_digest 8 | header_digest 8
    pub const BYTES: usize = 8 + 4 + 8 + 8 + 8 + 4 + 4 + 8 + 8 + 4 + 4 + 4 + 8 + 8 + 8;

    fn encode(&self) -> [u8; Self::BYTES] {
        let mut b = [0u8; Self::BYTES];
        b[0..8].copy_from_slice(&MAGIC);
        b[8..12].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
        b[12..20].copy_from_slice(&self.base_digest.to_be_bytes());
        b[20..28].copy_from_slice(&self.last_round.to_be_bytes());
        b[28..36].copy_from_slice(&self.last_term.to_be_bytes());
        b[36..40].copy_from_slice(&self.root_page_id.to_be_bytes());
        b[40..44].copy_from_slice(&self.catalog_page_id.to_be_bytes());
        b[44..52].copy_from_slice(&self.start_lsn.to_be_bytes());
        b[52..60].copy_from_slice(&self.end_lsn.to_be_bytes());
        b[60..64].copy_from_slice(&self.page_count.to_be_bytes());
        b[64..68].copy_from_slice(&self.arena_len.to_be_bytes());
        b[68..72].copy_from_slice(&self.branches_len.to_be_bytes());
        b[72..80].copy_from_slice(&self.image_len.to_be_bytes());
        b[80..88].copy_from_slice(&self.body_digest.to_be_bytes());
        let d = digest(&b[..88]);
        b[88..96].copy_from_slice(&d.to_be_bytes());
        b
    }

    /// Read a header out of the front of a payload, refusing anything that is not one.
    ///
    /// **Every refusal here happens before a byte is spooled**, which is the whole reason the
    /// header is fixed-width and comes first.
    pub fn decode(bytes: &[u8]) -> Result<PayloadHeader, SnapshotError> {
        if bytes.len() < Self::BYTES {
            return Err(SnapshotError::Refused(format!(
                "a snapshot payload begins with a {}-byte header and only {} bytes were offered",
                Self::BYTES,
                bytes.len()
            )));
        }
        if bytes[0..8] != MAGIC {
            return Err(SnapshotError::Refused(
                "these bytes do not begin with the snapshot magic, so they are not a ferrodb \
                 snapshot payload. Refusing rather than guessing at the layout."
                    .into(),
            ));
        }
        let version = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(SnapshotError::Refused(format!(
                "snapshot payload format version {version}, and this build reads \
                 {FORMAT_VERSION}. Refusing rather than reading a later layout as this one — the \
                 fields would parse and mean something else."
            )));
        }
        let claimed = u64::from_be_bytes(bytes[88..96].try_into().unwrap());
        let actual = digest(&bytes[..88]);
        if claimed != actual {
            return Err(SnapshotError::Refused(format!(
                "the snapshot payload header does not match its own digest ({claimed:#018x} \
                 claimed, {actual:#018x} computed): the first {} bytes are damaged, and every \
                 length this transfer would be sized by is inside them",
                Self::BYTES
            )));
        }
        let h = PayloadHeader {
            base_digest: u64::from_be_bytes(bytes[12..20].try_into().unwrap()),
            last_round: u64::from_be_bytes(bytes[20..28].try_into().unwrap()),
            last_term: u64::from_be_bytes(bytes[28..36].try_into().unwrap()),
            root_page_id: u32::from_be_bytes(bytes[36..40].try_into().unwrap()),
            catalog_page_id: u32::from_be_bytes(bytes[40..44].try_into().unwrap()),
            start_lsn: u64::from_be_bytes(bytes[44..52].try_into().unwrap()),
            end_lsn: u64::from_be_bytes(bytes[52..60].try_into().unwrap()),
            page_count: u32::from_be_bytes(bytes[60..64].try_into().unwrap()),
            arena_len: u32::from_be_bytes(bytes[64..68].try_into().unwrap()),
            branches_len: u32::from_be_bytes(bytes[68..72].try_into().unwrap()),
            image_len: u64::from_be_bytes(bytes[72..80].try_into().unwrap()),
            body_digest: u64::from_be_bytes(bytes[80..88].try_into().unwrap()),
        };
        h.check_internally_consistent()?;
        Ok(h)
    }

    /// The claims this header makes about itself, checked against each other.
    ///
    /// A header can be perfectly intact and still describe an impossible payload — that is a
    /// different failure from damage and it is refused separately, because the two have different
    /// causes (a bug in a sender, versus a byte that changed in flight).
    fn check_internally_consistent(&self) -> Result<(), SnapshotError> {
        let page_bytes = self.page_count as u64 * crate::storage::disk_manager::PAGE_SIZE as u64;
        if self.image_len != page_bytes {
            return Err(SnapshotError::Refused(format!(
                "the snapshot header says {} pages but {} image bytes, and {} pages is {page_bytes} \
                 bytes. `backup::restore` refuses exactly this mismatch at restore time; refusing \
                 it here means nothing is spooled for it first.",
                self.page_count, self.image_len, self.page_count
            )));
        }
        if self.last_round == 0 {
            return Err(SnapshotError::Refused(
                "the snapshot header names round 0, which is 'before the log begins' and not a \
                 round"
                    .into(),
            ));
        }
        if self.page_count == 0 {
            // `backup::take` refuses to write a zero-page backup for the same reason it is refused
            // here: restoring one produces an empty database that looks like a successful restore.
            return Err(SnapshotError::Refused(
                "the snapshot header claims 0 pages. A zero-page image is not a backup of an empty \
                 database, it is a backup that collected nothing, and installing it would replace a \
                 follower's state with nothing while reporting success."
                    .into(),
            ));
        }
        Ok(())
    }

    /// What `total_bytes` must be for a payload with this header.
    pub fn total_bytes(&self) -> u64 {
        Self::BYTES as u64 + self.arena_len as u64 + self.branches_len as u64 + self.image_len
    }
}

/// Whether a transfer that says it is **done** actually is: the right length, and the right bytes.
///
/// A named function rather than two branches inside the receive path, and the reason is a mutant.
/// Inline, the length check is unreachable independently of the digest check — a short transfer
/// fails both, and a long one has already been refused by the per-chunk bound — so removing it
/// changed nothing any behavioural test could see. A rule no test can distinguish is a rule nobody
/// has shown matters. Asked directly, each half is answerable on its own.
///
/// **The length check stays in front of the digest even though the digest would catch the same
/// case.** It is certain where a digest is probabilistic, it is a comparison of two integers rather
/// than a fold over a gigabyte, and it names what is actually wrong: the sender and this node
/// disagree about how much a whole snapshot is, which is a different fault from bytes that changed
/// in flight.
fn completion_verdict(
    received: u64,
    total: u64,
    body_digest: u64,
    claimed: u64,
) -> Result<(), CompletionFault> {
    if received != total {
        // The sender and this node disagree about how much a whole snapshot is. The bytes accepted
        // so far are not in question — only the claim that they are all of them.
        return Err(CompletionFault::Length);
    }
    if body_digest != claimed {
        // Every byte arrived and they are not the bytes the sender digested. Which byte is wrong is
        // exactly what a digest cannot say, so no prefix of this transfer is worth keeping.
        return Err(CompletionFault::Digest);
    }
    Ok(())
}

/// Why a completed transfer was refused. **Two variants because the two are recovered differently**,
/// not for the sake of a taxonomy: one keeps the bytes already accepted and one cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionFault {
    Length,
    Digest,
}

/// FNV-1a over `bytes`, from the standard offset basis.
///
/// The same function `replicate.rs` chains its log digests with, reused rather than copied: one
/// hash in the consensus layer means a snapshot's digest and a log's digest cannot come to disagree
/// about what hashing is. Resumable, which is what lets a receiver digest a stream it never holds.
fn digest(bytes: &[u8]) -> u64 {
    fnv64_update(super::replicate::FNV_OFFSET, bytes)
}

/// A snapshot a **sender** holds: the envelope, and the bytes it describes.
///
/// The bytes are in memory. See the module header for why that is right for a sender and wrong for
/// a receiver.
#[derive(Clone, PartialEq)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub header: PayloadHeader,
    bytes: Arc<Vec<u8>>,
}

impl std::fmt::Debug for Snapshot {
    /// Prints the *length* of the payload and never the payload. A derived `Debug` here puts a
    /// gigabyte into a panic message, and the panic is usually about something else.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("meta", &self.meta)
            .field("header", &self.header)
            .field("payload_bytes", &self.bytes.len())
            .finish()
    }
}

impl Snapshot {
    /// Build a payload out of the four things a ferrodb snapshot is made of, and the point it is
    /// taken at.
    ///
    /// The header is computed here and nowhere else, so a payload whose header disagrees with its
    /// own body cannot be constructed by an in-tree caller.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        at: &SnapshotPoint,
        root_page_id: u32,
        catalog_page_id: u32,
        label: crate::replication::backup::BackupLabel,
        arena: &[u8],
        branches: &[u8],
        image: Vec<u8>,
    ) -> Result<Snapshot, SnapshotError> {
        let arena_len = fits_u32(arena.len(), "the arena image")?;
        let branches_len = fits_u32(branches.len(), "the branch catalog image")?;

        let mut body = Vec::with_capacity(arena.len() + branches.len() + image.len());
        body.extend_from_slice(arena);
        body.extend_from_slice(branches);
        body.extend_from_slice(&image);

        let header = PayloadHeader {
            base_digest: at.base_digest,
            last_round: at.last_round,
            last_term: at.last_term,
            root_page_id,
            catalog_page_id,
            start_lsn: label.start_lsn,
            end_lsn: label.end_lsn,
            page_count: label.page_count,
            arena_len,
            branches_len,
            image_len: image.len() as u64,
            body_digest: digest(&body),
        };
        header.check_internally_consistent()?;

        let mut bytes = Vec::with_capacity(PayloadHeader::BYTES + body.len());
        bytes.extend_from_slice(&header.encode());
        bytes.extend_from_slice(&body);

        let meta = SnapshotMeta {
            last_round: at.last_round,
            last_term: at.last_term,
            config: at.config.clone(),
            total_bytes: bytes.len() as u64,
        };
        meta.validate()?;
        Ok(Snapshot { meta, header, bytes: Arc::new(bytes) })
    }

    pub fn payload(&self) -> &[u8] {
        &self.bytes
    }

    /// The chunk at `offset`, and whether it is the last one.
    fn chunk(&self, offset: u64) -> (&[u8], bool) {
        let from = (offset as usize).min(self.bytes.len());
        let to = (from + SNAPSHOT_CHUNK_BYTES).min(self.bytes.len());
        (&self.bytes[from..to], to == self.bytes.len())
    }
}

fn fits_u32(len: usize, what: &'static str) -> Result<u32, SnapshotError> {
    u32::try_from(len).map_err(|_| {
        SnapshotError::Refused(format!(
            "{what} is {len} bytes, which the format's u32 length field cannot express. Refused \
             rather than truncated: a wrapped length writes a payload that is correctly digested \
             and permanently undecodable."
        ))
    })
}

/// The round a snapshot taken **now** would cover, computed by the state machine.
///
/// The driver does not get to choose this. Every field here is one the state machine already knows
/// and the driver would otherwise have to re-derive — and a snapshot taken at the wrong round, or
/// carrying the wrong term or the wrong configuration, is not detectable by the receiver, which has
/// nothing to compare it against.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotPoint {
    /// The highest round handed to the storage engine. **Not `commit`**: the payload is an image of
    /// the engine, so it covers exactly what the engine has been given, and `applied <= commit`
    /// makes it committed state by construction.
    pub last_round: Round,
    pub last_term: Term,
    pub config: Config,
    pub base_digest: u64,
}

// ---------------------------------------------------------------------------------------------
// The seam to the storage engine.
// ---------------------------------------------------------------------------------------------

/// What a node must be able to do with its own state for state transfer to work.
///
/// A trait because the state machine may not touch a disk and the driver may not know what a
/// ferrodb page is. [`PageStoreSnapshots`] is the real implementation; a node that has no storage
/// engine attached simply has none of these, and its driver **refuses loudly** rather than stalling
/// a follower it can never repair.
pub trait SnapshotStore: Send {
    /// Everything this node has applied through `at.last_round`, as one transferable payload.
    fn capture(&mut self, at: &SnapshotPoint) -> Result<Snapshot, FerroError>;

    /// Replace this node's entire state with the payload in `path`, **durably before returning**.
    ///
    /// A path and not a buffer: the payload arrived from a peer and was never held in memory, and
    /// reading it back in to install it would give that peer the memory footprint the streaming
    /// exists to deny them.
    ///
    /// Must leave nothing of the old state readable — files *and* whatever is cached in front of
    /// them. An implementation that replaced only the files would leave this node answering from
    /// an in-memory index of a database it no longer has, and the only sign of it would be a
    /// restart much later that quietly changed the answers.
    fn install(&mut self, meta: &SnapshotMeta, path: &Path) -> Result<(), FerroError>;
}

// ---------------------------------------------------------------------------------------------
// Per-peer transfer state. Lives in `Progress` — see `replicate.rs`'s module header for why the
// frozen `Consensus` has no field of its own for it.
// ---------------------------------------------------------------------------------------------

/// A transfer this **leader** is performing to one peer.
///
/// One chunk in flight at a time. Stop-and-wait, deliberately: the acknowledgement is an absolute
/// byte cursor (`InstallSnapshotResp.received_through`), so a window would need a second structure
/// to track holes, and the frozen response carries nothing to describe one with. The cost is a
/// round trip per chunk and the chunk is a megabyte.
#[derive(Debug, Clone, PartialEq)]
pub struct SendCursor {
    snapshot: Arc<Snapshot>,
    /// Bytes the peer has confirmed. The next chunk starts here.
    acked: u64,
}

impl SendCursor {
    pub fn round(&self) -> Round {
        self.snapshot.meta.last_round
    }
    pub fn acked(&self) -> u64 {
        self.acked
    }
    pub fn total(&self) -> u64 {
        self.snapshot.meta.total_bytes
    }
}

/// A transfer this **follower** is receiving.
///
/// O(1) in the size of the snapshot, and that is a requirement rather than an optimisation: every
/// field here is derived from bytes as they pass through, and nothing is ever sized from
/// `total_bytes`.
#[derive(Debug, Clone, PartialEq)]
pub struct RecvCursor {
    meta: SnapshotMeta,
    header: PayloadHeader,
    /// Bytes accepted so far. Also the offset the next chunk must arrive at.
    received: u64,
    /// The body digest so far — everything past the header.
    body_digest: u64,
    /// Set once the last chunk has been accepted and the payload verified. The driver takes it
    /// from here, installs it, and reports back with `Event::Persisted`.
    complete: bool,
}

impl RecvCursor {
    pub fn meta(&self) -> &SnapshotMeta {
        &self.meta
    }
    pub fn header(&self) -> &PayloadHeader {
        &self.header
    }
    pub fn received(&self) -> u64 {
        self.received
    }
    pub fn is_complete(&self) -> bool {
        self.complete
    }
}

// ---------------------------------------------------------------------------------------------
// The protocol.
// ---------------------------------------------------------------------------------------------

impl Consensus {
    /// The point a snapshot taken now would cover, or `None` if there is nothing to snapshot.
    ///
    /// `None` when nothing has been applied: an image of a database that has agreed on no round is
    /// not a snapshot of anything, and `backup::take` refuses a zero-page image for the same
    /// reason.
    pub fn snapshot_point(&self) -> Option<SnapshotPoint> {
        if self.applied == 0 {
            return None;
        }
        let last_term = self.term_at(self.applied)?;
        let base_digest = self.digest_at(self.applied)?;
        Some(SnapshotPoint {
            last_round: self.applied,
            last_term,
            config: self.cfg.clone(),
            base_digest,
        })
    }

    /// Discard this node's log prefix through `through`, because a snapshot now covers it.
    ///
    /// The state-machine half of a checkpoint. The driver does the same to the durable
    /// [`crate::consensus::log::RoundLog`] in the same step, and the two must not be allowed to
    /// drift: `ensure_log` refuses on the very next entry point if the tail no longer describes
    /// `snapshot_round`.
    ///
    /// **`applied` is the ceiling, not `commit`.** A snapshot is an image of the storage engine, so
    /// a floor above what the engine has been given would be a claim that this node holds state it
    /// has never seen — and the rounds proving otherwise would have just been discarded.
    pub fn compact(&mut self, through: Round) -> Result<(), SnapshotError> {
        if through <= self.snapshot_round {
            // Already at or below the floor. Not an error: a retention policy runs every turn and
            // most turns have nothing to do.
            return Ok(());
        }
        if through > self.applied {
            return Err(SnapshotError::Refused(format!(
                "cannot checkpoint through round {through}: this node has applied only through {}.                  A floor above what the storage engine holds is a claim about state that was never                  installed, and the rounds that would prove otherwise are what the checkpoint                  discards.",
                self.applied
            )));
        }
        if through > self.last_round {
            return Err(SnapshotError::Refused(format!(
                "cannot checkpoint through round {through}: this node's log ends at {}",
                self.last_round
            )));
        }
        let Some(term) = self.term_at(through) else {
            return Err(SnapshotError::Refused(format!(
                "cannot checkpoint through round {through}: this node cannot answer for its term"
            )));
        };
        let base_digest = self.digest_at(through).unwrap_or(0);
        self.compact_tail(through, base_digest);
        self.snapshot_round = through;
        self.snapshot_term = term;
        Ok(())
    }

    /// The rolling digest at this node's own snapshot floor.
    ///
    /// The driver records this beside the floor, because it is the one thing a restart cannot
    /// recompute: every entry it was folded over has been discarded. See [`PayloadHeader::base_digest`].
    pub fn floor_digest(&self) -> u64 {
        self.digest_at(self.snapshot_round).unwrap_or(0)
    }

    /// Peers this leader cannot serve with entries, in id order.
    ///
    /// The driver reads this every turn and arms a transfer for each. Nothing here sends: a peer
    /// with no snapshot armed is simply skipped by `send_append_to`, and the *next* turn arms one.
    pub fn peers_needing_snapshot(&self) -> Vec<NodeId> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        self.progress
            .iter()
            .filter(|(n, p)| **n != self.self_id && p.needs_snapshot && p.sending.is_none())
            .map(|(n, _)| *n)
            .collect()
    }

    /// Arm a transfer of `snap` to `peer`.
    ///
    /// **Every check the receiver cannot make is made here.** A receiver has nothing to compare a
    /// snapshot against — that is the whole reason it needs one — so a snapshot that names the
    /// wrong term at its own last round, or covers a round this leader cannot then continue from,
    /// is caught on the only node that can catch it.
    pub fn offer_snapshot_to(
        &mut self,
        peer: NodeId,
        snap: Arc<Snapshot>,
    ) -> Result<(), SnapshotError> {
        if self.role != Role::Leader {
            return Err(SnapshotError::Refused(format!(
                "node {} is {} and only a leader may serve a snapshot",
                self.self_id, self.role
            )));
        }
        if !self.cfg.is_known(peer) {
            return Err(SnapshotError::Refused(format!(
                "{peer} is in no configuration this node holds, so serving it state would be \
                 seeding a node the cluster has removed"
            )));
        }
        snap.meta.validate()?;
        let m = &snap.meta;

        if m.last_round > self.applied {
            return Err(SnapshotError::Refused(format!(
                "the snapshot covers round {} but this leader has applied only through {}. The \
                 payload is an image of the storage engine, so it cannot describe a round the \
                 engine has not been given.",
                m.last_round, self.applied
            )));
        }
        if m.last_round < self.snapshot_round {
            return Err(SnapshotError::Refused(format!(
                "the snapshot covers round {} but this leader's own log begins after {}, so after \
                 installing it the peer would need entries this leader no longer holds — a second \
                 transfer, immediately.",
                m.last_round, self.snapshot_round
            )));
        }
        match self.term_at(m.last_round) {
            Some(t) if t == m.last_term => {}
            Some(t) => {
                return Err(SnapshotError::Refused(format!(
                    "the snapshot says round {} is of term {}, and this leader's log says term {t}. \
                     One of them is a snapshot of a different history, and `(round, term)` is the \
                     pair every later log-matching check is decided by.",
                    m.last_round, m.last_term
                )));
            }
            None => {
                return Err(SnapshotError::Refused(format!(
                    "this leader cannot answer for round {}, so it cannot vouch for a snapshot of \
                     it",
                    m.last_round
                )));
            }
        }
        if snap.header.base_digest != self.digest_at(m.last_round).unwrap_or(0) {
            return Err(SnapshotError::Refused(format!(
                "the snapshot's base digest {:#018x} is not this leader's digest at round {} \
                 ({:#018x}). Installing it would anchor the receiver's digest chain somewhere this \
                 leader is not, and every acknowledgement above the floor would then report a \
                 divergence that is not there.",
                snap.header.base_digest,
                m.last_round,
                self.digest_at(m.last_round).unwrap_or(0)
            )));
        }

        let p = self.progress.entry(peer).or_default();
        p.needs_snapshot = true;
        p.sending = Some(SendCursor { snapshot: snap, acked: 0 });
        Ok(())
    }

    /// The transfer this leader has in flight to `peer`, if any. For the driver and for tests.
    pub fn snapshot_in_flight(&self, peer: NodeId) -> Option<&SendCursor> {
        self.progress.get(&peer)?.sending.as_ref()
    }

    /// The transfer this node is receiving, if any.
    pub fn snapshot_incoming(&self) -> Option<&RecvCursor> {
        self.progress.get(&self.self_id)?.receiving.as_ref()
    }

    /// Send the next chunk of an armed transfer.
    ///
    /// Called from `send_append_to` in place of the `Append` a peer below this leader's floor
    /// cannot be sent. With nothing armed it sends nothing — the driver arms one on its next turn,
    /// and a bare `return` here is what the F2 stub did, which muted the peer for ever because the
    /// only thing that cleared `needs_snapshot` was a success that could no longer arrive.
    pub(crate) fn send_snapshot_chunk_to(&mut self, peer: NodeId, out: &mut Vec<Action>) {
        let term = self.hard.term;
        let me = self.self_id;
        let Some(p) = self.progress.get(&peer) else { return };
        let Some(cur) = p.sending.as_ref() else { return };
        let (data, done) = cur.snapshot.chunk(cur.acked);
        let msg = Message {
            from: me,
            to: peer,
            term,
            body: Body::InstallSnapshot {
                meta: cur.snapshot.meta.clone(),
                offset: cur.acked,
                data: data.to_vec(),
                done,
            },
        };
        out.push(Action::Send(msg));
    }

    /// The leader half: a peer reports how much of the transfer it holds.
    pub(crate) fn on_install_snapshot_resp(
        &mut self,
        from: NodeId,
        received_through: u64,
        out: &mut Vec<Action>,
    ) {
        if self.role != Role::Leader || from == self.self_id {
            return;
        }
        if !self.cfg.is_known(from) {
            // A removed node keeps running and keeps answering. Its answers must not resurrect a
            // `Progress` for a set it has left — the same rule `on_append_resp` applies.
            return;
        }

        // **A peer mid-transfer answers no `Append`, so without this its `silent` counter keeps
        // climbing and eats this leader's own lease** (`election.rs`: a leader that stops hearing
        // from a majority steps down). A snapshot to one peer of three would otherwise cost the
        // leader its office for the duration of the transfer — the lease firing on a cluster that
        // is perfectly healthy and merely busy.
        self.progress.entry(from).or_default().silent = 0;

        let Some(cur) = self.progress.get(&from).and_then(|p| p.sending.clone()) else {
            // No transfer armed: a duplicate or a late answer to one that already finished. There
            // is nothing to advance and nothing to resend.
            return;
        };

        if received_through > cur.total() {
            // Not possible from a correct receiver; a peer cannot hold more of a transfer than it
            // has. Ignored rather than clamped: a claim this wrong is not evidence about anything.
            return;
        }
        if received_through == cur.acked {
            // Nothing new. Not an error — a duplicated frame — and answering it with another chunk
            // would answer a duplicate with a duplicate for ever.
            return;
        }

        // **A lower report moves the cursor BACKWARDS, deliberately.** The receiver is the only
        // authority on what it holds, and the case that decides this is a follower that crashes
        // mid-transfer: it comes back holding nothing and says so, and a sender that treated its
        // own cursor as monotonic would go on sending chunks from the middle of a payload the
        // receiver can never complete — a follower that is never repaired, with both nodes healthy
        // and both behaving. A duplicated stale report costs one re-sent chunk and then converges,
        // because the receiver answers its real position to the very next one. Bandwidth is the
        // cheaper of the two failures by an unbounded margin.
        let done = received_through == cur.total();
        {
            let p = self.progress.entry(from).or_default();
            if let Some(c) = p.sending.as_mut() {
                c.acked = received_through;
            }
            if done {
                // **`next`, and deliberately NOT `matched`.** `next` is optimism and is corrected
                // by a refusal; `matched` is what quorum is counted over. A byte cursor says the
                // peer received the payload, never that it made the payload durable — so treating
                // a completed transfer as a match would count a replica of state this leader has no
                // evidence is on that peer's disk. The peer's next `AppendResp` is that evidence,
                // and if the install did not survive, that same append is refused with a hint at
                // the peer's real floor and the transfer simply happens again.
                p.sending = None;
                p.needs_snapshot = false;
                p.next = cur.round() + 1;
            }
        }

        if done {
            self.send_append_to(from, out);
        } else {
            self.send_snapshot_chunk_to(from, out);
        }
    }

    /// The follower half: a chunk of state arrives.
    ///
    /// Returns nothing and installs nothing. It decides whether the chunk is acceptable, records
    /// exactly enough to decide the next one, and answers with the byte cursor the sender resumes
    /// from. The bytes are the driver's: see the module header.
    pub(crate) fn on_install_snapshot(
        &mut self,
        from: NodeId,
        term: Term,
        meta: SnapshotMeta,
        offset: u64,
        data: Vec<u8>,
        done: bool,
        out: &mut Vec<Action>,
    ) {
        // A leader sending state is still a leader. Adopting it here and not only on `Append` is
        // what stops this node campaigning through a term that already has one, during a transfer
        // that may take many round trips — exactly the window in which no `Append` arrives.
        let adopting = self.leader != Some(from);
        if self.role != Role::Follower || adopting || self.hard.term != term {
            self.become_follower(term, Some(from), out);
        } else {
            self.since_heard = 0;
        }
        if adopting {
            // Nothing is established with THIS leader yet, whatever was with the last one.
            self.set_agreed(0);
        }

        // ---- refusals, every one of them before a byte is spooled -------------------------------

        if meta.validate().is_err() {
            self.refuse_snapshot(from, out);
            return;
        }
        if meta.last_round <= self.snapshot_round {
            // This node's floor is already at or above the snapshot's. Installing it would move the
            // floor backwards, and it would replace state this node has with older state.
            // Answered as complete so the sender stops resending and moves to entries, which is
            // what it would have done had it known this node's floor.
            self.ack_snapshot(from, meta.total_bytes, out);
            return;
        }
        if self.term_at(meta.last_round) == Some(meta.last_term) {
            // This node already holds `(last_round, last_term)` in its own log, so by the
            // log-matching property it already holds everything the snapshot covers. There is
            // nothing to transfer.
            self.ack_snapshot(from, meta.total_bytes, out);
            return;
        }

        let held = self.snapshot_incoming().cloned();
        let mut cur = if offset == 0 {
            // **A chunk at offset 0 is a transfer starting, and the header decides whether it is
            // THIS transfer.**
            //
            // Keying on the meta alone is not enough and the difference is not academic: two
            // captures of the same round have the same `(last_round, last_term, config,
            // total_bytes)` and different bytes, because a base backup copies pages while writes
            // land and is a smear of states rather than an instant (`backup.rs`'s own header says
            // so). So a transfer interrupted by a leader change and resumed by the new leader's
            // capture would splice two images by offset, digest to nothing at the end, and — since
            // a refusal does not destroy the cursor — do it again for ever.
            //
            // The header carries `body_digest`, so header equality is payload equality. Equal
            // means this is a duplicate of a first chunk already accepted and the cursor stands;
            // different means a different payload and the cursor restarts.
            let Ok(header) = PayloadHeader::decode(&data) else {
                self.refuse_snapshot(from, out);
                return;
            };
            if header.last_round != meta.last_round
                || header.last_term != meta.last_term
                || header.total_bytes() != meta.total_bytes
            {
                // The envelope and the payload disagree about what this is. Refused rather than
                // resolved in favour of either: nothing here can tell which is lying.
                self.refuse_snapshot(from, out);
                return;
            }
            match held {
                Some(c) if c.header == header && c.received > 0 => {
                    // A duplicate of the first chunk of a transfer already under way. Answering the
                    // real cursor rather than restarting is what keeps one duplicated frame from
                    // costing a transfer that is nearly finished.
                    let n = c.received;
                    self.progress.entry(self.self_id).or_default().receiving = Some(c);
                    self.ack_snapshot(from, n, out);
                    return;
                }
                _ => RecvCursor {
                    meta: meta.clone(),
                    header,
                    received: 0,
                    body_digest: super::replicate::FNV_OFFSET,
                    complete: false,
                },
            }
        } else {
            match held {
                Some(c) if c.meta == meta => c,
                _ => {
                    // The sender is resuming a transfer this node is not holding — it crashed, or
                    // this is a reorder. Answer 0 so it restarts from the header, which is the only
                    // offset at which a payload can be validated at all.
                    self.ack_snapshot(from, 0, out);
                    return;
                }
            }
        };

        if cur.complete {
            // Already received whole and waiting on the driver. Re-answer the same cursor; the
            // sender is repeating itself because this node has not yet reported the install.
            let n = cur.received;
            self.progress.entry(self.self_id).or_default().receiving = Some(cur);
            self.ack_snapshot(from, n, out);
            return;
        }

        if offset != cur.received {
            // Out of order, duplicated, or a hole. Answered with the resume point rather than
            // buffered: buffering a hole means holding bytes whose position a peer chose, and the
            // frozen response has no way to describe more than one cursor anyway.
            let n = cur.received;
            self.progress.entry(self.self_id).or_default().receiving = Some(cur);
            self.ack_snapshot(from, n, out);
            return;
        }
        if cur.received + data.len() as u64 > meta.total_bytes {
            // A chunk that would push the transfer past its own declared length. Refused whole:
            // accepting a prefix of it would leave the digest over bytes nobody declared.
            self.refuse_snapshot(from, out);
            return;
        }

        // ---- accept ----------------------------------------------------------------------------

        // Digest only the body. The header is already checked against its own digest, and
        // re-including it would mean the sender had to compute `body_digest` over bytes that
        // contain `body_digest`.
        let body_from = (PayloadHeader::BYTES as u64).saturating_sub(cur.received) as usize;
        if body_from < data.len() {
            cur.body_digest = fnv64_update(cur.body_digest, &data[body_from..]);
        }
        cur.received += data.len() as u64;

        if done {
            // **The two halves of the completion rule are answered differently, because they are
            // recoverable differently.**
            //
            // A `done` at the wrong LENGTH says nothing about the bytes: only the claim of
            // completeness is wrong, and the megabytes already accepted are still good. Keeping the
            // cursor and answering with what is held lets the sender carry on from there — and
            // means one truncated or forged frame cannot cost a gigabyte of progress.
            //
            // A DIGEST mismatch at the right length is the opposite: some byte in the payload is
            // wrong and nothing here can say which, so there is no prefix worth keeping. The cursor
            // goes and the answer is zero, which is where the sender restarts from — the only
            // offset at which a payload can be validated at all.
            match completion_verdict(
                cur.received,
                meta.total_bytes,
                cur.body_digest,
                cur.header.body_digest,
            ) {
                Ok(()) => cur.complete = true,
                Err(CompletionFault::Length) => {
                    let n = cur.received;
                    self.progress.entry(self.self_id).or_default().receiving = Some(cur);
                    self.ack_snapshot(from, n, out);
                    return;
                }
                Err(CompletionFault::Digest) => {
                    self.progress.entry(self.self_id).or_default().receiving = None;
                    self.ack_snapshot(from, 0, out);
                    return;
                }
            }
        }

        let n = cur.received;
        self.progress.entry(self.self_id).or_default().receiving = Some(cur);
        self.ack_snapshot(from, n, out);
    }

    /// The install is durable on this node's disk. Move the state machine to it.
    ///
    /// Called from `on_persisted` when the driver reports a round that is the pending snapshot's,
    /// and **only** then. Nothing here may run on the arrival of the last chunk: bytes in flight are
    /// not bytes on a disk, and a state machine that moved its floor to a round its storage engine
    /// had not yet accepted would answer for a history it does not hold — rule 4's failure, in a
    /// different costume.
    pub(crate) fn finish_install(&mut self, out: &mut Vec<Action>) -> Result<(), FerroError> {
        let Some(cur) = self.snapshot_incoming().cloned() else { return Ok(()) };
        if !cur.complete {
            return Ok(());
        }

        // The configuration goes in through F5's single seam, which refuses damage — an empty voter
        // set, or a `(version, term)` colliding with a different configuration — by latching this
        // node out of office. Propagated, never swallowed: the step-down IS the enforcement.
        self.note_config_in_log(cur.meta.config.clone(), out)?;

        self.snapshot_round = cur.meta.last_round;
        self.snapshot_term = cur.meta.last_term;
        self.last_round = cur.meta.last_round;
        self.last_term = cur.meta.last_term;
        // The whole log is discarded, not just the prefix. A follower needing state transfer holds
        // rounds this leader cannot vouch for at all — that is why it is being re-seeded — and
        // `RoundLog::discard_prefix` does the same on the disk side when `through` is above the end
        // of the log.
        self.install_snapshot_tail(cur.meta.last_round, cur.header.base_digest);

        // **The round watermark, which is what clears `unjoined` — through F1's rule, not here.**
        // `DISTRIBUTED.md` §F6 says an install "ends by setting the follower's round watermark,
        // which is also what clears its `unjoined` flag (F1)", and the emphasis is on *setting the
        // watermark*. Clearing the flag here instead would be the defect `mod.rs` names on the
        // field: `meta.last_round` is the sender's compaction floor, at or below its commit and
        // normally below it, so "installed" proves this node holds the snapshot's prefix and never
        // that it holds what a quorum holds. The next `Append` carries the leader's real `commit`,
        // `observe_quorum_watermark` compares it against this `durable`, and the flag clears on
        // evidence — with the empty-cluster zero case already handled there.
        self.durable = cur.meta.last_round;

        // A snapshot is committed state by construction (`offer_snapshot_to` refuses one above the
        // sender's `applied`, and `applied <= commit`), and the storage engine now holds it. So
        // `commit` and `applied` move with the floor — unlike a restored log *tail*, whose
        // committedness is a fact about a quorum that only a leader's `Append` can re-establish.
        // Leaving `applied` behind would make the next `Action::Apply` walk rounds that no longer
        // exist anywhere.
        self.commit = self.commit.max(cur.meta.last_round);
        self.applied = self.applied.max(cur.meta.last_round);
        self.set_agreed(cur.meta.last_round);

        self.progress.entry(self.self_id).or_default().receiving = None;
        Ok(())
    }

    /// The round a completed-but-not-yet-installed snapshot covers, for the driver.
    pub fn pending_install_round(&self) -> Option<Round> {
        let cur = self.snapshot_incoming()?;
        cur.complete.then_some(cur.meta.last_round)
    }

    /// The answer that says "I hold `n` bytes of it".
    fn ack_snapshot(&mut self, to: NodeId, n: u64, out: &mut Vec<Action>) {
        out.push(Action::Send(Message {
            from: self.self_id,
            to,
            term: self.hard.term,
            body: Body::InstallSnapshotResp { received_through: n },
        }));
    }

    /// The answer that says "I accepted nothing", which is also `mod.rs`'s stale-term refusal.
    ///
    /// A refusal deliberately **does not** destroy a transfer already in flight. One damaged or
    /// impossible chunk is not evidence about the megabytes already accepted, and dropping them
    /// would turn a single corrupted frame into a restart of the whole transfer.
    fn refuse_snapshot(&mut self, to: NodeId, out: &mut Vec<Action>) {
        let n = self.snapshot_incoming().map_or(0, |c| c.received);
        self.ack_snapshot(to, n, out);
    }
}

// ---------------------------------------------------------------------------------------------
// The real store: ferrodb's own base backup, streamed.
// ---------------------------------------------------------------------------------------------

/// A [`SnapshotStore`] over ferrodb's storage engine.
///
/// Every byte it produces comes from code that already existed and is already tested:
/// [`crate::replication::backup::take`] for the page image and its LSN window,
/// `ArenaPageStore::state_bytes` for the live arenas, and the branch catalog's own on-disk record
/// stream for the branch metadata. Nothing here walks a page.
///
/// # One conservative residue, stated
///
/// `DiskManager::next_page_id` is an in-memory allocator cursor and an install does not lower it,
/// so a node re-seeded from a *smaller* database keeps allocating above the old high-water mark
/// until it restarts. That wastes page ids and cannot hand out a live page —
/// `DiskManager::high_water` is defined as the max of the bitmap scan and this cursor precisely so
/// it never regresses. Named because the safe direction of a stale allocator is not obvious from
/// reading it.
pub struct PageStoreSnapshots {
    pool: Arc<crate::buffer::buffer_pool::BufferPoolManager>,
    wal: Arc<crate::wal::log::WalManager>,
    arenas: Arc<crate::branch::arena::ArenaPageStore>,
    branches: Arc<crate::branch::catalog::LogBranchCatalog>,
    /// `Catalog::first_catalog_page_id` for this node. Carried rather than assumed to be 1: the
    /// value is a field on `Catalog` and a snapshot that hard-coded it would install a database
    /// whose catalog the receiver looks for in the wrong page.
    catalog_page_id: u32,
    /// Where this node's page file, arena image and branch catalog live, so an install can replace
    /// them.
    paths: StorePaths,
    /// Scratch for `backup::take`, which writes a directory.
    scratch: PathBuf,
}

/// The three files a snapshot install replaces, plus the page file it restores into.
#[derive(Debug, Clone)]
pub struct StorePaths {
    pub page_file: PathBuf,
    pub arena_image: PathBuf,
    pub branch_catalog: PathBuf,
}

impl PageStoreSnapshots {
    pub fn new(
        pool: Arc<crate::buffer::buffer_pool::BufferPoolManager>,
        wal: Arc<crate::wal::log::WalManager>,
        arenas: Arc<crate::branch::arena::ArenaPageStore>,
        branches: Arc<crate::branch::catalog::LogBranchCatalog>,
        catalog_page_id: u32,
        paths: StorePaths,
        scratch: impl Into<PathBuf>,
    ) -> Self {
        PageStoreSnapshots {
            pool,
            wal,
            arenas,
            branches,
            catalog_page_id,
            paths,
            scratch: scratch.into(),
        }
    }

    /// The branch catalog as its own file format: `u32 length | BranchRecord::serialize()`.
    ///
    /// The catalog's own `replay` reads exactly this, so a receiver installs by writing the bytes
    /// and reopening. That matters: `LogBranchCatalog::put` neither advances the epoch and id
    /// counters nor removes records the sender no longer has, so replaying a snapshot's records one
    /// by one into a live catalog would leave a follower minting branch ids that collide and
    /// holding branches the leader reaped.
    fn branch_image(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for r in self.branches.all_records() {
            let bytes = r.serialize();
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        out
    }

    fn trunk_root(&self) -> Result<u32, FerroError> {
        Ok(self.branches.get_raw(crate::branch::types::BranchId::TRUNK.id)?.root_page_id)
    }
}

impl SnapshotStore for PageStoreSnapshots {
    fn capture(&mut self, at: &SnapshotPoint) -> Result<Snapshot, FerroError> {
        let dir = self.scratch.join(format!("capture-{}", at.last_round));
        // A stale directory from an interrupted capture would leave `take` writing a shorter image
        // over a longer one and the label describing the new one — a mixture of two databases.
        let _ = std::fs::remove_dir_all(&dir);

        let handle = crate::replication::backup::take(&self.pool, &self.wal, &dir)?;
        let label = handle.label;
        let image = std::fs::read(dir.join(crate::replication::backup::BASE_IMAGE))
            .map_err(|e| FerroError::Io(format!("read the captured page image: {e}")))?;

        let arena = self.arenas.state_bytes();
        let branches = self.branch_image();
        let root = self.trunk_root()?;

        let snap = Snapshot::build(
            at,
            root,
            self.catalog_page_id,
            label,
            &arena,
            &branches,
            image,
        )
        .map_err(SnapshotError::into_ferro)?;

        // The pin is released here on purpose: the payload is complete and self-contained, so
        // nothing further reads the primary's log through it, and holding it would keep the WAL
        // growing for as long as any snapshot object lived.
        drop(handle);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(snap)
    }

    fn install(&mut self, meta: &SnapshotMeta, path: &Path) -> Result<(), FerroError> {
        let mut f = std::fs::File::open(path)
            .map_err(|e| FerroError::Io(format!("open the spooled snapshot: {e}")))?;
        let header = read_header(&mut f).map_err(SnapshotError::into_ferro)?;
        if header.last_round != meta.last_round {
            return Err(FerroError::Corruption(format!(
                "the spooled snapshot is of round {} and the install was told round {}",
                header.last_round, meta.last_round
            )));
        }

        let dir = self.scratch.join(format!("install-{}", meta.last_round));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)
            .map_err(|e| FerroError::Io(format!("create the install scratch dir: {e}")))?;

        // Explode the payload back into the directory shape `backup::restore` reads, so the
        // restore — including its check that the image's length matches the page count its label
        // claims — is the same code path a local base-backup restore takes.
        let arena = read_section(&mut f, header.arena_len as u64)?;
        let branches = read_section(&mut f, header.branches_len as u64)?;
        stream_section(
            &mut f,
            header.image_len,
            &dir.join(crate::replication::backup::BASE_IMAGE),
        )?;
        let label = crate::replication::backup::BackupLabel {
            start_lsn: header.start_lsn,
            end_lsn: header.end_lsn,
            page_count: header.page_count,
        };
        std::fs::write(
            dir.join(crate::replication::backup::BACKUP_LABEL),
            label.encode().as_bytes(),
        )
        .map_err(|e| FerroError::Io(format!("write the install label: {e}")))?;

        // **Invalidate before replacing, and never flush.** Every frame this pool holds describes
        // a database that is about to stop existing, so writing one back would put a page of the
        // old database into the middle of the new one — and `examples/repl_primary.rs` names the
        // consequence exactly: "every such page still passes its checksum, so refusing here is the
        // only detection point." `invalidate_all` refuses whole if any frame is pinned, because a
        // live reader holding a frame index would otherwise be handed another database's bytes.
        self.pool.invalidate_all()?;

        crate::replication::backup::restore(&dir, &self.paths.page_file)?;
        std::fs::write(&self.paths.arena_image, &arena)
            .map_err(|e| FerroError::Io(format!("write the arena image: {e}")))?;
        std::fs::write(&self.paths.branch_catalog, &branches)
            .map_err(|e| FerroError::Io(format!("write the branch catalog: {e}")))?;

        // The live objects, not only the files. A node that replaced its durable state and went on
        // serving the old in-memory index would answer for a database it no longer has, and the
        // only sign of it would be a restart much later that "changed" the answers.
        self.arenas.load_state(&arena)?;
        self.branches.reload_from(&self.paths.branch_catalog, header.root_page_id)?;

        // Durable before returning. The state machine moves its floor on the strength of this call
        // having returned, so an install that is still in a page cache is an install that a power
        // loss turns into a node claiming a history it does not hold.
        for p in [&self.paths.page_file, &self.paths.arena_image, &self.paths.branch_catalog] {
            std::fs::File::open(p)
                .and_then(|h| h.sync_all())
                .map_err(|e| FerroError::Io(format!("fsync {}: {e}", p.display())))?;
        }
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}

fn read_header(f: &mut std::fs::File) -> Result<PayloadHeader, SnapshotError> {
    use std::io::Read;
    let mut buf = [0u8; PayloadHeader::BYTES];
    f.read_exact(&mut buf).map_err(|e| {
        SnapshotError::Refused(format!("the spooled snapshot is shorter than its own header: {e}"))
    })?;
    PayloadHeader::decode(&buf)
}

fn read_section(f: &mut std::fs::File, len: u64) -> Result<Vec<u8>, FerroError> {
    use std::io::Read;
    let mut v = vec![0u8; len as usize];
    f.read_exact(&mut v)
        .map_err(|e| FerroError::Io(format!("read a {len}-byte snapshot section: {e}")))?;
    Ok(v)
}

/// Copy `len` bytes from the spool into `dest` **without holding them**.
fn stream_section(f: &mut std::fs::File, len: u64, dest: &Path) -> Result<(), FerroError> {
    use std::io::{Read, Write};
    let mut out = std::fs::File::create(dest)
        .map_err(|e| FerroError::Io(format!("create {}: {e}", dest.display())))?;
    let mut left = len;
    let mut buf = vec![0u8; 1 << 16];
    while left > 0 {
        let want = (buf.len() as u64).min(left) as usize;
        f.read_exact(&mut buf[..want])
            .map_err(|e| FerroError::Io(format!("read the snapshot page image: {e}")))?;
        out.write_all(&buf[..want])
            .map_err(|e| FerroError::Io(format!("write {}: {e}", dest.display())))?;
        left -= want as u64;
    }
    out.sync_all().map_err(|e| FerroError::Io(format!("fsync {}: {e}", dest.display())))?;
    Ok(())
}
