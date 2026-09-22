//! F0b — the durable round log: term-stamped, contiguous, and recoverable after a checkpoint.
//!
//! **OWNER: agent F0b.** This is the store, not the protocol. See `DISTRIBUTED.md` §F0 for why a
//! round and not an LSN. Nothing here knows what a leader is; it knows how to hold a contiguous
//! sequence of [`Entry`] on a disk that can stop working half way through a write.
//!
//! # The framing is the WAL's framing
//!
//! `wal/log.rs` already survives a torn tail, and the way it does it is worth exactly one copy in
//! this codebase: a length-prefixed frame with a trailing CRC32, and a recovery scan that walks
//! frames from the start and stops at the first one that fails **any** of its checks. This module
//! reuses that shape and the same [`crc32`] implementation, and it adds one check the WAL's scan
//! has an analogue of: the WAL requires each frame's embedded LSN to equal the offset it was found
//! at, and this one requires each frame's embedded **round** to equal the round its position
//! implies. That check is what makes a hole visible by arithmetic — the whole reason §F0 chose a
//! round over a byte offset.
//!
//! # What it does differently, and why each difference is load-bearing
//!
//! **The header is checksummed, and there are two of them.** The WAL's 24-byte header carries no
//! checksum at all, which `storage::sim`'s own documentation names as a gap
//! (`a_garbled_wal_header_takes_the_database_down_with_its_data_intact`). That gap is affordable
//! there because a lost WAL costs a recovery; it is not affordable here, because this log holds the
//! only record of what a cluster agreed to. So the header has a CRC32 **and** lives in one of two
//! files that take turns being live, the live one being whichever holds the greatest generation
//! whose CRC validates.
//!
//! **A prefix can be discarded, which the WAL cannot do.** `WalManager::truncate` throws the log
//! away whole and restarts it, because nothing downstream needs the suffix. A checkpoint here must
//! keep the suffix: rounds above the snapshot are still the only copy of entries a follower may
//! still need. Discarding a prefix is therefore a **rewrite**, and a rewrite is the operation a
//! crash is most likely to catch half done. That is what the second file is for:
//!
//! 1. the survivors are written into the spare file and **fsynced**;
//! 2. only then is the spare's header written, carrying `generation + 1`, and fsynced.
//!
//! Until step 2's fsync completes the spare has no valid header and the old file is still live, so
//! a crash at any point leaves the *entire* pre-compaction log intact. After it, the new file is
//! live because its generation is greater. There is no window in which both are half-true, and no
//! rename, no temporary path, and no directory fsync — every byte of the switch goes through the
//! same [`Storage`] seam a fault can be aimed at, which is what lets `tests_log.rs` sweep a crash
//! across every single operation of a compaction and assert nothing was lost.
//!
//! **The second file is a switch, not a backup, and the difference is deliberate.** Once the switch
//! is durable the old file is emptied, so an unreadable live header is a hard refusal rather than a
//! quiet fall-back to the previous generation. Falling back would look like resilience and would be
//! data loss: rounds appended *since* the switch exist only in the live file, and a node that came
//! up on the older generation would have forgotten rounds it had already acknowledged. A refusal
//! costs an operator a restart from a peer's snapshot; a silent rewind costs the cluster a
//! committed write.
//!
//! # The floor, and the error that is the point of it
//!
//! After a checkpoint, `snapshot_round` is the floor: rounds at or below it are gone from the log
//! and exist only inside the snapshot. A read below the floor returns [`LogError::Compacted`],
//! carrying the floor, and **that is not a generic failure** — it is the signal that tells a leader
//! to send `InstallSnapshot` instead of entries. There is deliberately no `From<LogError> for
//! FerroError`: an automatic conversion would let a `?` erase the one distinction the caller has to
//! branch on, turning "this follower needs a snapshot" into "something went wrong" at a call site
//! nobody would look at twice. Crossing into [`FerroError`] is spelled [`LogError::into_ferro`], so
//! it is greppable and deliberate.
//!
//! # What this module also owns, for everyone else's benefit
//!
//! [`encode_command`]/[`decode_command`] and [`encode_entry`]/[`decode_entry`] are the **one**
//! encoding of a [`Command`]. `transport.rs` (F3) has to put entries on a wire and must reuse
//! these rather than write a second encoder: two encoders of one type are two chances to disagree
//! about a `Config`'s learners, and the disagreement would present as a follower that quietly
//! counts the wrong quorum.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::column::DataType;
use crate::error::FerroError;
use crate::storage::storage::Storage;
use crate::wal::log::{
    ColumnAlteration, DdlOp, crc32, pread_all, pwrite_all, short, take_str, take_u16, take_u32,
    take_u64, take_u8, write_str,
};

use super::config::Config;
use super::{BranchOp, Command, Entry, NodeId, Round, Term};

/// `0xF3EE_DB0*` is this codebase's file family; `03` is this log. `01` is the WAL and `02` is
/// unused, left alone rather than recycled so a mistyped path is refused instead of half-parsed.
const MAGIC: u32 = 0xF3EE_DB03;
const VERSION: u32 = 1;

/// `magic | version | generation | snapshot_round | snapshot_term | crc32(the previous 32 bytes)`.
const HEADER_SIZE: usize = 36;

/// `total_len | term | round | at least one payload byte | crc32`.
const MIN_FRAME: usize = 4 + 8 + 8 + 1 + 4;

/// The largest entry this log will store, **derived from what the transport can carry** rather than
/// picked.
///
/// An entry larger than one replication frame is an entry no follower can ever be sent, so a log
/// that accepted it would hold a round that can never reach a quorum — a write that is durable,
/// unreplicable, and undetectable until a follower falls behind. Refusing at `append` is the only
/// point where the caller still has somewhere to put the error. The headroom is for the `Append`
/// envelope around the entry (from, to, term, prev_round, prev_term, commit, and F7's MAC).
pub const MAX_ENTRY_BYTES: usize = crate::replication::MAX_FRAME_BYTES - 4096;

/// The largest frame a scan will read from disk. A corrupt length field is a request to allocate,
/// and an unbounded one is a denial of service triggered by four bad bytes.
const MAX_FRAME: usize = MAX_ENTRY_BYTES + 64;

/// Bytes buffered before a compaction flushes them to the spare file. Bounds the memory a rewrite
/// costs to something unrelated to how long the log is.
const COPY_BATCH: usize = 1 << 20;

/// Why the log refused.
///
/// Structured rather than a string because exactly one of these variants changes what the caller
/// does next, and matching on rendered text is how that distinction gets lost in a refactor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogError {
    /// **The round asked for is below the floor: send a snapshot, not entries.**
    ///
    /// The one variant callers must branch on. `floor` is `snapshot_round` — the last round the
    /// snapshot covers — so a leader knows both that it must send `InstallSnapshot` and what the
    /// receiver will hold once it has.
    Compacted { asked: Round, floor: Round },
    /// The round is above the end of the log, or is round 0, which is not a round: `Round` counts
    /// from 1 and 0 means "before the log begins".
    NotFound { asked: Round, first: Round, last: Round },
    /// An append would leave a hole. Rounds are contiguous, and this is the check that keeps them
    /// so — the property every other part of §F0 is built on.
    NonContiguous { expected: Round, got: Round },
    /// An entry's term is lower than the term of the entry before it. Terms never decrease along a
    /// log; one that does is a stale frame that survived where it should not have.
    TermWentBackwards { last: Term, got: Term },
    /// A caller's claim about the term at a round disagrees with what the log holds.
    TermMismatch { round: Round, held: Term, claimed: Term },
    /// An entry too large for the transport to ever carry. See [`MAX_ENTRY_BYTES`].
    TooLarge { bytes: usize, limit: usize },
    /// A value the encoding's length fields cannot express.
    ///
    /// Its own variant rather than a flavour of [`LogError::TooLarge`], because they are refused
    /// for opposite reasons: `TooLarge` is a policy — the transport's frame size — while this is
    /// arithmetic. `write_str` is `wal::log`'s and writes `s.len() as u16`, so a 65536-byte table
    /// name would be written with a **length prefix of zero** and the frame, complete with a valid
    /// CRC over exactly the bytes intended, would be durable and permanently undecodable. Silent
    /// truncation on the way in is the worst shape a bug can have here: nothing downstream can tell
    /// the difference, because everything downstream checks the bytes against a checksum of
    /// themselves.
    Unrepresentable { what: &'static str, len: usize, limit: usize },
    /// Bytes on disk are not what was written, or are a shape this build cannot read.
    Corrupt(String),
    /// The underlying storage refused.
    Io(String),
    /// A durability operation failed in a way that leaves this handle unable to promise anything.
    /// Every mutating call refuses from then on; the remedy is to reopen, which re-derives the
    /// truth from the bytes that survived.
    Poisoned(String),
}

impl LogError {
    /// The floor, if this refusal is the "send a snapshot" one. Written as a named accessor so a
    /// caller can express the branch without matching the variant inline at six call sites.
    pub fn needs_snapshot(&self) -> Option<Round> {
        match self {
            LogError::Compacted { floor, .. } => Some(*floor),
            _ => None,
        }
    }

    /// Cross into the crate's error type **after** deciding what to do about
    /// [`LogError::Compacted`].
    ///
    /// Deliberately a named method and not a `From` impl: `FerroError` has no variant that
    /// preserves "this peer needs a snapshot", so the conversion is lossy in exactly the place it
    /// matters, and a `?` that performed it silently would be a correctness bug wearing the
    /// clothes of an idiom.
    pub fn into_ferro(self) -> FerroError {
        match self {
            LogError::Corrupt(s) => FerroError::Corruption(format!("round log: {s}")),
            other => FerroError::Wal(format!("round log: {other}")),
        }
    }
}

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::Compacted { asked, floor } => write!(
                f,
                "round {asked} is at or below the snapshot floor {floor}; it is no longer in the \
                 log and the peer needs a snapshot rather than entries"
            ),
            LogError::NotFound { asked, first, last } => {
                write!(f, "round {asked} is not in the log, which holds {first}..={last}")
            }
            LogError::NonContiguous { expected, got } => {
                write!(f, "an append of round {got} would leave a hole; the next round is {expected}")
            }
            LogError::TermWentBackwards { last, got } => {
                write!(f, "term {got} follows term {last}; terms never decrease along a log")
            }
            LogError::TermMismatch { round, held, claimed } => write!(
                f,
                "round {round} was written in term {held}, but the caller claims term {claimed}"
            ),
            LogError::TooLarge { bytes, limit } => write!(
                f,
                "an entry of {bytes} bytes is over the {limit}-byte limit, so no follower could \
                 ever be sent it"
            ),
            LogError::Unrepresentable { what, len, limit } => write!(
                f,
                "a {what} of {len} does not fit this encoding's {limit}-wide length field, and \
                 writing it would truncate the length rather than the value"
            ),
            LogError::Corrupt(s) => write!(f, "DATA CORRUPTION: {s}"),
            LogError::Io(s) => write!(f, "io error: {s}"),
            LogError::Poisoned(s) => write!(f, "the log is poisoned and refuses to mutate: {s}"),
        }
    }
}

impl std::error::Error for LogError {}

/// **Only the decoders convert this way.** The bounds-checked readers in `wal::log`
/// (`take_u64`, `take_str`, ...) speak [`FerroError`] and the only failure they can produce is a
/// record that ends mid-field, which is corruption.
///
/// `pread_all`/`pwrite_all` also speak [`FerroError`] and are **not** covered by this: they wrap an
/// operating-system error, and a failing device reported as `DATA CORRUPTION` sends an operator to
/// look for a bad checksum that does not exist. Those two go through [`read_at`]/[`write_at`]
/// instead, which classify as [`LogError::Io`]. That is why every call in this file names one of
/// those helpers rather than using `?` on the free function.
impl From<FerroError> for LogError {
    fn from(e: FerroError) -> Self {
        LogError::Corrupt(e.to_string())
    }
}

fn io<E: std::fmt::Display>(e: E) -> LogError {
    LogError::Io(e.to_string())
}

/// A positional read of log bytes, classified as I/O rather than as corruption.
fn read_at(file: &dyn Storage, buf: &mut [u8], offset: u64) -> Result<(), LogError> {
    pread_all(file, buf, offset).map_err(io)
}

/// A positional write of log bytes, classified as I/O rather than as corruption.
fn write_at(file: &dyn Storage, bytes: &[u8], offset: u64) -> Result<(), LogError> {
    pwrite_all(file, bytes, offset).map_err(io)
}

// ---------------------------------------------------------------------------------------------
// The header
// ---------------------------------------------------------------------------------------------

/// What a log file claims about itself. One per file; the live file is the one whose `generation`
/// is greatest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Header {
    generation: u64,
    snapshot_round: Round,
    snapshot_term: Term,
}

impl Header {
    fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut h = [0u8; HEADER_SIZE];
        h[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        h[4..8].copy_from_slice(&VERSION.to_be_bytes());
        h[8..16].copy_from_slice(&self.generation.to_be_bytes());
        h[16..24].copy_from_slice(&self.snapshot_round.to_be_bytes());
        h[24..32].copy_from_slice(&self.snapshot_term.to_be_bytes());
        let crc = crc32(&h[0..32]);
        h[32..36].copy_from_slice(&crc.to_be_bytes());
        h
    }

    /// `Ok(None)` means "this file does not carry a header I can trust" — it was never initialized,
    /// or the header write was caught by a crash. That is a normal state and not an error: it is
    /// what the *other* file is for.
    ///
    /// A header whose CRC validates but whose version this build does not know is an **error**, not
    /// a `None`. Treating it as absent would let a downgrade reinitialize a log written by a newer
    /// build, which is data loss performed by the recovery path.
    fn decode(bytes: &[u8; HEADER_SIZE]) -> Result<Option<Header>, LogError> {
        let stored = u32::from_be_bytes(bytes[32..36].try_into().unwrap());
        if crc32(&bytes[0..32]) != stored {
            return Ok(None);
        }
        if u32::from_be_bytes(bytes[0..4].try_into().unwrap()) != MAGIC {
            return Ok(None);
        }
        let version = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(LogError::Corrupt(format!(
                "a round log written at format version {version}; this build reads version {VERSION}"
            )));
        }
        Ok(Some(Header {
            generation: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            snapshot_round: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
            snapshot_term: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
        }))
    }
}

// ---------------------------------------------------------------------------------------------
// The log
// ---------------------------------------------------------------------------------------------

/// One frame's place in the live file. Rebuilt by the recovery scan and maintained in step with
/// every append and truncation, so a read is one `pread` rather than a walk.
///
/// The round is **implicit in the position**: `index[i]` describes round `snapshot_round + 1 + i`.
/// Storing it would be storing a fact the vector's shape already asserts, and the two could then
/// disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Frame {
    offset: u64,
    len: u32,
    term: Term,
}

/// A durable, contiguous, term-stamped log of [`Entry`], recoverable across a crash and across a
/// checkpoint that discards a prefix.
///
/// Reads take `&self`; everything that changes the log takes `&mut self`. That is not decoration —
/// the consensus state machine never touches this type at all (it returns `Action::Persist` and the
/// caller performs it), so the only writer is the one thread fulfilling those actions.
pub struct RoundLog {
    /// The two files that take turns being live. Indexed by `live`.
    files: [Arc<dyn Storage>; 2],
    live: usize,
    generation: u64,

    snapshot_round: Round,
    snapshot_term: Term,

    index: Vec<Frame>,
    /// Offset just past the last frame in the live file. The four bytes at this offset are the
    /// zero terminator; see [`RoundLog::write_batch`].
    end_offset: u64,
    /// The highest round this log has **promised** is durable — advanced only by
    /// [`RoundLog::sync`], never by a write that merely returned `Ok`.
    durable_round: Round,

    poisoned: Option<String>,
}

/// Hand-written rather than derived, because the two `Arc<dyn Storage>` cannot be derived over and
/// because the interesting state is six numbers: which file is live, at what generation, where the
/// floor is, and how far the log reaches in memory versus on the device.
impl std::fmt::Debug for RoundLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RoundLog {{ live: {}, generation: {}, floor: {}@{}, rounds: {}..={}, durable: {}{} }}",
            if self.live == 0 { "a" } else { "b" },
            self.generation,
            self.snapshot_round,
            self.snapshot_term,
            self.first_round(),
            self.last_round(),
            self.durable_round,
            match &self.poisoned {
                Some(why) => format!(", POISONED: {why}"),
                None => String::new(),
            }
        )
    }
}

impl RoundLog {
    /// Open the log at `<base>.a` / `<base>.b`, recovering whatever survived. The production entry
    /// point.
    pub fn open(base: &Path) -> Result<RoundLog, LogError> {
        let a = Self::open_file(&Self::side_path(base, 'a'))?;
        let b = Self::open_file(&Self::side_path(base, 'b'))?;
        Self::with_storage(a, b)
    }

    fn side_path(base: &Path, side: char) -> PathBuf {
        let mut name = base.file_name().map(|n| n.to_os_string()).unwrap_or_default();
        name.push(format!(".{side}"));
        base.with_file_name(name)
    }

    fn open_file(path: &Path) -> Result<Arc<dyn Storage>, LogError> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(io)?;
        Ok(Arc::new(f))
    }

    /// Open the log on any two [`Storage`]s. This is the seam a fault is aimed through: hand it two
    /// handles from a `SimFabric` and every byte of every durability decision becomes injectable.
    pub fn with_storage(a: Arc<dyn Storage>, b: Arc<dyn Storage>) -> Result<RoundLog, LogError> {
        let files = [a, b];
        let mut headers: [Option<Header>; 2] = [None, None];
        let mut lens = [0u64; 2];
        for i in 0..2 {
            lens[i] = files[i].len().map_err(io)?;
            if lens[i] >= HEADER_SIZE as u64 {
                let mut buf = [0u8; HEADER_SIZE];
                read_at(&*files[i], &mut buf, 0)?;
                headers[i] = Header::decode(&buf)?;
            }
        }

        let live = match (headers[0], headers[1]) {
            (None, None) => {
                // Nothing readable. Initializing is only safe if there is nothing to lose, and the
                // boundary is exact: **frames begin at `HEADER_SIZE`**, so a file no longer than
                // the header cannot contain a byte of any entry. Anything longer might, and the one
                // thing recovery must not do is quietly reinitialize over entries a cluster agreed
                // to — which is what a garbled header on a full log would otherwise produce, with
                // no error anywhere.
                //
                // The boundary is `>` and not `> 0` for a reason a stricter test would get wrong:
                // the very first thing a brand-new log does is write this header, and a crash that
                // tears that write leaves a few bytes and no entries. Refusing there would brick a
                // log at first start over a file that has never held anything.
                if lens[0] > HEADER_SIZE as u64 || lens[1] > HEADER_SIZE as u64 {
                    return Err(LogError::Corrupt(format!(
                        "neither round log file carries a readable header, but they hold {} and {} \
                         bytes, which is room for entries; refusing to reinitialize over them",
                        lens[0], lens[1]
                    )));
                }
                let h = Header { generation: 1, snapshot_round: 0, snapshot_term: 0 };
                // No truncate first, and that is provable rather than hopeful: this arm is only
                // reached when both files are at most `HEADER_SIZE` bytes, and the write below is
                // exactly `HEADER_SIZE` bytes at offset 0, so every byte of whatever a torn earlier
                // attempt left is overwritten. A `set_len` here would be a line no test could ever
                // make matter.
                //
                // Written and fsynced before a single entry may be appended. A header that is not
                // durable when the entries above it are is a log that reopens as empty.
                write_at(&*files[0], &h.encode(), 0)?;
                files[0].sync_all().map_err(io)?;
                headers[0] = Some(h);
                0
            }
            (Some(_), None) => 0,
            (None, Some(_)) => 1,
            (Some(x), Some(y)) => {
                if x.generation == y.generation {
                    // Impossible from this code — a generation is claimed by exactly one file — so
                    // it means a file was copied over another, and there is no principled way to
                    // choose. Refusing beats picking one and being confidently wrong about which
                    // rounds the cluster agreed to.
                    return Err(LogError::Corrupt(format!(
                        "both round log files claim generation {}; one was copied over the other \
                         and there is no way to tell which holds the cluster's log",
                        x.generation
                    )));
                }
                if x.generation > y.generation { 0 } else { 1 }
            }
        };

        let h = headers[live].expect("the live file has a header by construction");
        let scan = scan_frames(&*files[live], h.snapshot_round, h.snapshot_term, lens[live])?;

        // Trim whatever the scan refused to trust, exactly as `WalManager::with_storage` does. The
        // frames are already unreachable — the scan stops at them — so this reclaims space and
        // keeps the file's length equal to the log's length, which every later append relies on.
        if scan.end_offset < lens[live] {
            files[live].set_len(scan.end_offset).map_err(io)?;
            files[live].sync_all().map_err(io)?;
        }

        let durable_round = h.snapshot_round + scan.frames.len() as u64;
        Ok(RoundLog {
            files,
            live,
            generation: h.generation,
            snapshot_round: h.snapshot_round,
            snapshot_term: h.snapshot_term,
            index: scan.frames,
            end_offset: scan.end_offset,
            durable_round,
            poisoned: None,
        })
    }

    // -- what the log holds -------------------------------------------------------------------

    /// The last round covered by the snapshot; the log begins at `snapshot_round + 1`.
    pub fn snapshot_round(&self) -> Round {
        self.snapshot_round
    }

    /// The term of [`RoundLog::snapshot_round`], needed for the log-matching check on the first
    /// entry above the floor.
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    /// The first round the log can serve. `snapshot_round + 1` — which is 1 on a log that has never
    /// been checkpointed, because rounds are contiguous from 1.
    pub fn first_round(&self) -> Round {
        self.snapshot_round + 1
    }

    /// The highest round held. Equal to [`RoundLog::snapshot_round`] when the log is empty.
    pub fn last_round(&self) -> Round {
        self.snapshot_round + self.index.len() as u64
    }

    /// The term of [`RoundLog::last_round`] — the candidate half of the §5.4.1 election
    /// restriction.
    pub fn last_term(&self) -> Term {
        self.index.last().map_or(self.snapshot_term, |f| f.term)
    }

    /// The highest round this log has **promised** is on the device.
    ///
    /// Distinct from [`RoundLog::last_round`] and the distinction is the whole of "fsync before
    /// ack": an entry that has been written but not synced is one a correlated power loss takes
    /// away, and a follower that acked it turned that loss into acknowledged data loss.
    pub fn durable_round(&self) -> Round {
        self.durable_round
    }

    /// How many entries are held above the floor.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Which of the two files is live, and at what generation. For tests and for an operator
    /// staring at two files wondering which one matters.
    pub fn live_generation(&self) -> u64 {
        self.generation
    }

    /// The term an entry was written in.
    ///
    /// `round == 0` answers 0 and `round == snapshot_round` answers the snapshot's term, because
    /// both are legitimate `prev_round` values in an `Append` and neither is in the log.
    pub fn term_at(&self, round: Round) -> Result<Term, LogError> {
        if round == 0 {
            return Ok(0);
        }
        if round == self.snapshot_round {
            return Ok(self.snapshot_term);
        }
        Ok(self.frame(round)?.term)
    }

    /// One entry.
    pub fn entry(&self, round: Round) -> Result<Entry, LogError> {
        let f = *self.frame(round)?;
        let bytes = self.read_frame(&f)?;
        decode_frame(&bytes, round, f.term)
    }

    /// Entries from `from` onwards, bounded by both a count and a byte budget.
    ///
    /// **At least one entry is always returned when one exists**, even if it alone blows the byte
    /// budget. A budget that can return nothing is a leader that stops making progress the moment
    /// one large entry reaches the head of the log, which presents as a stall with no error.
    ///
    /// `from == last_round + 1` is not an error: it is the ordinary steady state of a follower that
    /// is caught up, and it returns an empty vector.
    pub fn range(
        &self,
        from: Round,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<Entry>, LogError> {
        self.check_readable(from)?;
        if from > self.last_round() {
            if from == self.last_round() + 1 {
                return Ok(Vec::new());
            }
            return Err(LogError::NotFound {
                asked: from,
                first: self.first_round(),
                last: self.last_round(),
            });
        }
        if max_entries == 0 {
            return Ok(Vec::new());
        }
        let start = (from - self.first_round()) as usize;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for (i, f) in self.index[start..].iter().enumerate() {
            if !out.is_empty() && (out.len() >= max_entries || bytes + f.len as usize > max_bytes) {
                break;
            }
            let raw = self.read_frame(f)?;
            out.push(decode_frame(&raw, from + i as u64, f.term)?);
            bytes += f.len as usize;
        }
        Ok(out)
    }

    // -- changing the log ---------------------------------------------------------------------

    /// Append entries, which must continue the log exactly.
    ///
    /// The **whole batch is validated before a byte is written**, so a refusal leaves the log
    /// exactly as it was. Validating as it goes would leave a prefix of a rejected batch durable,
    /// which is a hole created by the check meant to prevent one.
    ///
    /// This does not make anything durable. [`RoundLog::sync`] does, and until it has,
    /// [`RoundLog::durable_round`] does not move.
    pub fn append(&mut self, entries: &[Entry]) -> Result<(), LogError> {
        self.check_live()?;
        if entries.is_empty() {
            return Ok(());
        }

        let mut expect = self.last_round() + 1;
        let mut prev_term = self.last_term();
        let mut frames: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        for e in entries {
            if e.round != expect {
                return Err(LogError::NonContiguous { expected: expect, got: e.round });
            }
            if e.term < prev_term {
                return Err(LogError::TermWentBackwards { last: prev_term, got: e.term });
            }
            let frame = encode_frame(e)?;
            frames.push(frame);
            expect += 1;
            prev_term = e.term;
        }

        let mut buf = Vec::with_capacity(frames.iter().map(|f| f.len()).sum::<usize>() + 4);
        let mut placed = Vec::with_capacity(frames.len());
        let mut at = self.end_offset;
        for (f, e) in frames.iter().zip(entries) {
            placed.push(Frame { offset: at, len: f.len() as u32, term: e.term });
            at += f.len() as u64;
            buf.extend_from_slice(f);
        }
        self.write_batch(&buf, self.end_offset)?;

        self.index.extend(placed);
        self.end_offset = at;
        Ok(())
    }

    /// Make everything appended so far durable and report how far that reaches.
    ///
    /// `sync_data` and not `sync_all`: the only metadata that matters is the file's length, which
    /// `fdatasync` is required to flush because the data cannot be read back without it. The WAL's
    /// `flush` makes the same call for the same reason.
    ///
    /// **A failed fsync poisons.** Not caution: on Linux a writeback error is reported to *one*
    /// fsync and then cleared, and the pages are marked clean, so the obvious recovery — call
    /// `sync()` again — returns `Ok` about bytes that never reached the device and would move
    /// `durable_round` over rounds the failure dropped. There is no way to retry durability from
    /// here, so the handle refuses and a reopen re-derives the frontier from what actually survived.
    pub fn sync(&mut self) -> Result<Round, LogError> {
        self.check_live()?;
        if let Err(e) = self.files[self.live].sync_data() {
            let why = format!(
                "an fsync through round {} failed, and a second fsync cannot be trusted to report \
                 the same failure twice: {e}",
                self.last_round()
            );
            self.poisoned = Some(why.clone());
            return Err(LogError::Poisoned(why));
        }
        self.durable_round = self.last_round();
        Ok(self.durable_round)
    }

    /// Remove `from` and everything above it — the conflicting suffix a follower drops when its
    /// `(prev_round, prev_term)` did not match.
    ///
    /// `from == last_round + 1` is a no-op rather than an error, because a follower asked to
    /// truncate at a point it has not reached is the ordinary case. `from > last_round + 1` **is**
    /// an error: it is a caller working from a log it does not have, and succeeding silently would
    /// hide that.
    ///
    /// The in-memory view shrinks **before** the syscall. If `set_len` or its fsync fails, this
    /// handle is left believing it holds *less* than the file does, which is the safe direction —
    /// the log will never claim a round it might not have — and the handle is poisoned so nothing
    /// appends over a tail whose removal was not made durable.
    pub fn truncate_from(&mut self, from: Round) -> Result<(), LogError> {
        self.check_live()?;
        self.check_readable(from)?;
        let last = self.last_round();
        if from > last {
            if from == last + 1 {
                return Ok(());
            }
            return Err(LogError::NotFound { asked: from, first: self.first_round(), last });
        }

        let idx = (from - self.first_round()) as usize;
        let new_end = self.index[idx].offset;
        self.index.truncate(idx);
        self.end_offset = new_end;
        self.durable_round = self.durable_round.min(from - 1);

        let file = Arc::clone(&self.files[self.live]);
        if let Err(e) = file.set_len(new_end).and_then(|()| file.sync_all()) {
            // POISON, never a plain `Io`. The in-memory index has already been shortened above, so
            // if the `set_len`/`sync_all` pair did not reach the device the file may still be long
            // while this handle believes it is short. A caller that saw a recoverable-looking
            // `Io` and appended would write its new tail at `new_end`, over bytes whose removal
            // was never made durable — and a crash there leaves a log whose recovery cannot tell
            // the replacement suffix from the one it replaced.
            let why = format!("a truncation to round {} could not be made durable: {e}", from - 1);
            self.poisoned = Some(why.clone());
            return Err(LogError::Poisoned(why));
        }
        Ok(())
    }

    /// **The checkpoint.** Everything through `through` is now covered by a snapshot; discard it and
    /// keep the rest.
    ///
    /// `term` is the term of `through`, and when `through` is inside the log it is **checked
    /// against what the log holds**. A caller whose snapshot disagrees with the log about the term
    /// at its own last round has a snapshot of a different history, and installing it would leave
    /// the floor asserting a `(round, term)` pair the log never contained — which is precisely the
    /// pair every later log-matching check is decided by.
    ///
    /// `through` above the end of the log is the `InstallSnapshot` case: the log is emptied and the
    /// floor jumps to a round this node never held.
    ///
    /// Nothing is lost if this is interrupted. See the module header for the two-file switch.
    pub fn discard_prefix(&mut self, through: Round, term: Term) -> Result<(), LogError> {
        self.check_live()?;
        if through < self.snapshot_round {
            return Ok(());
        }
        if through == self.snapshot_round {
            if term != self.snapshot_term {
                return Err(LogError::TermMismatch {
                    round: through,
                    held: self.snapshot_term,
                    claimed: term,
                });
            }
            return Ok(());
        }
        if through <= self.last_round() {
            let held = self.frame(through)?.term;
            if held != term {
                return Err(LogError::TermMismatch { round: through, held, claimed: term });
            }
        }

        let survivors: Vec<Frame> = if through < self.last_round() {
            let start = (through + 1 - self.first_round()) as usize;
            self.index[start..].to_vec()
        } else {
            Vec::new()
        };

        let spare = 1 - self.live;
        let dst = Arc::clone(&self.files[spare]);

        // 1. Reclaim the spare. Its previous contents are the log as it was two compactions ago and
        //    nothing reads them: the live file's generation is greater. Reclaiming here rather than
        //    after the switch means there is no post-switch step whose failure would have to be
        //    either swallowed or reported as a failure of a compaction that in fact succeeded.
        dst.set_len(0).map_err(io)?;

        // 2. The survivors, then a zero terminator, then an fsync. Data before metadata: the header
        //    written in step 3 is the thing that makes this file live, so every byte it will
        //    describe has to be on the device before it exists.
        let mut at = HEADER_SIZE as u64;
        let mut batch: Vec<u8> = Vec::with_capacity(COPY_BATCH.min(1 << 16));
        let mut moved = Vec::with_capacity(survivors.len());
        for f in &survivors {
            let raw = self.read_frame(f)?;
            moved.push(Frame { offset: at, len: f.len, term: f.term });
            at += raw.len() as u64;
            batch.extend_from_slice(&raw);
            if batch.len() >= COPY_BATCH {
                write_at(&*dst, &batch, at - batch.len() as u64)?;
                batch.clear();
            }
        }
        batch.extend_from_slice(&[0u8; 4]);
        write_at(&*dst, &batch, at + 4 - batch.len() as u64)?;
        dst.sync_data().map_err(io)?;

        // 3. The header, and **the point of no return**. Until its fsync returns, the old file is
        //    still the live one and the whole pre-compaction log is intact. From the moment the
        //    write is issued, whether the switch has happened is no longer something this handle can
        //    know: a `pwrite` that returned `Ok` reaches the device even if the `fsync` behind it
        //    reports an error, and on Linux an fsync error is reported once and then cleared, so the
        //    *next* fsync says `Ok` about bytes that never landed.
        //
        //    So a failure here **poisons**. Returning a plain error and carrying on is the shape
        //    that loses acknowledged data: the caller keeps the handle, appends rounds 11..20 into
        //    what it believes is the live file, `sync()` returns `Ok(20)` and the node acks them —
        //    and then a restart finds the *other* file live at the greater generation and comes up
        //    holding neither. Refusing everything from here and letting a reopen re-derive the
        //    truth from the bytes is the only answer that cannot lose a round.
        let h = Header {
            generation: self.generation + 1,
            snapshot_round: through,
            snapshot_term: term,
        };
        if let Err(e) = write_at(&*dst, &h.encode(), 0).and_then(|()| dst.sync_all().map_err(io))
        {
            let why = format!(
                "a checkpoint at round {through} could not be made durable, so whether the \
                 generation-{} header is live is no longer knowable from this handle: {e}",
                h.generation
            );
            self.poisoned = Some(why.clone());
            return Err(LogError::Poisoned(why));
        }

        let stale = self.live;
        self.live = spare;
        self.generation = h.generation;
        self.snapshot_round = through;
        self.snapshot_term = term;
        self.index = moved;
        self.end_offset = at;
        self.durable_round = self.last_round();

        // 4. **Retire the old file, durably.** Not hygiene: while it still carries a valid header it
        //    is still a candidate to be chosen as live, and the module header's promise that an
        //    unreadable live header is a hard refusal rather than a quiet rewind rests on it not
        //    being one. If the newer file's header is later damaged, an old file left intact is
        //    exactly the "resilient" fall-back that silently forgets every round appended since the
        //    switch. `set_len(0)` alone is not enough — under a device that only makes writes
        //    durable at an fsync, an unsynced truncate leaves the previous generation whole on the
        //    platter.
        //
        //    A failure poisons rather than being dropped. The switch itself already succeeded, so
        //    this is not a success reported as a failure: it is a refusal to keep operating in a
        //    state whose failure mode is silent. A reopen is unaffected — the new generation is the
        //    greater one and wins.
        let retire = self.files[stale]
            .set_len(0)
            .and_then(|()| self.files[stale].sync_all());
        if let Err(e) = retire {
            let why = format!(
                "the checkpoint at round {through} is durable, but the superseded file could not be \
                 retired, so it is still a candidate the recovery could fall back to: {e}"
            );
            self.poisoned = Some(why.clone());
            return Err(LogError::Poisoned(why));
        }
        Ok(())
    }

    // -- internals ----------------------------------------------------------------------------

    fn check_live(&self) -> Result<(), LogError> {
        match &self.poisoned {
            Some(why) => Err(LogError::Poisoned(why.clone())),
            None => Ok(()),
        }
    }

    /// The floor check, in one place. Every read and every truncation goes through it, so a new
    /// entry point cannot be added that forgets to distinguish "below the snapshot" from "not
    /// there".
    fn check_readable(&self, round: Round) -> Result<(), LogError> {
        if round == 0 {
            return Err(LogError::NotFound {
                asked: 0,
                first: self.first_round(),
                last: self.last_round(),
            });
        }
        if round <= self.snapshot_round {
            return Err(LogError::Compacted { asked: round, floor: self.snapshot_round });
        }
        Ok(())
    }

    fn frame(&self, round: Round) -> Result<&Frame, LogError> {
        self.check_readable(round)?;
        let last = self.last_round();
        if round > last {
            return Err(LogError::NotFound { asked: round, first: self.first_round(), last });
        }
        Ok(&self.index[(round - self.first_round()) as usize])
    }

    fn read_frame(&self, f: &Frame) -> Result<Vec<u8>, LogError> {
        let mut buf = vec![0u8; f.len as usize];
        read_at(&*self.files[self.live], &mut buf, f.offset)?;
        Ok(buf)
    }

    /// Write `bytes` at `offset`, followed by a **zero terminator**.
    ///
    /// The terminator is four bytes of zero, which is below `MIN_FRAME` and therefore stops
    /// [`scan_frames`] dead. It exists for one narrow case with an expensive outcome: bytes left
    /// over from a longer previous life of this file, sitting immediately after the last frame,
    /// which happened to line up on a frame boundary with the right round and a valid CRC. A scan
    /// would walk into them and resurrect entries from a log that was truncated away. It costs four
    /// bytes per batch, always overwritten by the next one.
    fn write_batch(&mut self, bytes: &[u8], offset: u64) -> Result<(), LogError> {
        let mut buf = Vec::with_capacity(bytes.len() + 4);
        buf.extend_from_slice(bytes);
        buf.extend_from_slice(&[0u8; 4]);
        write_at(&*self.files[self.live], &buf, offset)?;
        Ok(())
    }
}

/// What a recovery scan concluded.
struct Scan {
    frames: Vec<Frame>,
    /// Offset just past the last frame the scan was willing to trust.
    end_offset: u64,
}

/// Walk frames from the start of the data region and stop at the first one that fails any check.
///
/// The checks, and what each is the only defence against:
///
/// * **length in range** — a corrupt length field is a request to allocate, and one that runs past
///   the file is a read of bytes that are not there;
/// * **CRC32 over the frame** — the only thing that catches a write that landed at full length with
///   the wrong bytes. `storage::sim` records the measurement: with only torn writes modelled,
///   removing every CRC check from the WAL left its sweep green, because the length bound caught
///   the short tail on its own;
/// * **the embedded round equals the round this position implies** — the analogue of the WAL's
///   embedded-LSN check, and what makes a hole arithmetic rather than a reconciliation;
/// * **the term does not decrease** — terms are monotonic along a log, so a frame that lowers one
///   is a survivor of an earlier life of these bytes rather than a continuation of this one.
fn scan_frames(
    file: &dyn Storage,
    snapshot_round: Round,
    snapshot_term: Term,
    file_len: u64,
) -> Result<Scan, LogError> {
    let mut frames = Vec::new();
    let mut offset = HEADER_SIZE as u64;
    let mut expected_round = snapshot_round + 1;
    let mut prev_term = snapshot_term;

    loop {
        if offset + 4 > file_len {
            break;
        }
        let mut len_buf = [0u8; 4];
        read_at(file, &mut len_buf, offset)?;
        let total = u32::from_be_bytes(len_buf) as usize;
        if total < MIN_FRAME || total > MAX_FRAME || offset + total as u64 > file_len {
            break;
        }
        let mut frame = vec![0u8; total];
        read_at(file, &mut frame, offset)?;
        let stored = u32::from_be_bytes(frame[total - 4..].try_into().unwrap());
        if crc32(&frame[..total - 4]) != stored {
            break;
        }
        let term = u64::from_be_bytes(frame[4..12].try_into().unwrap());
        let round = u64::from_be_bytes(frame[12..20].try_into().unwrap());
        if round != expected_round || term < prev_term {
            break;
        }
        frames.push(Frame { offset, len: total as u32, term });
        prev_term = term;
        expected_round += 1;
        offset += total as u64;
    }

    Ok(Scan { frames, end_offset: offset })
}

// ---------------------------------------------------------------------------------------------
// The frame, and the encoding of a Command
// ---------------------------------------------------------------------------------------------

/// Every length field in this encoding is narrower than a `usize`, and a cast that silently wraps
/// is how a value becomes undecodable *after* it is durable. One helper per width, called at every
/// site that would otherwise write `len as u16` / `len as u32`.
fn fits_u16(len: usize, what: &'static str) -> Result<u16, LogError> {
    u16::try_from(len)
        .map_err(|_| LogError::Unrepresentable { what, len, limit: u16::MAX as usize })
}

fn fits_u32(len: usize, what: &'static str) -> Result<u32, LogError> {
    u32::try_from(len)
        .map_err(|_| LogError::Unrepresentable { what, len, limit: u32::MAX as usize })
}

/// `wal::log::write_str`, with this module's error type on the refusal.
///
/// ⛔ **THIS NO LONGER CHECKS THE LENGTH, AND MUST NOT.** It used to call `fits_u16` first,
/// because `write_str` wrote `s.len() as u16` unchecked; D154 moved the guard into `write_str`,
/// which is the only place it can be the authority for all three formats built on it. A check
/// here in front of that one would mask every mutant of it. What remains is the `LogError`
/// translation, so a consensus caller still matches on `LogError::Unrepresentable` — and
/// `tests_log.rs` asserts exactly that, including the `what` label.
fn put_str(out: &mut Vec<u8>, s: &str, what: &'static str) -> Result<(), LogError> {
    write_str(out, s, what).map_err(|_| LogError::Unrepresentable {
        what,
        len: s.len(),
        limit: u16::MAX as usize,
    })
}

/// `total_len | term | round | payload | crc32`, the WAL's shape with a round where its LSN goes.
fn encode_frame(e: &Entry) -> Result<Vec<u8>, LogError> {
    let mut payload = Vec::new();
    encode_command(&e.command, &mut payload)?;
    let total = 4 + 8 + 8 + payload.len() + 4;
    if total > MAX_ENTRY_BYTES {
        return Err(LogError::TooLarge { bytes: total, limit: MAX_ENTRY_BYTES });
    }
    let mut frame = Vec::with_capacity(total);
    frame.extend_from_slice(&(total as u32).to_be_bytes());
    frame.extend_from_slice(&e.term.to_be_bytes());
    frame.extend_from_slice(&e.round.to_be_bytes());
    frame.extend_from_slice(&payload);
    let crc = crc32(&frame);
    frame.extend_from_slice(&crc.to_be_bytes());
    Ok(frame)
}

/// Decode a frame the scan already validated, cross-checking the `(round, term)` the index believes
/// it holds.
///
/// The cross-check is not redundant with the scan. The scan runs once at open; the index is carried
/// across every append, truncation and compaction after that, and an index that drifts from the
/// file would serve the *wrong entry* under a correct-looking round — the one failure mode that a
/// checksum cannot see, because the bytes are exactly what was written.
fn decode_frame(frame: &[u8], round: Round, term: Term) -> Result<Entry, LogError> {
    if frame.len() < MIN_FRAME {
        return Err(LogError::Corrupt(format!("a frame of {} bytes is too short", frame.len())));
    }
    let total = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
    if total != frame.len() {
        return Err(LogError::Corrupt(format!(
            "a frame claims {total} bytes but {} were read",
            frame.len()
        )));
    }
    let stored = u32::from_be_bytes(frame[total - 4..].try_into().unwrap());
    if crc32(&frame[..total - 4]) != stored {
        return Err(LogError::Corrupt(format!("the frame at round {round} fails its crc32")));
    }
    let got_term = u64::from_be_bytes(frame[4..12].try_into().unwrap());
    let got_round = u64::from_be_bytes(frame[12..20].try_into().unwrap());
    if got_round != round || got_term != term {
        return Err(LogError::Corrupt(format!(
            "the index says round {round} term {term} lives here, but the frame says round \
             {got_round} term {got_term}"
        )));
    }
    let command = decode_command(&frame[20..total - 4])?;
    Ok(Entry { term, round, command })
}

/// One entry as `term | round | command`, without the framing.
///
/// This is what `transport.rs` puts inside an `Append`: the log's own framing is a property of the
/// file, and re-sending a length and a CRC that the transport already provides would be two
/// answers to one question.
pub fn encode_entry(e: &Entry, out: &mut Vec<u8>) -> Result<(), LogError> {
    out.extend_from_slice(&e.term.to_be_bytes());
    out.extend_from_slice(&e.round.to_be_bytes());
    encode_command(&e.command, out)
}

/// The inverse of [`encode_entry`], refusing trailing bytes.
pub fn decode_entry(bytes: &[u8]) -> Result<Entry, LogError> {
    let mut at = 0usize;
    let term = take_u64(bytes, &mut at)?;
    let round = take_u64(bytes, &mut at)?;
    let command = decode_command(&bytes[at..])?;
    Ok(Entry { term, round, command })
}

/// The **one** encoding of a [`Command`].
///
/// Tags are assigned once and never recycled: a tag that changes meaning turns an old log into a
/// plausible new one. Every arm is written out rather than derived, so adding a variant to
/// `Command` fails to compile here instead of silently acquiring a tag someone else already used.
pub fn encode_command(c: &Command, out: &mut Vec<u8>) -> Result<(), LogError> {
    match c {
        Command::WalBatch { start_lsn, bytes } => {
            out.push(0);
            out.extend_from_slice(&start_lsn.to_be_bytes());
            out.extend_from_slice(&fits_u32(bytes.len(), "wal batch")?.to_be_bytes());
            out.extend_from_slice(bytes);
        }
        Command::Catalog { op, table, columns } => {
            out.push(1);
            write_ddl_op(out, op)?;
            put_str(out, table, "table name")?;
            out.extend_from_slice(&fits_u16(columns.len(), "column count")?.to_be_bytes());
            for (name, ty, nullable) in columns {
                put_str(out, name, "column name")?;
                write_data_type(out, ty);
                out.push(u8::from(*nullable));
            }
        }
        Command::Branch { op } => {
            out.push(2);
            match op {
                BranchOp::Fork { child, parent, fork_epoch, lease_millis } => {
                    out.push(0);
                    out.extend_from_slice(&child.to_be_bytes());
                    out.extend_from_slice(&parent.to_be_bytes());
                    out.extend_from_slice(&fork_epoch.to_be_bytes());
                    out.extend_from_slice(&lease_millis.to_be_bytes());
                }
                BranchOp::Merge { branch, base_round } => {
                    out.push(1);
                    out.extend_from_slice(&branch.to_be_bytes());
                    out.extend_from_slice(&base_round.to_be_bytes());
                }
                BranchOp::Abandon { branch } => {
                    out.push(2);
                    out.extend_from_slice(&branch.to_be_bytes());
                }
                BranchOp::Reap { branch, generation } => {
                    out.push(3);
                    out.extend_from_slice(&branch.to_be_bytes());
                    out.extend_from_slice(&generation.to_be_bytes());
                }
            }
        }
        Command::ArenaGrant { node, first_page, page_count } => {
            out.push(3);
            out.extend_from_slice(&node.0.to_be_bytes());
            out.extend_from_slice(&first_page.to_be_bytes());
            out.extend_from_slice(&page_count.to_be_bytes());
        }
        Command::TxnIdRange { node, lo, hi } => {
            out.push(4);
            out.extend_from_slice(&node.0.to_be_bytes());
            out.extend_from_slice(&lo.to_be_bytes());
            out.extend_from_slice(&hi.to_be_bytes());
        }
        Command::LeaseTick { unix_millis } => {
            out.push(5);
            out.extend_from_slice(&unix_millis.to_be_bytes());
        }
        Command::Checkpoint => out.push(6),
        Command::Membership { config } => {
            out.push(7);
            write_config(out, config)?;
        }
        Command::NoOp => out.push(8),
    }
    Ok(())
}

/// The inverse of [`encode_command`].
///
/// **Trailing bytes are refused.** A decoder that stops at the end of the value it recognised
/// accepts two encodings of one command, and two encodings mean two nodes can hold byte-different
/// logs that decode identically — which defeats every digest and every byte comparison built on top
/// of this.
pub fn decode_command(bytes: &[u8]) -> Result<Command, LogError> {
    let mut at = 0usize;
    let c = read_command(bytes, &mut at)?;
    if at != bytes.len() {
        return Err(LogError::Corrupt(format!(
            "a command decoded from {at} of {} bytes; the {} trailing bytes are a second encoding \
             of the same value",
            bytes.len(),
            bytes.len() - at
        )));
    }
    Ok(c)
}

fn read_command(bytes: &[u8], at: &mut usize) -> Result<Command, LogError> {
    Ok(match take_u8(bytes, at)? {
        0 => {
            let start_lsn = take_u64(bytes, at)?;
            let len = take_u32(bytes, at)? as usize;
            Command::WalBatch { start_lsn, bytes: take_bytes(bytes, at, len)? }
        }
        1 => {
            let op = read_ddl_op(bytes, at)?;
            let table = take_str(bytes, at)?;
            let n = take_u16(bytes, at)? as usize;
            let mut columns = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                let name = take_str(bytes, at)?;
                let ty = read_data_type(bytes, at)?;
                let nullable = take_u8(bytes, at)? != 0;
                columns.push((name, ty, nullable));
            }
            Command::Catalog { op, table, columns }
        }
        2 => Command::Branch {
            op: match take_u8(bytes, at)? {
                0 => BranchOp::Fork {
                    child: take_u64(bytes, at)?,
                    parent: take_u64(bytes, at)?,
                    fork_epoch: take_u64(bytes, at)?,
                    lease_millis: take_u64(bytes, at)?,
                },
                1 => BranchOp::Merge {
                    branch: take_u64(bytes, at)?,
                    base_round: take_u64(bytes, at)?,
                },
                2 => BranchOp::Abandon { branch: take_u64(bytes, at)? },
                3 => BranchOp::Reap {
                    branch: take_u64(bytes, at)?,
                    generation: take_u32(bytes, at)?,
                },
                other => {
                    return Err(LogError::Corrupt(format!("unknown branch op tag {other}")));
                }
            },
        },
        3 => Command::ArenaGrant {
            node: NodeId(take_u32(bytes, at)?),
            first_page: take_u32(bytes, at)?,
            page_count: take_u32(bytes, at)?,
        },
        4 => Command::TxnIdRange {
            node: NodeId(take_u32(bytes, at)?),
            lo: take_u64(bytes, at)?,
            hi: take_u64(bytes, at)?,
        },
        5 => Command::LeaseTick { unix_millis: take_u64(bytes, at)? },
        6 => Command::Checkpoint,
        7 => Command::Membership { config: read_config(bytes, at)? },
        8 => Command::NoOp,
        other => return Err(LogError::Corrupt(format!("unknown command tag {other}"))),
    })
}

/// `version | term | members | learners`.
///
/// Both lists travel. A configuration that shipped only its voters would arrive as one whose
/// learners had been removed, and removing a learner is a membership change nobody proposed.
fn write_config(out: &mut Vec<u8>, cfg: &Config) -> Result<(), LogError> {
    out.extend_from_slice(&cfg.version.to_be_bytes());
    out.extend_from_slice(&cfg.term.to_be_bytes());
    out.extend_from_slice(&fits_u32(cfg.members().len(), "voter count")?.to_be_bytes());
    for n in cfg.members() {
        out.extend_from_slice(&n.0.to_be_bytes());
    }
    out.extend_from_slice(&fits_u32(cfg.learners().len(), "learner count")?.to_be_bytes());
    for n in cfg.learners() {
        out.extend_from_slice(&n.0.to_be_bytes());
    }
    Ok(())
}

fn read_config(bytes: &[u8], at: &mut usize) -> Result<Config, LogError> {
    let version = take_u64(bytes, at)?;
    let term = take_u64(bytes, at)?;
    let members = read_ids(bytes, at)?;
    let learners = read_ids(bytes, at)?;
    Ok(Config::new(members, version, term).with_learners(learners))
}

/// A length-prefixed list of node ids.
///
/// **The claimed count never reaches an allocator.** `Vec::with_capacity(n)` on an `n` that came
/// off a disk or a socket is a seventeen-gigabyte allocation triggered by four bad bytes, and a
/// check placed *beside* such a call is a check somebody can delete without any test noticing —
/// measured: a mutant that removed exactly that check survived the whole suite, because both
/// versions still returned an error and no assertion can see how much memory was reserved on the
/// way. So the bound is the **slice**: the bytes are proven present first, and the vector's
/// capacity is then a function of a slice that exists rather than of a number a peer chose.
fn read_ids(bytes: &[u8], at: &mut usize) -> Result<Vec<NodeId>, LogError> {
    let n = take_u32(bytes, at)? as usize;
    let want = n.checked_mul(4).ok_or_else(|| {
        LogError::Corrupt(format!("a node list claims {n} entries, which cannot be a byte count"))
    })?;
    let slice = bytes.get(*at..at.saturating_add(want)).ok_or_else(|| {
        LogError::Corrupt(format!(
            "a node list claims {n} entries ({want} bytes) but only {} bytes remain",
            bytes.len().saturating_sub(*at)
        ))
    })?;
    *at += want;
    Ok(slice
        .chunks_exact(4)
        .map(|c| NodeId(u32::from_be_bytes(c.try_into().unwrap())))
        .collect())
}

fn take_bytes(bytes: &[u8], at: &mut usize, n: usize) -> Result<Vec<u8>, LogError> {
    let end = at.checked_add(n).ok_or_else(|| LogError::from(short(*at, n, bytes.len())))?;
    let slice = bytes.get(*at..end).ok_or_else(|| LogError::from(short(*at, n, bytes.len())))?;
    *at = end;
    Ok(slice.to_vec())
}

fn write_ddl_op(out: &mut Vec<u8>, op: &DdlOp) -> Result<(), LogError> {
    match op {
        DdlOp::CreateTable => out.push(0),
        DdlOp::DropTable => out.push(1),
        DdlOp::AlterColumn(alt) => {
            out.push(2);
            match alt {
                ColumnAlteration::Add { column } => {
                    out.push(0);
                    put_str(out, column, "column name")?;
                }
                ColumnAlteration::Rename { from, to } => {
                    out.push(1);
                    put_str(out, from, "column name")?;
                    put_str(out, to, "column name")?;
                }
                ColumnAlteration::Retype { column, from } => {
                    out.push(2);
                    put_str(out, column, "column name")?;
                    write_data_type(out, from);
                }
            }
        }
    }
    Ok(())
}

fn read_ddl_op(bytes: &[u8], at: &mut usize) -> Result<DdlOp, LogError> {
    Ok(match take_u8(bytes, at)? {
        0 => DdlOp::CreateTable,
        1 => DdlOp::DropTable,
        2 => DdlOp::AlterColumn(match take_u8(bytes, at)? {
            0 => ColumnAlteration::Add { column: take_str(bytes, at)? },
            1 => ColumnAlteration::Rename {
                from: take_str(bytes, at)?,
                to: take_str(bytes, at)?,
            },
            2 => ColumnAlteration::Retype {
                column: take_str(bytes, at)?,
                from: read_data_type(bytes, at)?,
            },
            other => {
                return Err(LogError::Corrupt(format!("unknown column alteration tag {other}")));
            }
        }),
        other => return Err(LogError::Corrupt(format!("unknown ddl op tag {other}"))),
    })
}

/// A [`DataType`] as one tag byte, **using the same tags `wal::log` uses**.
///
/// A second copy, and it is here under protest: `wal::log`'s `write_data_type` is private and
/// `mod.rs` is not this row's to change. The duplication is therefore not a choice, but a silent
/// divergence would be: `Command::Catalog` and `RecKind::Ddl` describe the same schema change, and
/// if the two files disagreed about a tag, a `Timestamp` column replicated through consensus would
/// arrive as a `Decimal` in the change feed. `tests_log.rs` pins the agreement by deriving the
/// WAL's tag for every variant from the WAL's own encoder and comparing.
fn write_data_type(out: &mut Vec<u8>, ty: &DataType) {
    match ty {
        DataType::Integer => out.push(0),
        DataType::Float => out.push(1),
        DataType::Boolean => out.push(2),
        DataType::Varchar(n) => {
            out.push(3);
            out.extend_from_slice(&n.to_be_bytes());
        }
        DataType::BigInt => out.push(4),
        DataType::Decimal => out.push(5),
        DataType::Timestamp => out.push(6),
    }
}

fn read_data_type(bytes: &[u8], at: &mut usize) -> Result<DataType, LogError> {
    Ok(match take_u8(bytes, at)? {
        0 => DataType::Integer,
        1 => DataType::Float,
        2 => DataType::Boolean,
        3 => DataType::Varchar(take_u16(bytes, at)?),
        4 => DataType::BigInt,
        5 => DataType::Decimal,
        6 => DataType::Timestamp,
        other => return Err(LogError::Corrupt(format!("unknown column type tag {other}"))),
    })
}

// `mod.rs` is frozen for this phase and carries no `mod tests_log`, so the test module is declared
// here. `#[path]` on a module that is not inside an inline block resolves against the directory of
// this file, which is where the brief says the test file lives.
#[cfg(test)]
#[path = "tests_log.rs"]
mod tests_log;
