//! Primary/replica replication by WAL log shipping.
//!
//! The substrate was already right for this. An LSN *is* a byte offset, every record carries a
//! CRC32, `read_record` hands back the next LSN, and `scan_valid_end` copes with a torn tail. So
//! replication here is shipping bytes of an existing ordered log, not new storage machinery.
//!
//! # What this is, and firmly is not
//!
//! It is **asynchronous physical replication**: a replica connects, says how far it has got, and
//! the primary streams the log onward. The replica applies records and converges.
//!
//! It is **not consensus**. There is no Raft, no leader election, no automatic failover, and no
//! split-brain protection. A primary/replica pair without consensus cannot safely promote a
//! replica — two nodes that both believe they are primary will diverge, and nothing here would
//! detect it. Every one of those is a real feature this does not have, and calling log shipping
//! "distributed" without saying so would be the kind of overclaim that a reader who works on
//! replication would catch in a minute.
//!
//! # The rule that matters most
//!
//! **A primary must never ship a record it has not durably written.** Streaming from the in-memory
//! buffer would let a replica hold records the primary loses on a crash — the replica is then
//! *ahead* of the primary, which is unrecoverable divergence rather than ordinary lag. So the
//! source stops at `flushed_lsn`, and D22 is why that value can now be trusted: it used to
//! over-report by ~117KB under concurrent flushes.

use std::io::{Read, Write};

use crate::error::FerroError;
use crate::wal::log::WalManager;

/// Base backup — see [`backup`]. Log shipping alone cannot start a replica: the primary truncates
/// its WAL at every checkpoint, so a replica needs a page image plus the LSN it corresponds to.
pub mod backup;

/// `0xFEDB` then a protocol version, so a mismatched peer is rejected at the handshake rather than
/// misparsed into nonsense several frames later.
pub const REPL_MAGIC: u32 = 0xFEDB_0001;
pub const REPL_VERSION: u16 = 1;

/// Largest batch a single `Records` frame may carry, so a replica cannot be made to allocate an
/// unbounded buffer by a peer claiming a huge length.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// Replica -> primary: "I am durable up to this LSN; send me what follows."
    Hello { from_lsn: u64 },
    /// Primary -> replica: a run of raw WAL frames beginning at `start_lsn`.
    Records { start_lsn: u64, bytes: Vec<u8> },
    /// Primary -> replica: nothing further is durable yet; `durable_lsn` is how far the primary has.
    UpToDate { durable_lsn: u64 },
    Error { message: String },
}

impl Message {
    fn tag(&self) -> u8 {
        match self {
            Message::Hello { .. } => b'H',
            Message::Records { .. } => b'R',
            Message::UpToDate { .. } => b'U',
            Message::Error { .. } => b'E',
        }
    }

    fn body(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            Message::Hello { from_lsn } => b.extend_from_slice(&from_lsn.to_be_bytes()),
            Message::Records { start_lsn, bytes } => {
                b.extend_from_slice(&start_lsn.to_be_bytes());
                b.extend_from_slice(bytes);
            }
            Message::UpToDate { durable_lsn } => b.extend_from_slice(&durable_lsn.to_be_bytes()),
            Message::Error { message } => b.extend_from_slice(message.as_bytes()),
        }
        b
    }

    /// `tag | u32 length-of-body | body`.
    ///
    /// The length covers the body **only** — not the tag and not itself. pgwire's length includes
    /// itself and excludes the tag, which is a different convention; mixing the two up is the
    /// classic way a hand-written protocol appears to work and then desyncs, so this one is stated
    /// plainly here and pinned by a test rather than left to be inferred.
    pub fn encode(&self) -> Vec<u8> {
        let body = self.body();
        let mut out = Vec::with_capacity(body.len() + 5);
        out.push(self.tag());
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    pub fn read_from(r: &mut impl Read) -> Result<Message, FerroError> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag).map_err(|e| FerroError::Io(e.to_string()))?;
        let mut len = [0u8; 4];
        r.read_exact(&mut len).map_err(|e| FerroError::Io(e.to_string()))?;
        let len = u32::from_be_bytes(len) as usize;
        if len > MAX_FRAME_BYTES {
            // Refuse rather than allocate: a peer should not be able to choose this process's
            // memory usage.
            return Err(FerroError::Wal(format!(
                "replication frame claims {len} bytes, over the {MAX_FRAME_BYTES} limit"
            )));
        }
        let mut body = vec![0u8; len];
        r.read_exact(&mut body).map_err(|e| FerroError::Io(e.to_string()))?;

        let need = |n: usize| -> Result<(), FerroError> {
            if body.len() < n {
                return Err(FerroError::Wal(format!(
                    "replication frame '{}' is {} bytes, needs {n}",
                    tag[0] as char,
                    body.len()
                )));
            }
            Ok(())
        };
        let u64_at = |b: &[u8]| u64::from_be_bytes(b[0..8].try_into().unwrap());

        match tag[0] {
            b'H' => {
                need(8)?;
                Ok(Message::Hello { from_lsn: u64_at(&body) })
            }
            b'R' => {
                need(8)?;
                Ok(Message::Records { start_lsn: u64_at(&body), bytes: body[8..].to_vec() })
            }
            b'U' => {
                need(8)?;
                Ok(Message::UpToDate { durable_lsn: u64_at(&body) })
            }
            b'E' => Ok(Message::Error { message: String::from_utf8_lossy(&body).into_owned() }),
            other => Err(FerroError::Wal(format!(
                "unknown replication frame '{}'",
                other as char
            ))),
        }
    }

    pub fn write_to(&self, w: &mut impl Write) -> Result<(), FerroError> {
        w.write_all(&self.encode()).map_err(|e| FerroError::Io(e.to_string()))?;
        w.flush().map_err(|e| FerroError::Io(e.to_string()))
    }
}

/// Handshake: magic + version, exchanged before any message.
pub fn write_handshake(w: &mut impl Write) -> Result<(), FerroError> {
    let mut buf = Vec::with_capacity(6);
    buf.extend_from_slice(&REPL_MAGIC.to_be_bytes());
    buf.extend_from_slice(&REPL_VERSION.to_be_bytes());
    w.write_all(&buf).map_err(|e| FerroError::Io(e.to_string()))?;
    w.flush().map_err(|e| FerroError::Io(e.to_string()))
}

pub fn read_handshake(r: &mut impl Read) -> Result<(), FerroError> {
    let mut buf = [0u8; 6];
    r.read_exact(&mut buf).map_err(|e| FerroError::Io(e.to_string()))?;
    let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    let version = u16::from_be_bytes(buf[4..6].try_into().unwrap());
    if magic != REPL_MAGIC {
        return Err(FerroError::Wal(format!(
            "not a ferrodb replication peer (magic {magic:#x})"
        )));
    }
    if version != REPL_VERSION {
        return Err(FerroError::Wal(format!(
            "replication protocol version {version}; this build speaks {REPL_VERSION}"
        )));
    }
    Ok(())
}

/// Reads durable WAL bytes out of a primary for shipping.
pub struct ReplicationSource<'a> {
    wal: &'a WalManager,
}

impl<'a> ReplicationSource<'a> {
    pub fn new(wal: &'a WalManager) -> Self {
        ReplicationSource { wal }
    }

    /// Where this primary's log begins.
    ///
    /// A fresh replica cannot simply ask from 0: `truncate` moves the base forward, so the oldest
    /// retained record is at `base_lsn`, not at the origin. Without this a new replica asks for a
    /// range that no longer exists and is told the log was truncated away — which is a true answer
    /// to the wrong question. Discovered by a test that started at 0 and got exactly that error.
    pub fn start_lsn(&self) -> u64 {
        self.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// How far this primary is willing to ship: its durable frontier.
    pub fn durable_lsn(&self) -> u64 {
        self.wal.flushed_lsn.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Records starting at `from_lsn`, stopping at the durable frontier.
    ///
    /// **Never ships past `flushed_lsn`.** Streaming buffered records would let the replica hold
    /// data the primary loses on a crash — the replica would then be ahead of its primary, which
    /// is divergence rather than lag, and nothing downstream could reconcile it. Lag is fine;
    /// being ahead is not.
    ///
    /// Returns the raw frames and the LSN immediately after the last one, so the caller's next
    /// request is exact rather than inferred from a byte count.
    pub fn read_from(&self, from_lsn: u64, max_bytes: usize) -> Result<(Vec<u8>, u64), FerroError> {
        let durable = self.durable_lsn();
        if from_lsn >= durable {
            return Ok((Vec::new(), from_lsn));
        }
        let mut out = Vec::new();
        let mut lsn = from_lsn;
        while lsn < durable && out.len() < max_bytes {
            let (_rec, next) = self.wal.read_record(lsn)?;
            if next > durable {
                // A record straddling the durable frontier is not durable. Stop before it.
                break;
            }
            let len = (next - lsn) as usize;
            let frame = self.wal.raw_frame(lsn, len)?;
            out.extend_from_slice(&frame);
            lsn = next;
        }
        Ok((out, lsn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_length_covers_the_body_only() {
        // Stated explicitly because pgwire in this same codebase uses a different convention, and
        // confusing the two is how a hand-rolled protocol desyncs three frames in.
        let m = Message::Hello { from_lsn: 42 };
        let bytes = m.encode();
        assert_eq!(bytes[0], b'H');
        let len = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
        assert_eq!(len, m.body().len(), "length must cover the body only");
        assert_eq!(bytes.len(), 1 + 4 + len, "tag + length + body");
    }

    #[test]
    fn every_message_round_trips() {
        for m in [
            Message::Hello { from_lsn: 7 },
            Message::Records { start_lsn: 100, bytes: vec![1, 2, 3, 4] },
            Message::UpToDate { durable_lsn: 999 },
            Message::Error { message: "nope".into() },
        ] {
            let encoded = m.encode();
            let mut cursor = std::io::Cursor::new(encoded);
            assert_eq!(Message::read_from(&mut cursor).unwrap(), m);
        }
    }

    #[test]
    fn an_empty_records_batch_is_distinct_from_no_batch() {
        // Zero records is a legitimate answer ("you are caught up"), and must not decode as
        // something else or fail.
        let m = Message::Records { start_lsn: 5, bytes: Vec::new() };
        let mut cursor = std::io::Cursor::new(m.encode());
        assert_eq!(Message::read_from(&mut cursor).unwrap(), m);
    }

    #[test]
    fn a_truncated_frame_is_refused_rather_than_misread() {
        let m = Message::Hello { from_lsn: 7 };
        let full = m.encode();
        for cut in 1..full.len() {
            let mut cursor = std::io::Cursor::new(full[..cut].to_vec());
            assert!(
                Message::read_from(&mut cursor).is_err(),
                "a {cut}-byte prefix decoded instead of being refused"
            );
        }
    }

    #[test]
    fn an_oversized_length_is_refused_without_allocating() {
        let mut bytes = vec![b'R'];
        bytes.extend_from_slice(&(u32::MAX).to_be_bytes());
        let mut cursor = std::io::Cursor::new(bytes);
        let e = Message::read_from(&mut cursor).unwrap_err();
        assert!(format!("{e}").contains("over the"), "got {e}");
    }

    #[test]
    fn a_frame_too_short_for_its_own_fields_is_refused() {
        // 'H' claims 3 bytes of body but Hello needs 8.
        let mut bytes = vec![b'H'];
        bytes.extend_from_slice(&3u32.to_be_bytes());
        bytes.extend_from_slice(&[0, 0, 0]);
        let mut cursor = std::io::Cursor::new(bytes);
        assert!(Message::read_from(&mut cursor).is_err());
    }

    #[test]
    fn the_handshake_rejects_a_stranger_and_a_version_mismatch() {
        let mut good = Vec::new();
        write_handshake(&mut good).unwrap();
        assert!(read_handshake(&mut std::io::Cursor::new(good)).is_ok());

        let mut wrong_magic = Vec::new();
        wrong_magic.extend_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        wrong_magic.extend_from_slice(&REPL_VERSION.to_be_bytes());
        let e = read_handshake(&mut std::io::Cursor::new(wrong_magic)).unwrap_err();
        assert!(format!("{e}").contains("not a ferrodb replication peer"), "got {e}");

        let mut wrong_version = Vec::new();
        wrong_version.extend_from_slice(&REPL_MAGIC.to_be_bytes());
        wrong_version.extend_from_slice(&99u16.to_be_bytes());
        let e = read_handshake(&mut std::io::Cursor::new(wrong_version)).unwrap_err();
        assert!(format!("{e}").contains("version 99"), "got {e}");
    }
}

/// Applies a primary's shipped WAL frames on a replica.
///
/// Redo goes through `wal::recovery::apply_redo`, the same code recovery uses, rather than a
/// second implementation that could drift from it. That also inherits its idempotence: a record
/// whose LSN is at or below the page's own LSN is skipped, which is what makes a reconnect's
/// overlap harmless instead of a double-apply.
pub struct ReplicaApplier {
    bp: std::sync::Arc<crate::buffer::buffer_pool::BufferPoolManager>,
    applied_lsn: std::sync::atomic::AtomicU64,
}

impl ReplicaApplier {
    /// `start_lsn` is where this replica's log begins — the primary's base, not zero.
    pub fn new(
        bp: std::sync::Arc<crate::buffer::buffer_pool::BufferPoolManager>,
        start_lsn: u64,
    ) -> Self {
        ReplicaApplier { bp, applied_lsn: std::sync::atomic::AtomicU64::new(start_lsn) }
    }

    /// How far this replica has applied. This is what it sends in `Hello`.
    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Apply a batch of raw frames that begin at `start_lsn`.
    ///
    /// Returns the LSN immediately after the last frame applied.
    ///
    /// Every frame is CRC-checked against the bytes that actually arrived, and its embedded LSN is
    /// checked against where the walk says it should be. A batch that fails either is refused
    /// whole: applying a prefix and then erroring would leave the replica at an LSN it cannot
    /// justify, which is worse than refusing to advance at all.
    pub fn apply(&self, start_lsn: u64, bytes: &[u8]) -> Result<u64, FerroError> {
        use crate::wal::log::{crc32, LogRecord};

        // Validate the whole batch before touching a page.
        let mut checked: Vec<(u64, LogRecord)> = Vec::new();
        let mut at = 0usize;
        let mut lsn = start_lsn;
        while at < bytes.len() {
            if at + 4 > bytes.len() {
                return Err(FerroError::Wal("batch ends mid-length-prefix".into()));
            }
            let total = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            if total < 33 || at + total > bytes.len() {
                return Err(FerroError::Wal(format!(
                    "frame at offset {at} claims {total} bytes, which runs past the batch"
                )));
            }
            let frame = &bytes[at..at + total];

            let stored = u32::from_be_bytes(frame[total - 4..total].try_into().unwrap());
            if crc32(&frame[..total - 4]) != stored {
                return Err(FerroError::Wal(format!(
                    "frame at lsn {lsn} failed its CRC; the bytes on the wire are not the bytes \
                     the primary wrote"
                )));
            }

            let embedded = u64::from_be_bytes(frame[4..12].try_into().unwrap());
            if embedded != lsn {
                return Err(FerroError::Wal(format!(
                    "frame says it is lsn {embedded} but the stream places it at {lsn}; the \
                     replica would silently apply the log out of order"
                )));
            }

            let prev_lsn = u64::from_be_bytes(frame[12..20].try_into().unwrap());
            let txn_id = u64::from_be_bytes(frame[20..28].try_into().unwrap());
            let kind = crate::wal::log::RecKind::deserialize(&frame[28..total - 4])?;
            checked.push((lsn, LogRecord { lsn, prev_lsn, txn_id, kind }));

            at += total;
            lsn += total as u64;
        }

        // A replica's file does not yet contain the pages the primary is describing, so redo
        // would fail with a bare EOF on the first record. Recovery has the same problem after a
        // crash that extended the file without flushing, and solves it the same way: materialise
        // an empty page first. Found by the test, which is why this mirrors `recover()` rather
        // than being a second answer to one question.
        for (_, rec) in &checked {
            let touched = match &rec.kind {
                crate::wal::log::RecKind::HeapInsert { page_id, .. }
                | crate::wal::log::RecKind::HeapDelete { page_id, .. }
                | crate::wal::log::RecKind::HeapUpdate { page_id, .. } => Some(*page_id),
                crate::wal::log::RecKind::Clr { redo, .. } => match redo.as_ref() {
                    crate::wal::log::RecKind::HeapInsert { page_id, .. }
                    | crate::wal::log::RecKind::HeapDelete { page_id, .. }
                    | crate::wal::log::RecKind::HeapUpdate { page_id, .. } => Some(*page_id),
                    _ => None,
                },
                _ => None,
            };
            if let Some(page_id) = touched {
                if self.bp.disk_manager.read(page_id).is_err() {
                    self.bp.disk_manager.write(
                        page_id,
                        &crate::storage::heap_page::Page::empty(page_id).serialize()?,
                    )?;
                }
            }
        }

        // Only now apply. Redo is idempotent by page LSN, so an overlap re-sent after a reconnect
        // is skipped rather than applied twice.
        for (rec_lsn, rec) in &checked {
            match &rec.kind {
                crate::wal::log::RecKind::HeapInsert { .. }
                | crate::wal::log::RecKind::HeapDelete { .. }
                | crate::wal::log::RecKind::HeapUpdate { .. } => {
                    crate::wal::recovery::apply_redo(&self.bp, *rec_lsn, &rec.kind)?;
                }
                crate::wal::log::RecKind::Clr { redo, .. } => {
                    crate::wal::recovery::apply_redo(&self.bp, *rec_lsn, redo)?;
                }
                _ => {}
            }
        }

        // `fetch_max`, not a store: a late batch that overlaps ground already covered must not
        // move the replica backwards.
        self.applied_lsn.fetch_max(lsn, std::sync::atomic::Ordering::SeqCst);
        Ok(lsn)
    }
}
