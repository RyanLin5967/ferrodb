//! **D212 (a') — the store REVERT's durable history lives in: `<db>.history`.**
//!
//! SCALE-DESIGN "D212 (a') DECIDED" and "AMENDED". The history of a merge is written in three
//! places, in this order, and each has one job:
//!
//! 1. **The WAL, inside the publish transaction** (`RecKind::RevertHistory`, tag 11). This is what
//!    makes the history atomic with the rows: the transaction's `Commit` record decides for both,
//!    so no crash can leave rows without their history or history without its rows.
//! 2. **This store's in-memory queue**, the moment `TxnManager::commit` has flushed the `Commit`
//!    record — before `TxnEnd` and before the automatic checkpoint that `commit` itself may run.
//! 3. **This file**, written only by the checkpoint hook (`TxnManager::checkpoint_locked`), after
//!    the pages are flushed and BEFORE the WAL is truncated: every queued record in ONE
//!    [`append_durably`] (one fsync per checkpoint, none per publish), or, when a prune is due, the
//!    retained window plus the queue in ONE [`replace_atomically`]. A failure refuses the
//!    truncation, so the WAL keeps every record the file does not.
//!
//! Open reverses it: the file is loaded, then `recover` queues every tag-11 record of a
//! transaction that has a `Commit` and whose `hseq` is above the file's last, and the first
//! checkpoint drains them. Idempotency is keyed on `hseq`, never on the publish ordinal, because a
//! REVERT's marker has an `hseq` and no ordinal.
//!
//! # The file: the arena's `[image][tail record]*`, not a new shape
//!
//! `branch::arena`'s `<db>.arena` is the validated precedent, and this reuses its rules exactly:
//! the image is rewritten through [`replace_atomically`], records are appended through
//! [`append_durably`], every record carries its own CRC, a torn LAST record is dropped, and a bad
//! record with more bytes behind it is corruption and refused. As there, a process that did not
//! write the image itself never appends to it: its first write is a full rewrite, which is what
//! drops a torn tail the crashed session left.
//!
//! ```text
//! image  = MAGIC u32 | VERSION u8 | count u32 | record{count} | crc32 u32 (over all before it)
//! record = hseq u64 | ordinal u64 | len u32 | body[len] | crc32 u32 (over hseq..body)
//! tail   = record*
//! ```
//!
//! # Retention is physical
//!
//! The window is the newest `W` publishes (records whose `ordinal` is non-zero) and every record
//! after the oldest of them. Once `max(W/8, 1)` publishes have been drained since the last prune
//! and more than `W` are held, the drain rewrites the image with the window only: the file is
//! bounded by about `(W + W/8)` publishes whatever the number ever made, a prune reads nothing
//! (the window is in memory), and it costs one rewrite per `W/8` publishes — flat per merge.
//!
//! # What this file does not know
//!
//! Bodies are opaque here. What a record MEANS — a publish's ops and captures, a REVERT's marker —
//! is `agent_sql::revert_store`'s, which is the only reader of a body.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::error::FerroError;
use crate::storage::atomic_file::{append_durably, replace_atomically, OsFileOps};
use crate::wal::log::{crc32, take_u32, take_u64, take_u8};

/// How many publishes back a REVERT reaches when `FERRODB_REVERT_RETENTION_MERGES` is not set.
pub const DEFAULT_RETENTION_MERGES: u64 = 1024;

/// **The window `W`: how many publishes back a REVERT reaches.**
/// `FERRODB_REVERT_RETENTION_MERGES`, or [`DEFAULT_RETENTION_MERGES`] when unset. A value that is set
/// and is not a positive integer is REFUSED rather than defaulted: a typo would otherwise shrink or
/// grow what REVERT can reach, silently. Read by the entry point that opens the store, once.
pub fn retention_from_env() -> Result<u64, FerroError> {
    match std::env::var("FERRODB_REVERT_RETENTION_MERGES") {
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_RETENTION_MERGES),
        Err(e) => Err(FerroError::Internal(format!("FERRODB_REVERT_RETENTION_MERGES: {e}"))),
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(n) if n > 0 => Ok(n),
            _ => Err(FerroError::Internal(format!(
                "FERRODB_REVERT_RETENTION_MERGES={v:?} is not a positive number of merges; \
                 refusing to open with a REVERT window nobody chose"
            ))),
        },
    }
}

const MAGIC: u32 = 0x4652_4831; // "FRH1"
const IMAGE_VERSION: u8 = 1;
/// `MAGIC | VERSION | count`.
const IMAGE_HEADER: usize = 4 + 1 + 4;
/// `hseq | ordinal | len` in front of a body, `crc32` behind it.
const RECORD_FRAME: usize = 8 + 8 + 4 + 4;

/// One record of REVERT's history, as the store holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRecord {
    /// The history sequence: strictly increasing across every record ever written, publishes and
    /// markers alike. The store's idempotency key.
    pub hseq: u64,
    /// The publish ordinal for a publish, strictly increasing across publishes; 0 for anything
    /// else (a REVERT's marker). Retention counts publishes by it.
    pub ordinal: u64,
    /// Opaque here; see `agent_sql::revert_store`.
    pub body: Vec<u8>,
}

impl HistoryRecord {
    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), FerroError> {
        let len = u32::try_from(self.body.len()).map_err(|_| FerroError::Unrepresentable {
            what: "a REVERT history record".to_string(),
            len: self.body.len(),
            limit: u32::MAX as usize,
        })?;
        let start = out.len();
        out.extend_from_slice(&self.hseq.to_be_bytes());
        out.extend_from_slice(&self.ordinal.to_be_bytes());
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&self.body);
        let crc = crc32(&out[start..]);
        out.extend_from_slice(&crc.to_be_bytes());
        Ok(())
    }
}

/// Integers a test can read to show what the store did, and what it did not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HistoryCounters {
    /// Drains that wrote something.
    pub drains: u64,
    /// [`append_durably`] calls: one fsync each.
    pub appends: u64,
    /// [`replace_atomically`] calls: two fsyncs each.
    pub rewrites: u64,
    /// Rewrites that dropped records below the window.
    pub prunes: u64,
    /// Bytes of the file read at open. Nothing reads it after.
    pub bytes_read_at_open: u64,
}

/// REVERT's durable history. See the module doc.
pub struct HistoryStore {
    path: PathBuf,
    retention: u64,
    state: Mutex<StoreState>,
}

struct StoreState {
    /// Durable records, in `hseq` order.
    window: VecDeque<HistoryRecord>,
    /// Committed records not yet durable in the file, in `hseq` order.
    queue: Vec<HistoryRecord>,
    /// Whether THIS process wrote the image in the file. Until it has, it never appends: the tail
    /// behind an image it did not write may end in a torn record.
    image_written: bool,
    /// Publishes drained since the last prune (or held beyond `W` at open).
    publishes_since_prune: u64,
    counters: HistoryCounters,
}

impl HistoryStore {
    /// `<db>.history`, the file an entry point opens for the database at `db_path`.
    pub fn path_for_database(db_path: &str) -> PathBuf {
        format!("{db_path}.history").into()
    }

    /// **What an entry point calls, before `recover`:** `<db>.history` with the window from
    /// `FERRODB_REVERT_RETENTION_MERGES`.
    pub fn open_for_database(db_path: &str) -> Result<Arc<HistoryStore>, FerroError> {
        HistoryStore::open(HistoryStore::path_for_database(db_path), retention_from_env()?)
    }

    /// Open the store at `path`, reading it whole if it exists.
    ///
    /// A missing file is an empty history. A file that is not a history image, or whose image or a
    /// non-final record fails its checksum, is REFUSED rather than read as empty: an empty history
    /// would let a REVERT answer "no such merge" about one that was published.
    pub fn open(path: impl Into<PathBuf>, retention: u64) -> Result<Arc<HistoryStore>, FerroError> {
        if retention == 0 {
            return Err(FerroError::Internal(
                "a REVERT history store needs a retention window of at least one merge".into(),
            ));
        }
        let path = path.into();
        let (window, read) = match std::fs::read(&path) {
            Ok(bytes) => (load(&bytes)?, bytes.len() as u64),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (VecDeque::new(), 0),
            Err(e) => {
                return Err(FerroError::Io(format!("reading {}: {e}", path.display())));
            }
        };
        let held = window.iter().filter(|r| r.ordinal > 0).count() as u64;
        let state = StoreState {
            window,
            queue: Vec::new(),
            image_written: false,
            publishes_since_prune: held.saturating_sub(retention),
            counters: HistoryCounters { bytes_read_at_open: read, ..HistoryCounters::default() },
        };
        Ok(Arc::new(HistoryStore { path, retention, state: Mutex::new(state) }))
    }

    /// The window `W`, in publishes.
    pub fn retention(&self) -> u64 {
        self.retention
    }

    /// The highest `hseq` held, durable or queued; 0 when there is none.
    pub fn last_hseq(&self) -> u64 {
        let s = self.state.lock().unwrap();
        s.queue.last().or(s.window.back()).map_or(0, |r| r.hseq)
    }

    /// Queue committed records for the next drain, skipping any whose `hseq` is already held.
    ///
    /// Skipping is what makes the open's catch-up idempotent: the WAL a crash left may hold records
    /// the file already has, and they are recognised by `hseq` alone.
    pub fn enqueue(&self, records: Vec<HistoryRecord>) {
        let mut s = self.state.lock().unwrap();
        for r in records {
            let last = s.queue.last().or(s.window.back()).map_or(0, |r| r.hseq);
            if r.hseq > last {
                s.queue.push(r);
            }
        }
    }

    /// Every record held, durable then queued, in `hseq` order. At most about `W + W/8` publishes
    /// and the markers among them, whatever the number of merges ever made.
    pub fn records(&self) -> Vec<HistoryRecord> {
        let s = self.state.lock().unwrap();
        s.window.iter().chain(s.queue.iter()).cloned().collect()
    }

    pub fn counters(&self) -> HistoryCounters {
        self.state.lock().unwrap().counters
    }

    /// **The checkpoint hook's body.** Make every queued record durable — one [`append_durably`],
    /// or one [`replace_atomically`] when a prune is due or this process has not yet written the
    /// image — and only then report success. On failure nothing in memory changes, so the queue is
    /// written by the next attempt, and the caller must not truncate the WAL.
    pub fn drain(&self) -> Result<(), FerroError> {
        let mut s = self.state.lock().unwrap();
        if s.queue.is_empty() {
            return Ok(());
        }
        let queued_publishes = s.queue.iter().filter(|r| r.ordinal > 0).count() as u64;
        let since = s.publishes_since_prune + queued_publishes;
        let held = s.window.iter().chain(s.queue.iter()).filter(|r| r.ordinal > 0).count() as u64;
        let prune = since >= (self.retention / 8).max(1) && held > self.retention;
        if prune || !s.image_written {
            let cut = if prune { window_start(&s, self.retention) } else { 0 };
            let kept: Vec<HistoryRecord> =
                s.window.iter().chain(s.queue.iter()).filter(|r| r.hseq >= cut).cloned().collect();
            let image = encode_image(&kept)?;
            replace_atomically(&OsFileOps, &self.path, &image)
                .map_err(|e| FerroError::Io(format!("writing {}: {e}", self.path.display())))?;
            s.window = kept.into();
            s.queue.clear();
            s.image_written = true;
            s.publishes_since_prune = if prune { 0 } else { since };
            s.counters.rewrites += 1;
            if prune {
                s.counters.prunes += 1;
            }
        } else {
            let mut tail = Vec::new();
            for r in &s.queue {
                r.encode_into(&mut tail)?;
            }
            append_durably(&OsFileOps, &self.path, &tail)
                .map_err(|e| FerroError::Io(format!("appending to {}: {e}", self.path.display())))?;
            let queued: Vec<HistoryRecord> = s.queue.drain(..).collect();
            s.window.extend(queued);
            s.publishes_since_prune = since;
            s.counters.appends += 1;
        }
        s.counters.drains += 1;
        Ok(())
    }
}

/// The `hseq` of the oldest publish among the newest `retention`, or 0 when no more are held.
fn window_start(s: &StoreState, retention: u64) -> u64 {
    let publishes: Vec<u64> =
        s.window.iter().chain(s.queue.iter()).filter(|r| r.ordinal > 0).map(|r| r.hseq).collect();
    match usize::try_from(retention) {
        Ok(keep) if publishes.len() > keep => publishes[publishes.len() - keep],
        _ => 0,
    }
}

fn encode_image(records: &[HistoryRecord]) -> Result<Vec<u8>, FerroError> {
    let count = u32::try_from(records.len()).map_err(|_| FerroError::Unrepresentable {
        what: "a REVERT history image's record count".to_string(),
        len: records.len(),
        limit: u32::MAX as usize,
    })?;
    let mut out = Vec::with_capacity(IMAGE_HEADER + 4);
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.push(IMAGE_VERSION);
    out.extend_from_slice(&count.to_be_bytes());
    for r in records {
        r.encode_into(&mut out)?;
    }
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}

fn corrupt(msg: String) -> FerroError {
    FerroError::Internal(format!("REVERT history file: {msg}"))
}

/// One record at `bytes[at..]`: `Ok(Some((record, next)))`, `Ok(None)` when the bytes end inside
/// its frame, or `Err` when it is whole and fails its checksum.
fn take_record(bytes: &[u8], at: usize) -> Result<Option<(HistoryRecord, usize)>, FerroError> {
    let rest = &bytes[at..];
    if rest.len() < RECORD_FRAME {
        return Ok(None);
    }
    let mut i = 0usize;
    let hseq = take_u64(rest, &mut i)?;
    let ordinal = take_u64(rest, &mut i)?;
    let len = take_u32(rest, &mut i)? as usize;
    let Some(total) = len.checked_add(RECORD_FRAME).filter(|t| *t <= rest.len()) else {
        return Ok(None);
    };
    let stored = u32::from_be_bytes(rest[total - 4..total].try_into().unwrap());
    if crc32(&rest[..total - 4]) != stored {
        return Err(corrupt(format!("the record at byte {at} fails its checksum")));
    }
    let body = rest[20..20 + len].to_vec();
    Ok(Some((HistoryRecord { hseq, ordinal, body }, at + total)))
}

/// Load a whole `<db>.history`: the image, then every intact tail record behind it — the arena's
/// rule (`ArenaPageStore::replay_tail`): a torn LAST record is dropped, a checksum failure with
/// more bytes behind it is corruption and refused.
fn load(bytes: &[u8]) -> Result<VecDeque<HistoryRecord>, FerroError> {
    let mut at = 0usize;
    let magic = take_u32(bytes, &mut at).map_err(|_| corrupt("too short for an image".into()))?;
    if magic != MAGIC {
        return Err(corrupt(format!("not a history image (magic {magic:#x})")));
    }
    let version = take_u8(bytes, &mut at)?;
    if version != IMAGE_VERSION {
        return Err(corrupt(format!(
            "image version {version} was written by another build (this one reads {IMAGE_VERSION})"
        )));
    }
    let count = take_u32(bytes, &mut at)? as usize;
    let mut out: VecDeque<HistoryRecord> = VecDeque::new();
    for i in 0..count {
        match take_record(bytes, at)? {
            Some((r, next)) => {
                out.push_back(r);
                at = next;
            }
            None => return Err(corrupt(format!("the image ends inside record {i} of {count}"))),
        }
    }
    let crc_at = at;
    let stored = take_u32(bytes, &mut at).map_err(|_| corrupt("the image has no checksum".into()))?;
    if crc32(&bytes[..crc_at]) != stored {
        return Err(corrupt("the image fails its checksum".into()));
    }
    while at < bytes.len() {
        match take_record(bytes, at) {
            Ok(Some((r, next))) => {
                out.push_back(r);
                at = next;
            }
            // Torn: the last append did not finish, and was never acknowledged.
            Ok(None) => break,
            Err(e) => {
                let total = bytes.len() - at;
                // Whole and failing, as the LAST record: a torn append whose length field
                // survived. Anything behind it makes it corruption.
                let len = if total >= 20 {
                    u32::from_be_bytes(bytes[at + 16..at + 20].try_into().unwrap()) as usize
                } else {
                    0
                };
                if len + RECORD_FRAME < total {
                    return Err(e);
                }
                break;
            }
        }
    }
    let mut last = 0u64;
    for r in &out {
        if r.hseq <= last {
            return Err(corrupt(format!("hseq {} follows {last}; they must increase", r.hseq)));
        }
        last = r.hseq;
    }
    Ok(out)
}

/// **Reassemble tag-11 WAL parts into records**, for the open's catch-up.
///
/// A record larger than one WAL part is split across parts `0, 1, ..` of its transaction, the last
/// marked. `parts` is every `(txn, hseq, ordinal, part, last, bytes)` of the transactions that
/// COMMITTED, in log order; parts of different records may interleave, and are told apart by
/// `(txn, hseq)`. A record whose parts do not run `0, 1, ..` to one marked last is refused. The
/// result is in `hseq` order, which is what [`HistoryStore::enqueue`] keys on.
pub fn assemble(
    parts: Vec<(u64, u64, u64, u32, bool, Vec<u8>)>,
) -> Result<Vec<HistoryRecord>, FerroError> {
    // (txn, record so far, the last part appended to it)
    let mut open: Vec<(u64, HistoryRecord, u32)> = Vec::new();
    let mut done: Vec<HistoryRecord> = Vec::new();
    for (txn, hseq, ordinal, part, last, bytes) in parts {
        let at = match open.iter().position(|(t, r, _)| *t == txn && r.hseq == hseq) {
            None => {
                if part != 0 {
                    return Err(corrupt(format!(
                        "txn {txn}'s history record {hseq} starts at part {part}, not 0"
                    )));
                }
                open.push((txn, HistoryRecord { hseq, ordinal, body: bytes }, 0));
                open.len() - 1
            }
            Some(i) => {
                let (_, r, n) = &mut open[i];
                if part != *n + 1 {
                    return Err(corrupt(format!(
                        "txn {txn}'s history record {hseq} has part {part} after part {n}"
                    )));
                }
                r.body.extend_from_slice(&bytes);
                *n = part;
                i
            }
        };
        if last {
            done.push(open.remove(at).1);
        }
    }
    if let Some((t, r, _)) = open.first() {
        return Err(corrupt(format!("txn {t}'s history record {} has no last part", r.hseq)));
    }
    done.sort_by_key(|r| r.hseq);
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(hseq: u64, ordinal: u64) -> HistoryRecord {
        HistoryRecord { hseq, ordinal, body: format!("record {hseq}").into_bytes() }
    }

    fn open_in(dir: &tempfile::TempDir, retention: u64) -> Arc<HistoryStore> {
        HistoryStore::open(dir.path().join("t.history"), retention).unwrap()
    }

    #[test]
    fn drained_records_survive_a_reopen_and_queued_ones_do_not() {
        let dir = tempfile::tempdir().unwrap();
        let s = open_in(&dir, 8);
        s.enqueue(vec![rec(1, 1), rec(2, 2)]);
        s.drain().unwrap();
        s.enqueue(vec![rec(3, 0)]);
        s.drain().unwrap();
        s.enqueue(vec![rec(4, 3)]);
        assert_eq!(s.counters().rewrites, 1, "the first drain of a process rewrites the image");
        assert_eq!(s.counters().appends, 1, "later drains append");
        drop(s);
        let s = open_in(&dir, 8);
        let hseqs: Vec<u64> = s.records().iter().map(|r| r.hseq).collect();
        assert_eq!(hseqs, [1, 2, 3], "the queued record 4 was never drained, so it is not durable");
    }

    #[test]
    fn enqueue_skips_what_is_already_held_by_hseq() {
        let dir = tempfile::tempdir().unwrap();
        let s = open_in(&dir, 8);
        s.enqueue(vec![rec(1, 1), rec(2, 0)]);
        s.drain().unwrap();
        s.enqueue(vec![rec(1, 1), rec(2, 0), rec(3, 2)]);
        let hseqs: Vec<u64> = s.records().iter().map(|r| r.hseq).collect();
        assert_eq!(hseqs, [1, 2, 3]);
    }

    #[test]
    fn a_prune_keeps_the_newest_w_publishes_and_everything_after_the_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let s = open_in(&dir, 2);
        // W = 2, so a prune is due after max(2/8, 1) = 1 publish once more than 2 are held.
        let mut h = 0;
        for ordinal in 1..=5 {
            h += 1;
            s.enqueue(vec![rec(h, ordinal)]);
            h += 1;
            s.enqueue(vec![rec(h, 0)]); // a marker after each publish
            s.drain().unwrap();
        }
        let kept: Vec<(u64, u64)> = s.records().iter().map(|r| (r.hseq, r.ordinal)).collect();
        assert_eq!(kept, [(7, 4), (8, 0), (9, 5), (10, 0)]);
        assert!(s.counters().prunes >= 1);
        drop(s);
        let s = open_in(&dir, 2);
        assert_eq!(s.records().len(), 4, "the pruned image is what reopens");
    }

    #[test]
    fn a_torn_last_record_is_dropped_and_a_bad_middle_one_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.history");
        let s = HistoryStore::open(&path, 8).unwrap();
        s.enqueue(vec![rec(1, 1)]);
        s.drain().unwrap();
        s.enqueue(vec![rec(2, 2)]);
        s.drain().unwrap();
        s.enqueue(vec![rec(3, 3)]);
        s.drain().unwrap();
        drop(s);
        let whole = std::fs::read(&path).unwrap();

        // Torn: the last append lost its final bytes.
        std::fs::write(&path, &whole[..whole.len() - 3]).unwrap();
        let s = HistoryStore::open(&path, 8).unwrap();
        let hseqs: Vec<u64> = s.records().iter().map(|r| r.hseq).collect();
        assert_eq!(hseqs, [1, 2]);
        drop(s);

        // Corrupt: a byte of the MIDDLE tail record flipped, with a whole record behind it.
        let mut bad = whole.clone();
        let middle = bad.len() - 2 * (RECORD_FRAME + "record 3".len()) + 21;
        bad[middle] ^= 0xff;
        std::fs::write(&path, &bad).unwrap();
        assert!(HistoryStore::open(&path, 8).is_err(), "a corrupt middle record was read past");
    }

    #[test]
    fn a_file_that_is_not_a_history_image_is_refused_not_read_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.history");
        std::fs::write(&path, b"not a history file").unwrap();
        assert!(HistoryStore::open(&path, 8).is_err());
    }

    #[test]
    fn parts_reassemble_in_order_and_a_missing_part_is_refused() {
        let whole = assemble(vec![
            (5, 1, 1, 0, false, b"ab".to_vec()),
            (5, 1, 1, 1, true, b"cd".to_vec()),
            (6, 2, 0, 0, true, b"m".to_vec()),
        ])
        .unwrap();
        assert_eq!(whole[0], HistoryRecord { hseq: 1, ordinal: 1, body: b"abcd".to_vec() });
        assert_eq!(whole[1].hseq, 2);
        assert!(assemble(vec![(5, 1, 1, 1, true, b"cd".to_vec())]).is_err());
        assert!(assemble(vec![(5, 1, 1, 0, false, b"ab".to_vec())]).is_err());
    }
}
