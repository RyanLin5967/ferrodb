//! **D212 (a') — the store REVERT's durable history lives in: `<db>.history`.**
//!
//! SCALE-DESIGN "D212 (a') DECIDED" and "AMENDED". The history of a merge is written in three
//! places, in this order, and each has one job:
//!
//! 1. **The WAL, inside the publish transaction** (`RecKind::RevertHistory`, tag 12). This is what
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
//! Open reverses it: the file is loaded, then `recover` queues every tag-12 record of a
//! transaction that has a `Commit` and whose `hseq` the file does not hold — by membership, not by
//! position (AMENDED 2, F9) — and the first checkpoint drains them. Idempotency is keyed on `hseq`,
//! never on the publish ordinal, because a REVERT's marker has an `hseq` and no ordinal.
//!
//! Design authority: SCALE-DESIGN "D212 (a') DECIDED", "AMENDED" and "AMENDED 2", and "D253
//! CORRECTED", whose checkpoint fence covers the window between a `Commit` and the queue push —
//! this lane adds no second fence for it, and lands after D253 and after #16 (D213 item 4).
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

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::error::FerroError;
use crate::storage::atomic_file::{append_durably, replace_atomically, FileOps, OsFileOps};
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
    /// [`replace_atomically`] calls: two fsyncs each (the temporary, then the directory).
    pub rewrites: u64,
    /// Rewrites that dropped records below the window.
    pub prunes: u64,
    /// Drains whose write failed. The records stayed queued and the next write is a full rewrite.
    pub failed_drains: u64,
    /// Bytes of the file read at open. Nothing reads it after: a prune rewrites the window from
    /// memory.
    pub bytes_read_at_open: u64,
}

/// The queue bytes past which a commit drains the queue itself rather than wait for a checkpoint
/// (SCALE-DESIGN "D212 (a') AMENDED 2" F7): an idle open transaction blocks every checkpoint, and
/// the queue must not grow with merges while it does. One fsync per this many bytes.
pub const QUEUE_DRAIN_BYTES: usize = 1 << 20;

/// REVERT's durable history. See the module doc.
pub struct HistoryStore {
    path: PathBuf,
    retention: u64,
    /// The filesystem, as a seam: a unit test injects a write that fails.
    ops: Box<dyn FileOps + Send + Sync>,
    /// **The hook mutex** (AMENDED 2, F3): held from the drain, through the window computation, to
    /// the durable write, so two checkpoints — two committing threads can each run the automatic
    /// one — never interleave their writes. Taken before `atomic_file`'s `REPLACE_LOCK`, like the
    /// arena's `PersistState`.
    state: Mutex<StoreState>,
}

struct StoreState {
    /// Durable records, by `hseq`.
    window: BTreeMap<u64, HistoryRecord>,
    /// Committed records not yet durable in the file, by `hseq`. A map, so the open's catch-up
    /// tests membership in O(log n) per record rather than rescanning the queue.
    queue: BTreeMap<u64, HistoryRecord>,
    /// Bytes of `queue`'s records as they will be written.
    queued_bytes: usize,
    /// Whether THIS process wrote the image in the file, and no write has failed since. Until then
    /// it never appends: the tail behind an image it did not write may end in a torn record, and a
    /// failed append may have left one (AMENDED 2, F1 and F2).
    image_written: bool,
    /// Publishes drained since the last prune (or held beyond `W` at open).
    publishes_since_prune: u64,
    counters: HistoryCounters,
}

/// Names the file and the window; the records are not printed.
impl std::fmt::Debug for HistoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryStore")
            .field("path", &self.path)
            .field("retention", &self.retention)
            .finish_non_exhaustive()
    }
}

impl StoreState {
    fn holds(&self, hseq: u64) -> bool {
        self.window.contains_key(&hseq) || self.queue.contains_key(&hseq)
    }
}

impl HistoryStore {
    /// `<db>.history`, the file an entry point opens for the database at `db_path`.
    pub fn path_for_database(db_path: &str) -> PathBuf {
        format!("{db_path}.history").into()
    }

    /// **What an entry point calls, before `recover`:** `<db>.history` with the window from
    /// `FERRODB_REVERT_RETENTION_MERGES`.
    ///
    /// **Refuses a history file beside a database file that does not exist yet:** it was left by
    /// another database at that path, and reading it would give a new, empty database the merge
    /// history of an old one — history without its rows.
    /// `database_existed` is whether the database file was there BEFORE this open created it, which
    /// only the caller can know: every entry point creates the file before it attaches the store.
    pub fn open_for_database(
        db_path: &str,
        database_existed: bool,
    ) -> Result<Arc<HistoryStore>, FerroError> {
        let history = HistoryStore::path_for_database(db_path);
        if !database_existed && history.exists() {
            return Err(FerroError::Internal(format!(
                "{} exists but the database {db_path} does not: it is another database's REVERT \
                 history, and a new database must not inherit it. Move it away to create the \
                 database here",
                history.display()
            )));
        }
        HistoryStore::open(history, retention_from_env()?)
    }

    /// Open the store at `path`, reading it whole if it exists.
    ///
    /// A missing file is an empty history. A file that is not a history image, or whose image or a
    /// non-final record fails its checksum, is REFUSED rather than read as empty: an empty history
    /// would let a REVERT answer "no such merge" about one that was published.
    pub fn open(path: impl Into<PathBuf>, retention: u64) -> Result<Arc<HistoryStore>, FerroError> {
        HistoryStore::open_with_ops(path, retention, Box::new(OsFileOps))
    }

    fn open_with_ops(
        path: impl Into<PathBuf>,
        retention: u64,
        ops: Box<dyn FileOps + Send + Sync>,
    ) -> Result<Arc<HistoryStore>, FerroError> {
        if retention == 0 {
            return Err(FerroError::Internal(
                "a REVERT history store needs a retention window of at least one merge".into(),
            ));
        }
        let path = path.into();
        let (window, read) = match std::fs::read(&path) {
            Ok(bytes) => (load(&bytes)?, bytes.len() as u64),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (BTreeMap::new(), 0),
            Err(e) => {
                return Err(FerroError::Io(format!("reading {}: {e}", path.display())));
            }
        };
        let held = window.values().filter(|r| r.ordinal > 0).count() as u64;
        let state = StoreState {
            window,
            queue: BTreeMap::new(),
            queued_bytes: 0,
            image_written: false,
            publishes_since_prune: held.saturating_sub(retention),
            counters: HistoryCounters { bytes_read_at_open: read, ..HistoryCounters::default() },
        };
        Ok(Arc::new(HistoryStore { path, retention, ops, state: Mutex::new(state) }))
    }

    /// The window `W`, in publishes.
    pub fn retention(&self) -> u64 {
        self.retention
    }

    /// The highest `hseq` held, durable or queued; 0 when there is none.
    pub fn last_hseq(&self) -> u64 {
        let s = self.state.lock().unwrap();
        let durable = s.window.keys().next_back().copied().unwrap_or(0);
        s.queue.keys().next_back().copied().map_or(durable, |q| q.max(durable))
    }

    /// Queue committed records for the next drain, skipping any whose `hseq` is already held.
    ///
    /// By MEMBERSHIP, not by "above the tail" (AMENDED 2, F9): a committed record whose `hseq` is
    /// absent is queued wherever it falls, so a record that missed the file for any reason is
    /// recovered from the log rather than skipped for being older than a later one. Skipping what is
    /// held is what makes the open's catch-up idempotent.
    pub fn enqueue(&self, records: Vec<HistoryRecord>) {
        let mut s = self.state.lock().unwrap();
        for r in records {
            if !s.holds(r.hseq) {
                s.queued_bytes += RECORD_FRAME + r.body.len();
                s.queue.insert(r.hseq, r);
            }
        }
    }

    /// How many bytes are queued and not yet durable.
    pub fn queued_bytes(&self) -> usize {
        self.state.lock().unwrap().queued_bytes
    }

    /// Every record held, durable then queued, in `hseq` order. At most about `W + W/8` publishes
    /// and the markers among them, whatever the number of merges ever made.
    pub fn records(&self) -> Vec<HistoryRecord> {
        let s = self.state.lock().unwrap();
        let mut all: Vec<HistoryRecord> =
            s.window.values().chain(s.queue.values()).cloned().collect();
        all.sort_by_key(|r| r.hseq);
        all
    }

    pub fn counters(&self) -> HistoryCounters {
        self.state.lock().unwrap().counters
    }

    /// **The checkpoint hook's body.** Make every queued record durable — one [`append_durably`],
    /// or one [`replace_atomically`] when a prune is due or this process has not yet written the
    /// image since it opened or since a write failed — and only then report success.
    ///
    /// On failure the queue keeps every record (AMENDED 2, F2: they were never taken off it), the
    /// next write is forced to be a full rewrite, and the caller must not truncate the log. A retry
    /// after an append whose fsync failed therefore rewrites the image rather than appending the same
    /// records again behind possibly-written bytes; the reader keys on `hseq` either way.
    pub fn drain(&self) -> Result<(), FerroError> {
        let mut s = self.state.lock().unwrap();
        if s.queue.is_empty() {
            return Ok(());
        }
        let queued_publishes = s.queue.values().filter(|r| r.ordinal > 0).count() as u64;
        let since = s.publishes_since_prune + queued_publishes;
        let held =
            s.window.values().chain(s.queue.values()).filter(|r| r.ordinal > 0).count() as u64;
        let prune = since >= (self.retention / 8).max(1) && held > self.retention;
        let written = if prune || !s.image_written {
            let cut = if prune { window_start(&s, self.retention) } else { 0 };
            let mut kept: Vec<HistoryRecord> = s
                .window
                .values()
                .chain(s.queue.values())
                .filter(|r| r.hseq >= cut)
                .cloned()
                .collect();
            kept.sort_by_key(|r| r.hseq);
            encode_image(&kept).and_then(|image| {
                replace_atomically(&*self.ops, &self.path, &image)
                    .map_err(|e| FerroError::Io(format!("writing {}: {e}", self.path.display())))
            })
            .map(|()| Some(kept))
        } else {
            let mut tail = Vec::new();
            s.queue.values().try_for_each(|r| r.encode_into(&mut tail)).and_then(|()| {
                append_durably(&*self.ops, &self.path, &tail).map_err(|e| {
                    FerroError::Io(format!("appending to {}: {e}", self.path.display()))
                })
            })
            .map(|()| None)
        };
        match written {
            Err(e) => {
                s.image_written = false;
                s.counters.failed_drains += 1;
                Err(e)
            }
            Ok(Some(kept)) => {
                s.window = kept.into_iter().map(|r| (r.hseq, r)).collect();
                s.queue.clear();
                s.queued_bytes = 0;
                s.image_written = true;
                s.publishes_since_prune = if prune { 0 } else { since };
                s.counters.rewrites += 1;
                if prune {
                    s.counters.prunes += 1;
                }
                s.counters.drains += 1;
                Ok(())
            }
            Ok(None) => {
                let queued = std::mem::take(&mut s.queue);
                for r in queued.into_values() {
                    s.window.insert(r.hseq, r);
                }
                s.queued_bytes = 0;
                s.publishes_since_prune = since;
                s.counters.appends += 1;
                s.counters.drains += 1;
                Ok(())
            }
        }
    }
}

/// The `hseq` of the oldest publish among the newest `retention`, or 0 when no more are held.
fn window_start(s: &StoreState, retention: u64) -> u64 {
    let mut publishes: Vec<u64> = s
        .window
        .values()
        .chain(s.queue.values())
        .filter(|r| r.ordinal > 0)
        .map(|r| r.hseq)
        .collect();
    publishes.sort_unstable();
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
fn load(bytes: &[u8]) -> Result<BTreeMap<u64, HistoryRecord>, FerroError> {
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
    let mut out: Vec<HistoryRecord> = Vec::new();
    for i in 0..count {
        match take_record(bytes, at)? {
            Some((r, next)) => {
                out.push(r);
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
                out.push(r);
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
    // Keyed by `hseq`, the first copy kept: a record a retried write put down twice is one record
    // (AMENDED 2, F2), and one re-queued from the log lands in its place whatever its position.
    let mut by_hseq: BTreeMap<u64, HistoryRecord> = BTreeMap::new();
    for r in out {
        by_hseq.entry(r.hseq).or_insert(r);
    }
    Ok(by_hseq)
}

/// **Reassemble tag-12 WAL parts into records**, for the open's catch-up.
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

    /// A filesystem that fails the next `append` or `rename` it is told to, and otherwise is the
    /// real one — the fault a full disk or an EIO makes.
    struct Faulty {
        fail_append: std::sync::atomic::AtomicBool,
        fail_rename: std::sync::atomic::AtomicBool,
    }

    impl Faulty {
        fn new() -> Faulty {
            Faulty {
                fail_append: std::sync::atomic::AtomicBool::new(false),
                fail_rename: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl FileOps for Faulty {
        fn write(&self, path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
            OsFileOps.write(path, bytes)
        }
        fn append(&self, path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
            if self.fail_append.swap(false, std::sync::atomic::Ordering::SeqCst) {
                // Half the bytes land, as a write interrupted by a full disk leaves them.
                OsFileOps.append(path, &bytes[..bytes.len() / 2])?;
                return Err(std::io::Error::other("injected append failure"));
            }
            OsFileOps.append(path, bytes)
        }
        fn sync_file(&self, path: &std::path::Path) -> std::io::Result<()> {
            OsFileOps.sync_file(path)
        }
        fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
            if self.fail_rename.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(std::io::Error::other("injected rename failure"));
            }
            OsFileOps.rename(from, to)
        }
        fn sync_dir(&self, dir: &std::path::Path) -> std::io::Result<()> {
            OsFileOps.sync_dir(dir)
        }
    }

    /// **AMENDED 2, F2.** A failed write keeps every queued record queued and forces the next write
    /// to be a full rewrite — never an append behind the half a failed append left.
    ///
    /// Mutant: the queue is taken off before the write (no restore) — record 2 is gone.
    /// Mutant: a failure leaves `image_written` set — the retry appends behind the torn half, and
    /// the reopen refuses the file or loses record 3.
    #[test]
    fn a_failed_write_keeps_the_queue_and_forces_a_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.history");
        let faulty = Arc::new(Faulty::new());
        struct Shared(Arc<Faulty>);
        impl FileOps for Shared {
            fn write(&self, p: &std::path::Path, b: &[u8]) -> std::io::Result<()> { self.0.write(p, b) }
            fn append(&self, p: &std::path::Path, b: &[u8]) -> std::io::Result<()> { self.0.append(p, b) }
            fn sync_file(&self, p: &std::path::Path) -> std::io::Result<()> { self.0.sync_file(p) }
            fn rename(&self, f: &std::path::Path, t: &std::path::Path) -> std::io::Result<()> { self.0.rename(f, t) }
            fn sync_dir(&self, d: &std::path::Path) -> std::io::Result<()> { self.0.sync_dir(d) }
        }
        let s = HistoryStore::open_with_ops(&path, 8, Box::new(Shared(faulty.clone()))).unwrap();
        s.enqueue(vec![rec(1, 1)]);
        s.drain().unwrap();
        s.enqueue(vec![rec(2, 2)]);
        faulty.fail_append.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(s.drain().is_err(), "the injected failure was swallowed");
        assert_eq!(s.counters().failed_drains, 1);
        s.enqueue(vec![rec(3, 3)]);
        s.drain().unwrap();
        assert_eq!(s.counters().rewrites, 2, "the write after a failure was not a full rewrite");
        drop(s);
        let s = HistoryStore::open(&path, 8).unwrap();
        let hseqs: Vec<u64> = s.records().iter().map(|r| r.hseq).collect();
        assert_eq!(hseqs, [1, 2, 3], "a record queued across a failed write was lost or doubled");
    }

    /// **AMENDED 2, F1.** A torn tail, then an append at reopen: the first write of a process is a
    /// full rewrite, so nothing lands behind the bytes replay stopped on.
    ///
    /// Mutant: the store trusts a file it did not write (`image_written: true` at open) — record 3
    /// is appended behind the torn half of record 2, and the reopen drops it with the torn tail.
    #[test]
    fn a_torn_tail_is_rewritten_away_by_the_first_write_after_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.history");
        let s = HistoryStore::open(&path, 8).unwrap();
        s.enqueue(vec![rec(1, 1)]);
        s.drain().unwrap();
        s.enqueue(vec![rec(2, 2)]);
        s.drain().unwrap();
        drop(s);
        let whole = std::fs::read(&path).unwrap();
        std::fs::write(&path, &whole[..whole.len() - 5]).unwrap();

        let s = HistoryStore::open(&path, 8).unwrap();
        s.enqueue(vec![rec(3, 3)]);
        s.drain().unwrap();
        drop(s);
        let s = HistoryStore::open(&path, 8).unwrap();
        let hseqs: Vec<u64> = s.records().iter().map(|r| r.hseq).collect();
        assert_eq!(hseqs, [1, 3], "the record written after the torn tail was lost behind it");
    }

    /// **AMENDED 2, F3.** Two drains racing — two committing threads can each run the automatic
    /// checkpoint — write each record once, because the hook mutex is held across the write.
    ///
    /// Deterministic rather than hoped-for: each append waits (up to 100 ms) for a second append to
    /// begin. Under the mutex none can, so each round writes once and waits out the timeout. Under
    /// the mutant — the mutex released before the write — both threads reach the write with the
    /// same queue, meet, and both write it: the counters and the file show the record twice.
    #[test]
    fn two_racing_drains_write_each_record_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Rendezvous(AtomicUsize);
        impl FileOps for Rendezvous {
            fn write(&self, p: &std::path::Path, b: &[u8]) -> std::io::Result<()> {
                OsFileOps.write(p, b)
            }
            fn append(&self, p: &std::path::Path, b: &[u8]) -> std::io::Result<()> {
                let me = self.0.fetch_add(1, Ordering::SeqCst);
                let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
                while self.0.load(Ordering::SeqCst) < (me | 1) + 1
                    && std::time::Instant::now() < deadline
                {
                    std::thread::yield_now();
                }
                OsFileOps.append(p, b)
            }
            fn sync_file(&self, p: &std::path::Path) -> std::io::Result<()> {
                OsFileOps.sync_file(p)
            }
            fn rename(&self, f: &std::path::Path, t: &std::path::Path) -> std::io::Result<()> {
                OsFileOps.rename(f, t)
            }
            fn sync_dir(&self, d: &std::path::Path) -> std::io::Result<()> {
                OsFileOps.sync_dir(d)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.history");
        let s = HistoryStore::open_with_ops(&path, 64, Box::new(Rendezvous(AtomicUsize::new(0))))
            .unwrap();
        s.enqueue(vec![rec(1, 1)]);
        s.drain().unwrap();
        for round in 0..5u64 {
            s.enqueue(vec![rec(2 + round, 2 + round)]);
            std::thread::scope(|t| {
                t.spawn(|| s.drain().unwrap());
                t.spawn(|| s.drain().unwrap());
            });
        }
        let c = s.counters();
        assert_eq!((c.rewrites, c.appends), (1, 5), "a queued record was written by both drains");
        let whole = std::fs::read(&path).unwrap();
        let framed: usize = s.records().iter().map(|r| RECORD_FRAME + r.body.len()).sum();
        assert_eq!(whole.len(), IMAGE_HEADER + 4 + framed, "the file holds a record twice");
    }

    /// **AMENDED 2, F9.** The open re-queues by membership: a committed record the file lacks is
    /// queued even when a later one is there.
    #[test]
    fn a_missing_record_older_than_the_tail_is_queued_by_membership() {
        let dir = tempfile::tempdir().unwrap();
        let s = open_in(&dir, 8);
        s.enqueue(vec![rec(1, 1), rec(3, 3)]);
        s.drain().unwrap();
        s.enqueue(vec![rec(1, 1), rec(2, 2), rec(3, 3)]);
        s.drain().unwrap();
        drop(s);
        let s = open_in(&dir, 8);
        let hseqs: Vec<u64> = s.records().iter().map(|r| r.hseq).collect();
        assert_eq!(hseqs, [1, 2, 3]);
    }

    /// **The hook's position and its failure** (review of 816321d, finding 1): the store is written
    /// BEFORE the log is truncated, and a failed write refuses the truncation, so the log keeps the
    /// only other copy of every record the store does not hold.
    ///
    /// Mutant: `truncate` moved before the hook — the failed checkpoint leaves no history in the log.
    /// Mutant: the hook's error ignored — the failing checkpoint returns `Ok`.
    #[test]
    fn a_failed_hook_write_refuses_the_truncation_and_keeps_the_history_in_the_log() {
        use crate::buffer::buffer_pool::BufferPoolManager;
        use crate::storage::disk_manager::DiskManager;
        use crate::wal::log::{RecKind, WalManager};
        use crate::wal::txn::TxnManager;
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("h.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.path().join("h.wal")).unwrap());
        let txn = TxnManager::new(wal.clone(), bp);
        let faulty = Arc::new(Faulty::new());
        struct Shared(Arc<Faulty>);
        impl FileOps for Shared {
            fn write(&self, p: &std::path::Path, b: &[u8]) -> std::io::Result<()> { self.0.write(p, b) }
            fn append(&self, p: &std::path::Path, b: &[u8]) -> std::io::Result<()> { self.0.append(p, b) }
            fn sync_file(&self, p: &std::path::Path) -> std::io::Result<()> { self.0.sync_file(p) }
            fn rename(&self, f: &std::path::Path, t: &std::path::Path) -> std::io::Result<()> { self.0.rename(f, t) }
            fn sync_dir(&self, d: &std::path::Path) -> std::io::Result<()> { self.0.sync_dir(d) }
        }
        let store =
            HistoryStore::open_with_ops(dir.path().join("h.history"), 8, Box::new(Shared(faulty.clone())))
                .unwrap();
        txn.attach_history_store(store.clone()).unwrap();

        let t = txn.begin().unwrap();
        txn.bind_history(t, rec(1, 1)).unwrap();
        txn.commit(t).unwrap();
        let parts_in_log = || {
            let (mut lsn, end) = (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst));
            let mut n = 0;
            while lsn < end {
                let (r, next) = wal.read_record(lsn).unwrap();
                n += usize::from(matches!(r.kind, RecKind::RevertHistory { .. }));
                lsn = next;
            }
            n
        };
        assert_eq!(parts_in_log(), 1, "fixture: the committed record is in the log");

        // The first write of the process is a rewrite; fail its rename.
        faulty.fail_rename.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(txn.checkpoint().is_err(), "a checkpoint whose history write failed reported success");
        assert_eq!(parts_in_log(), 1, "the log was truncated past history the store never took");

        txn.checkpoint().unwrap();
        assert_eq!(parts_in_log(), 0, "anti-vacuity: a checkpoint that wrote the store truncates");
        assert_eq!(store.records(), vec![rec(1, 1)]);
    }

    /// **AMENDED 3, item 3.** A pruned record the log still holds is not brought back by the open's
    /// catch-up: the prune floor is in the image, and membership applies only at or above it.
    ///
    /// Mutant: `enqueue` ignores the floor — the pruned records are queued, rewritten, and back.
    #[test]
    fn a_pruned_record_is_not_resurrected_by_the_catch_up() {
        let dir = tempfile::tempdir().unwrap();
        let s = open_in(&dir, 2);
        for h in 1..=4 {
            s.enqueue(vec![rec(h, h)]);
            s.drain().unwrap();
        }
        let kept: Vec<u64> = s.records().iter().map(|r| r.hseq).collect();
        assert_eq!(kept, [3, 4], "fixture: W = 2 keeps the newest two publishes");
        drop(s);
        // Reopened with a wider window, so no prune can hide a resurrection by pruning it again.
        let s = open_in(&dir, 8);
        // The log outlived the window: the catch-up offers every committed record again.
        s.enqueue((1..=4).map(|h| rec(h, h)).collect());
        s.drain().unwrap();
        drop(s);
        let s = open_in(&dir, 8);
        let kept: Vec<u64> = s.records().iter().map(|r| r.hseq).collect();
        assert_eq!(kept, [3, 4], "a pruned record came back from the log");
    }
}
