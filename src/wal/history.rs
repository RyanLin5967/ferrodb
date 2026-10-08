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
//! image  = MAGIC u32 | VERSION u8 | incarnation u64 | floor u64 | count u32 | record{count} | crc32 u32 (over all before it)
//! record = hseq u64 | ordinal u64 | commit_lsn u64 | len u32 | body[len] | crc32 u32 (over hseq..body)
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
//! **The prune floor is persisted** (AMENDED 3, item 3): the image names the lowest `hseq` its last
//! prune kept, and the open's catch-up queues a record only if it is absent AND at or above that
//! floor. The log can outlive `W` merges (a pin, owed releases, a kept truncation), and without the
//! floor its copies of pruned records would come back at the next open, growing the file with the log
//! and putting merges past the window back in reach. The window, the counters and C1 are all taken
//! by `hseq`, never by a record's position in the file.
//!
//! # What a write costs, amortised (AMENDED 3, item 9)
//!
//! A publish adds no fsync of its own. The FIRST write after an open, or after a failed write, is a
//! full rewrite — O(W) bytes and two fsyncs (the temporary, then the directory) — and it can land on
//! a commit path, through the bounded drain below, once. Every later write is one append (one fsync)
//! per checkpoint, or per [`QUEUE_DRAIN_BYTES`] queued, plus one O(W) rewrite per `max(W/8, 1)`
//! publishes when a prune is due. The queue is filled only after `flush_up_to(commit_lsn)`
//! (`TxnManager::commit`), so no drain ever stores the history of a merge that did not commit.
//!
//! # What this file does not know
//!
//! Bodies are opaque here. What a record MEANS — a publish's ops and captures, a REVERT's marker —
//! is `agent_sql::revert_store`'s, which is the only reader of a body.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
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
/// 2 since AMENDED 3 (the incarnation, the floor, then the Commit LSN). No build wrote version 1:
/// the branch that had it was never built.
const IMAGE_VERSION: u8 = 2;
/// `MAGIC | VERSION | incarnation | floor | count`.
const IMAGE_HEADER: usize = 4 + 1 + 8 + 8 + 4;
/// An image's bytes beyond its records: the header and the trailing checksum.
pub const IMAGE_OVERHEAD: usize = IMAGE_HEADER + 4;
/// `hseq | ordinal | commit_lsn | len` in front of a body, `crc32` behind it.
pub const RECORD_FRAME: usize = 8 + 8 + 8 + 4 + 4;

/// One record of REVERT's history, as the store holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRecord {
    /// The history sequence: strictly increasing across every record ever written, publishes and
    /// markers alike. The store's idempotency key.
    pub hseq: u64,
    /// The publish ordinal for a publish, strictly increasing across publishes; 0 for anything
    /// else (a REVERT's marker). Retention counts publishes by it.
    pub ordinal: u64,
    /// **The LSN of its transaction's `Commit` record** (AMENDED 3, item 2): what an install or a
    /// restore compares with the image's `end_lsn`, so a store never holds history for rows the
    /// database beside it does not have. Stamped by `TxnManager::commit` once the `Commit` is durable
    /// and by the open's catch-up from the `Commit` it found; whatever a caller puts here before
    /// `commit` is overwritten.
    pub commit_lsn: u64,
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
        out.extend_from_slice(&self.commit_lsn.to_be_bytes());
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
    /// arena's `PersistState`, and after `att` and `release_retry` when a checkpoint takes it
    /// (AMENDED 3, item 5; the whole order is at `TxnManager::history`). Only this type's methods
    /// take it, and none returns the guard.
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
    /// **The prune floor** (AMENDED 3, item 3): the lowest `hseq` the last prune kept, persisted in
    /// the image. A committed record below it is one the window has already dropped, so the open's
    /// catch-up must not bring it back from a log that outlived `W` merges.
    floor: u64,
    /// **The database incarnation this history belongs to** (AMENDED 3, item 10a), persisted in the
    /// image, and declared into the log after every truncation (`TxnManager::declare_history`). 0
    /// until one is known: drawn at the first declaration, or adopted from the log's at the open.
    incarnation: u64,
    /// The queued bytes past which the commit path next drains ([`HistoryStore::drain_if_due`]):
    /// [`QUEUE_DRAIN_BYTES`], or double the queue after a failed attempt.
    next_drain_at: usize,
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
    /// `<db>.history`, the file `open_recovered` opens for the database at `db_path`.
    pub fn path_for_database(db_path: &Path) -> PathBuf {
        let mut p = db_path.as_os_str().to_os_string();
        p.push(".history");
        PathBuf::from(p)
    }

    /// The history file beside the log at `wal_path`: `<db>.history` for `<db>.wal`, the pair
    /// `open_recovered` opens (AMENDED 3, item 10). `None` for a log not named that way, or whose
    /// name is not UTF-8 (stated: such a log's history, if any, is not moved aside).
    pub fn path_for_wal(wal_path: &Path) -> Option<PathBuf> {
        let name = wal_path.file_name()?.to_str()?;
        let db = name.strip_suffix(".wal")?;
        Some(wal_path.with_file_name(format!("{db}.history")))
    }

    /// **What `wal::recovery::open_recovered` calls, before `recover`** (AMENDED 3, item 4):
    /// `<db>.history` with the window from `FERRODB_REVERT_RETENTION_MERGES`.
    ///
    /// **Refuses a history file beside a database file that does not exist yet:** it was left by
    /// another database at that path, and reading it would give a new, empty database the merge
    /// history of an old one — history without its rows.
    /// `database_existed` is whether the database file was there BEFORE this open created it, which
    /// only the caller can know: the open creates the file before it attaches the store.
    pub fn open_for_database(
        db_path: &Path,
        database_existed: bool,
    ) -> Result<Arc<HistoryStore>, FerroError> {
        let history = HistoryStore::path_for_database(db_path);
        if !database_existed && history.exists() {
            return Err(FerroError::Internal(format!(
                "{} exists but the database {} does not: it is another database's REVERT \
                 history, and a new database must not inherit it. Move it away to create the \
                 database here",
                history.display(),
                db_path.display()
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
        let ((incarnation, floor, window), read) = match std::fs::read(&path) {
            Ok(bytes) => (load(&bytes)?, bytes.len() as u64),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ((0, 0, BTreeMap::new()), 0),
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
            floor,
            incarnation,
            next_drain_at: QUEUE_DRAIN_BYTES,
            counters: HistoryCounters { bytes_read_at_open: read, ..HistoryCounters::default() },
        };
        Ok(Arc::new(HistoryStore { path, retention, ops, state: Mutex::new(state) }))
    }

    /// The file this store writes.
    pub fn path(&self) -> &Path {
        &self.path
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

    /// Queue committed records for the next drain, skipping any whose `hseq` is already held or is
    /// below the prune floor.
    ///
    /// By MEMBERSHIP, not by "above the tail" (AMENDED 2, F9): a committed record whose `hseq` is
    /// absent is queued wherever it falls at or above the floor, so a record that missed the file for
    /// any reason is recovered from the log rather than skipped for being older than a later one.
    /// Below the floor it was pruned, and stays pruned (AMENDED 3, item 3). Skipping what is held is
    /// what makes the open's catch-up idempotent.
    pub fn enqueue(&self, records: Vec<HistoryRecord>) {
        let mut s = self.state.lock().unwrap();
        for r in records {
            if r.hseq >= s.floor && !s.holds(r.hseq) {
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
        self.drain_locked(&mut s)
    }

    /// [`HistoryStore::drain`]'s body, with F3 already held by the caller.
    fn drain_locked(&self, s: &mut StoreState) -> Result<(), FerroError> {
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
            let floor = if prune { cut } else { s.floor };
            let incarnation = known_incarnation(s);
            encode_image(&kept, incarnation, floor).and_then(|image| {
                replace_atomically(&*self.ops, &self.path, &image)
                    .map_err(|e| FerroError::Io(format!("writing {}: {e}", self.path.display())))
            })
            .map(|()| Some((kept, floor)))
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
            Ok(Some((kept, floor))) => {
                s.window = kept.into_iter().map(|r| (r.hseq, r)).collect();
                s.floor = floor;
                s.next_drain_at = QUEUE_DRAIN_BYTES;
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
                s.next_drain_at = QUEUE_DRAIN_BYTES;
                s.publishes_since_prune = since;
                s.counters.appends += 1;
                s.counters.drains += 1;
                Ok(())
            }
        }
    }

    // ---- AMENDED 3, item 10a: the database incarnation --------------------------------------

    /// **The incarnation this history belongs to, drawn now if none is known yet** (AMENDED 3, item
    /// 10a). Drawn in memory and written with the next image; `TxnManager::declare_history` puts it
    /// in the log after every truncation, so an open reads it back from there even if no image was
    /// ever written.
    pub fn incarnation(&self) -> u64 {
        known_incarnation(&mut self.state.lock().unwrap())
    }

    /// **The open's check** (AMENDED 3, item 10a): the log declares `declared` as its database's
    /// incarnation. A store that knows none adopts it; a store that knows another is REFUSED: it is
    /// another database's REVERT history, and its merge ids would be revertible against these rows.
    pub(crate) fn adopt_or_check(&self, declared: u64) -> Result<(), FerroError> {
        let mut s = self.state.lock().unwrap();
        match s.incarnation {
            0 => {
                s.incarnation = declared;
                Ok(())
            }
            held if held == declared => Ok(()),
            held => Err(FerroError::Internal(format!(
                "{} is the REVERT history of database incarnation {held:#018x}, and this database's log \
                 declares incarnation {declared:#018x}: it is another database's history, and reading it \
                 as this one's would let a REVERT invert rows it never wrote. Move it away to open",
                self.path.display()
            ))),
        }
    }

    // ---- AMENDED 3, item 2: snapshot capture, install and restore ------------------------------

    /// **The bytes a snapshot or a base backup ships** (AMENDED 3, item 2). Under F3, every queued
    /// record is first made durable, exactly as the checkpoint hook's drain does, and then the durable
    /// window is encoded as one image with its prune floor. One hold across both, so a record pushed
    /// meanwhile waits for the next drain rather than landing between the drain and the copy.
    ///
    /// ⚠ **Stated, not covered:** a transaction whose `Commit` is below the image's `end_lsn` but that
    /// has not yet PUSHED (it sits between its `Commit` flush and the push, still in `att`) is in
    /// neither this copy nor, when its history parts precede the redo window's `start_lsn`, the
    /// window the install re-queues from. Its rows ship and its history does not: REVERT of it is
    /// then refused on the receiver (the safe direction). Closing it needs the capture to wait for
    /// such transactions, which is not decided.
    pub fn capture(&self) -> Result<Vec<u8>, FerroError> {
        let mut s = self.state.lock().unwrap();
        self.drain_locked(&mut s)?;
        let window: Vec<HistoryRecord> = s.window.values().cloned().collect();
        let incarnation = known_incarnation(&mut s);
        encode_image(&window, incarnation, s.floor)
    }

    /// **A captured image cut to the history of exactly the rows an image consistent at `end_lsn`
    /// holds** (AMENDED 3, item 2): the image's incarnation and prune floor, and the records whose
    /// `Commit` is below `end_lsn`. A record committed later is history of rows the receiver never
    /// gets, and a REVERT of it would invert writes it does not have. Empty `bytes` (a sender with no
    /// store) is an empty history with no incarnation.
    pub fn records_through(
        bytes: &[u8],
        end_lsn: u64,
    ) -> Result<(u64, u64, Vec<HistoryRecord>), FerroError> {
        if bytes.is_empty() {
            return Ok((0, 0, Vec::new()));
        }
        let (incarnation, floor, all) = load(bytes)?;
        Ok((incarnation, floor, all.into_values().filter(|r| r.commit_lsn < end_lsn).collect()))
    }

    /// **Replace this store, file and memory, with `records`** (AMENDED 3, item 2), as a snapshot
    /// install replaces the database under it. Records below `floor` are dropped and a repeated `hseq`
    /// keeps its first copy, as the loader does. The queue is EMPTIED: it held the replaced
    /// database's history. One full rewrite, under F3; on failure the store is unchanged, and the
    /// install's marker (`consensus::snapshot::install_marker`) refuses the node until it is re-seeded.
    /// The store takes the sender's incarnation (AMENDED 3, item 10a): the database is the sender's.
    ///
    /// Every installed record's Commit LSN becomes 0, "before this database's log": the LSNs it
    /// carried are the sender's, and a later capture from this node compares against ITS `end_lsn`
    /// (review of `a71d3ed`, F10). An image from a sender with no incarnation gets a fresh one here,
    /// so no image this build writes carries 0 (F5).
    pub fn install(&self, incarnation: u64, floor: u64, records: Vec<HistoryRecord>) -> Result<(), FerroError> {
        let mut s = self.state.lock().unwrap();
        let incarnation = if incarnation == 0 { fresh_incarnation() } else { incarnation };
        let mut kept: BTreeMap<u64, HistoryRecord> = BTreeMap::new();
        for mut r in records.into_iter().filter(|r| r.hseq >= floor) {
            r.commit_lsn = 0;
            kept.entry(r.hseq).or_insert(r);
        }
        let list: Vec<HistoryRecord> = kept.values().cloned().collect();
        let image = encode_image(&list, incarnation, floor)?;
        replace_atomically(&*self.ops, &self.path, &image)
            .map_err(|e| FerroError::Io(format!("writing {}: {e}", self.path.display())))?;
        let held = kept.values().filter(|r| r.ordinal > 0).count() as u64;
        s.window = kept;
        s.floor = floor;
        s.incarnation = incarnation;
        s.queue.clear();
        s.queued_bytes = 0;
        s.image_written = true;
        s.publishes_since_prune = held.saturating_sub(self.retention);
        s.counters.rewrites += 1;
        Ok(())
    }

    /// **Write `records` as the whole store at `path`**, for a restore with no store open (AMENDED 3,
    /// item 2): one image, written through [`replace_atomically`], each record's Commit LSN set to 0
    /// as [`HistoryStore::install`] sets it, and a fresh incarnation drawn when `incarnation` is 0.
    /// Returns the incarnation written, which the restore declares into the restored log.
    pub fn write_image(
        path: &Path,
        incarnation: u64,
        floor: u64,
        records: &[HistoryRecord],
    ) -> Result<u64, FerroError> {
        let incarnation = if incarnation == 0 { fresh_incarnation() } else { incarnation };
        let records: Vec<HistoryRecord> =
            records.iter().cloned().map(|r| HistoryRecord { commit_lsn: 0, ..r }).collect();
        let image = encode_image(&records, incarnation, floor)?;
        replace_atomically(&OsFileOps, path, &image)
            .map_err(|e| FerroError::Io(format!("writing {}: {e}", path.display())))?;
        Ok(incarnation)
    }

    /// **The commit path's bounded drain, backing off while writes fail** (AMENDED 2, F7; review of
    /// `a71d3ed`, F6). A drain when more than [`QUEUE_DRAIN_BYTES`] are queued. After a FAILED one,
    /// the next attempt waits until the queue has doubled, so a store that keeps failing costs
    /// O(queued bytes) in re-encoding over all attempts, not O(queued bytes) per commit. A success
    /// resets the threshold. The failure itself is not the committing transaction's: the checkpoint
    /// hook still refuses its truncation until a write succeeds.
    pub fn drain_if_due(&self) -> Result<(), FerroError> {
        let mut s = self.state.lock().unwrap();
        let due = s.next_drain_at.max(QUEUE_DRAIN_BYTES);
        if s.queued_bytes <= due {
            return Ok(());
        }
        let result = self.drain_locked(&mut s);
        // A success reset the threshold inside the drain, as every successful drain does.
        if result.is_err() {
            s.next_drain_at = s.queued_bytes.saturating_mul(2);
        }
        result
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

fn encode_image(records: &[HistoryRecord], incarnation: u64, floor: u64) -> Result<Vec<u8>, FerroError> {
    let count = u32::try_from(records.len()).map_err(|_| FerroError::Unrepresentable {
        what: "a REVERT history image's record count".to_string(),
        len: records.len(),
        limit: u32::MAX as usize,
    })?;
    let mut out = Vec::with_capacity(IMAGE_HEADER + 4);
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.push(IMAGE_VERSION);
    out.extend_from_slice(&incarnation.to_be_bytes());
    out.extend_from_slice(&floor.to_be_bytes());
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
    let commit_lsn = take_u64(rest, &mut i)?;
    let len = take_u32(rest, &mut i)? as usize;
    let Some(total) = len.checked_add(RECORD_FRAME).filter(|t| *t <= rest.len()) else {
        return Ok(None);
    };
    let stored = u32::from_be_bytes(rest[total - 4..total].try_into().unwrap());
    if crc32(&rest[..total - 4]) != stored {
        return Err(corrupt(format!("the record at byte {at} fails its checksum")));
    }
    let body = rest[RECORD_FRAME - 4..RECORD_FRAME - 4 + len].to_vec();
    Ok(Some((HistoryRecord { hseq, ordinal, commit_lsn, body }, at + total)))
}

/// Load a whole `<db>.history`: the image, then every intact tail record behind it — the arena's
/// rule (`ArenaPageStore::replay_tail`): a torn LAST record is dropped, a checksum failure with
/// more bytes behind it is corruption and refused. Returns the image's incarnation, its prune floor
/// and the records.
fn load(bytes: &[u8]) -> Result<(u64, u64, BTreeMap<u64, HistoryRecord>), FerroError> {
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
    let incarnation = take_u64(bytes, &mut at)?;
    let floor = take_u64(bytes, &mut at)?;
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
                // The length field is the frame's last four bytes before the body.
                let len = if total >= RECORD_FRAME - 4 {
                    let l = at + RECORD_FRAME - 8;
                    u32::from_be_bytes(bytes[l..l + 4].try_into().unwrap()) as usize
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
    Ok((incarnation, floor, by_hseq))
}

/// `s`'s incarnation, drawn now if none is known: before the first image this process writes, and
/// before the first declaration, so the file and the log never disagree about a drawn one.
fn known_incarnation(s: &mut StoreState) -> u64 {
    if s.incarnation == 0 {
        s.incarnation = fresh_incarnation();
    }
    s.incarnation
}

/// A random, non-zero database incarnation (AMENDED 3, item 10a), drawn as the runtime draws its
/// merge-id nonce: std's OS-seeded `RandomState`, a per-process counter, the pid and the clock.
fn fresh_incarnation() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static DRAWN: AtomicU64 = AtomicU64::new(0);
    loop {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(DRAWN.fetch_add(1, Ordering::Relaxed));
        h.write_u32(std::process::id());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        h.write_u128(now);
        let n = h.finish();
        if n != 0 {
            return n;
        }
    }
}

/// **The incarnation a log declares** (AMENDED 3, item 10a): its last `IncarnationDecl` record,
/// which `TxnManager::declare_history` writes after every truncation. `None` for a log with no
/// declaration (fresh, or never truncated since a store was attached).
pub(crate) fn declared_incarnation(records: &[crate::wal::log::LogRecord]) -> Option<u64> {
    use crate::wal::log::RecKind;
    records.iter().rev().find_map(|r| match r.kind {
        RecKind::IncarnationDecl { incarnation } => Some(incarnation),
        _ => None,
    })
}

/// **The history records of the committed transactions among `records`** (AMENDED 3, item 2): each
/// transaction's `RevertHistory` parts, reassembled, stamped with the LSN of its `Commit`. A
/// transaction with no `Commit` among them contributes nothing. Shared by the open's catch-up
/// (`wal::recovery::recover`) and a snapshot install's re-queue of its redo window, so the two
/// cannot disagree about what "committed" means.
///
/// **A record whose first part among `records` is not part 0 is DROPPED, not refused** (reviews of
/// `a71d3ed` F4/F9 and of `0d3fbb9` N1): its leading parts precede `records`, so it cannot be
/// reassembled here. A snapshot's redo window starts wherever the sender's backup began, and a log
/// whose truncation raced a transaction (the D253 hazard, closed by its fence) can hold the tail of
/// one. Refusing would fail every open and every install for ever. A transaction whose `Begin`
/// precedes `records` keeps its history when every part is present: the parts are written at its
/// commit (item 6), long after its `Begin`. The second value counts the records dropped.
pub(crate) fn committed_in(
    records: &[crate::wal::log::LogRecord],
) -> Result<(Vec<HistoryRecord>, usize), FerroError> {
    use crate::wal::log::RecKind;
    use std::collections::{HashMap, HashSet};
    let commit_lsns: HashMap<u64, u64> =
        records.iter().filter(|r| matches!(r.kind, RecKind::Commit)).map(|r| (r.txn_id, r.lsn)).collect();
    // (txn, hseq) of every record seen so far, and of those whose first part here was not 0.
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    let mut headless: HashSet<(u64, u64)> = HashSet::new();
    let mut parts: Vec<(u64, u64, u64, u64, u32, bool, Vec<u8>)> = Vec::new();
    for r in records {
        let (RecKind::RevertHistory { hseq, ordinal, part, last, bytes }, Some(commit_lsn)) =
            (&r.kind, commit_lsns.get(&r.txn_id))
        else {
            continue;
        };
        let key = (r.txn_id, *hseq);
        if seen.insert(key) && *part != 0 {
            headless.insert(key);
        }
        if !headless.contains(&key) {
            parts.push((r.txn_id, *commit_lsn, *hseq, *ordinal, *part, *last, bytes.clone()));
        }
    }
    if parts.is_empty() {
        return Ok((Vec::new(), headless.len()));
    }
    Ok((assemble(parts)?, headless.len()))
}

/// **Reassemble tag-12 WAL parts into records**, for the open's catch-up.
///
/// A record larger than one WAL part is split across parts `0, 1, ..` of its transaction, the last
/// marked. `parts` is every `(txn, commit_lsn, hseq, ordinal, part, last, bytes)` of the transactions
/// that COMMITTED, with the LSN of each one's `Commit`, in log order; parts of different records may
/// interleave, and are told apart by `(txn, hseq)`. A record whose parts do not run `0, 1, ..` to one marked last is refused. The
/// result is in `hseq` order, which is what [`HistoryStore::enqueue`] keys on.
pub fn assemble(
    parts: Vec<(u64, u64, u64, u64, u32, bool, Vec<u8>)>,
) -> Result<Vec<HistoryRecord>, FerroError> {
    // (txn, record so far, the last part appended to it)
    let mut open: Vec<(u64, HistoryRecord, u32)> = Vec::new();
    let mut done: Vec<HistoryRecord> = Vec::new();
    for (txn, commit_lsn, hseq, ordinal, part, last, bytes) in parts {
        let at = match open.iter().position(|(t, r, _)| *t == txn && r.hseq == hseq) {
            None => {
                if part != 0 {
                    return Err(corrupt(format!(
                        "txn {txn}'s history record {hseq} starts at part {part}, not 0"
                    )));
                }
                open.push((txn, HistoryRecord { hseq, ordinal, commit_lsn, body: bytes }, 0));
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
        HistoryRecord { hseq, ordinal, commit_lsn: 100 + hseq, body: format!("record {hseq}").into_bytes() }
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
            (5, 90, 1, 1, 0, false, b"ab".to_vec()),
            (5, 90, 1, 1, 1, true, b"cd".to_vec()),
            (6, 95, 2, 0, 0, true, b"m".to_vec()),
        ])
        .unwrap();
        assert_eq!(whole[0], HistoryRecord { hseq: 1, ordinal: 1, commit_lsn: 90, body: b"abcd".to_vec() });
        assert_eq!(whole[1].hseq, 2);
        assert!(assemble(vec![(5, 90, 1, 1, 1, true, b"cd".to_vec())]).is_err());
        assert!(assemble(vec![(5, 90, 1, 1, 0, false, b"ab".to_vec())]).is_err());
    }

    /// **AMENDED 3, item 6 (A8): a record split over several WAL parts is keyed `(txn, hseq, part)`**,
    /// so parts of two records that interleave in the log reassemble into two whole records, and
    /// membership and dedup only ever see whole records. Each keeps its own transaction's Commit LSN.
    ///
    /// Mutant: a part appended to the most recently opened record rather than the one its
    /// `(txn, hseq)` names — the two bodies are spliced.
    #[test]
    fn interleaved_parts_of_two_records_reassemble_into_two() {
        let whole = assemble(vec![
            (5, 90, 1, 1, 0, false, b"ab".to_vec()),
            (6, 95, 2, 2, 0, false, b"xy".to_vec()),
            (5, 90, 1, 1, 1, true, b"cd".to_vec()),
            (6, 95, 2, 2, 1, true, b"z".to_vec()),
        ])
        .unwrap();
        assert_eq!(
            whole,
            vec![
                HistoryRecord { hseq: 1, ordinal: 1, commit_lsn: 90, body: b"abcd".to_vec() },
                HistoryRecord { hseq: 2, ordinal: 2, commit_lsn: 95, body: b"xyz".to_vec() },
            ]
        );
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
        // Read from the log while it holds the `Commit`: the record must carry that LSN (item 2).
        let commit_lsn = {
            let (mut lsn, end) = (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst));
            let mut at = None;
            while lsn < end {
                let (r, next) = wal.read_record(lsn).unwrap();
                if r.txn_id == t && matches!(r.kind, RecKind::Commit) {
                    at = Some(r.lsn);
                }
                lsn = next;
            }
            at.expect("fixture: the Commit is in the log")
        };

        // The first write of the process is a rewrite; fail its rename.
        faulty.fail_rename.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(txn.checkpoint().is_err(), "a checkpoint whose history write failed reported success");
        assert_eq!(parts_in_log(), 1, "the log was truncated past history the store never took");

        txn.checkpoint().unwrap();
        assert_eq!(parts_in_log(), 0, "anti-vacuity: a checkpoint that wrote the store truncates");
        assert_eq!(store.records(), vec![HistoryRecord { commit_lsn, ..rec(1, 1) }]);
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

    /// **Review of `a71d3ed`, F6.** While every write fails, the commit path's drain backs off: its
    /// attempts grow with the log of the queue, not with the commits.
    ///
    /// Mutant: `drain_if_due` keeps its threshold at `QUEUE_DRAIN_BYTES` after a failure — one
    /// attempt, re-encoding the whole queue, per commit past it.
    #[test]
    fn a_failing_store_is_retried_on_the_commit_path_only_as_the_queue_doubles() {
        struct RenameFails;
        impl FileOps for RenameFails {
            fn write(&self, p: &std::path::Path, b: &[u8]) -> std::io::Result<()> { OsFileOps.write(p, b) }
            fn append(&self, p: &std::path::Path, b: &[u8]) -> std::io::Result<()> { OsFileOps.append(p, b) }
            fn sync_file(&self, p: &std::path::Path) -> std::io::Result<()> { OsFileOps.sync_file(p) }
            fn rename(&self, _: &std::path::Path, _: &std::path::Path) -> std::io::Result<()> {
                Err(std::io::Error::other("injected: every rename fails"))
            }
            fn sync_dir(&self, d: &std::path::Path) -> std::io::Result<()> { OsFileOps.sync_dir(d) }
        }
        let dir = tempfile::tempdir().unwrap();
        let s = HistoryStore::open_with_ops(dir.path().join("f.history"), 8, Box::new(RenameFails)).unwrap();
        let body = QUEUE_DRAIN_BYTES / 8;
        for h in 1..=128u64 {
            s.enqueue(vec![HistoryRecord { hseq: h, ordinal: h, commit_lsn: 0, body: vec![0; body] }]);
            let _ = s.drain_if_due();
        }
        let failed = s.counters().failed_drains;
        assert!(failed >= 1, "anti-vacuity: the queue never passed the threshold");
        assert!(failed <= 6, "{failed} failed drains over 128 commits: the commit path retried at every commit");
    }

    /// **Review of `a71d3ed`, F4 and F9.** A committed transaction whose `Begin` is not among the
    /// records (a snapshot's redo window that starts inside it, or a log whose truncation raced it)
    /// is dropped and counted, not refused: refusing would fail every open or install for ever.
    ///
    /// Mutant: the `Begin` filter removed — `assemble` refuses part 1 without part 0.
    #[test]
    fn a_transaction_that_began_before_the_records_is_dropped_not_refused() {
        use crate::wal::log::{LogRecord, RecKind};
        let rec = |lsn: u64, txn_id: u64, kind: RecKind| LogRecord { lsn, prev_lsn: 0, txn_id, kind };
        let part = |hseq: u64, part: u32| RecKind::RevertHistory { hseq, ordinal: hseq, part, last: true, bytes: vec![1] };
        let records = vec![
            // Txn 5's Begin and part 0 precede these records.
            rec(10, 5, part(1, 1)),
            rec(20, 5, RecKind::Commit),
            rec(30, 6, RecKind::Begin),
            rec(40, 6, part(2, 0)),
            rec(50, 6, RecKind::Commit),
        ];
        let (history, dropped) =
            committed_in(&records).expect("records starting inside a transaction were refused");
        assert_eq!(dropped, 1, "the transaction that began before the records was not counted");
        let got: Vec<(u64, u64)> = history.iter().map(|r| (r.hseq, r.commit_lsn)).collect();
        assert_eq!(got, [(2, 50)]);
    }

    /// **Review of `0d3fbb9`, N1.** A committed transaction whose `Begin` precedes the records but
    /// whose parts are all among them keeps its history: the parts are written at its commit (item
    /// 6), so a snapshot window that starts after its `Begin` still carries the whole record.
    ///
    /// Mutant: records dropped for a missing `Begin` (the `0d3fbb9` rule) — record 3 is lost.
    #[test]
    fn a_transaction_that_began_before_the_records_keeps_a_whole_record() {
        use crate::wal::log::{LogRecord, RecKind};
        let records = vec![
            LogRecord { lsn: 10, prev_lsn: 0, txn_id: 7, kind: RecKind::RevertHistory { hseq: 3, ordinal: 3, part: 0, last: false, bytes: vec![1] } },
            LogRecord { lsn: 20, prev_lsn: 0, txn_id: 7, kind: RecKind::RevertHistory { hseq: 3, ordinal: 3, part: 1, last: true, bytes: vec![2] } },
            LogRecord { lsn: 30, prev_lsn: 0, txn_id: 7, kind: RecKind::Commit },
        ];
        let (history, dropped) = committed_in(&records).unwrap();
        assert_eq!(dropped, 0);
        assert_eq!(history, vec![HistoryRecord { hseq: 3, ordinal: 3, commit_lsn: 30, body: vec![1, 2] }]);
    }
}
