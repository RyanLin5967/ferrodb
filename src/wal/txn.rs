use std::{collections::{HashMap, HashSet}, path::{Path, PathBuf}, sync::{Arc, Mutex, MutexGuard, atomic::{AtomicU32, AtomicU64, Ordering}}};

use crate::catalog::column::{DataType, Value};
use crate::storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager};
use crate::cluster::GrantedCounter;
use crate::storage::atomic_file::{FileOps, OsFileOps};
use crate::provenance::RunEntity;
use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{heap_page::Page, tuple::{Tuple, VersionHeader}}, wal::{free_intent::{self, FreeIntent}, log::{DdlOp, RecKind, WalManager, WalPin}}};

/// Commits between automatic checkpoints.
///
/// Overridable by `FERRODB_CHECKPOINT_INTERVAL` **so that tests can make truncation constant
/// instead of rare.** That is not a convenience knob: three separate features shipped with a test
/// that passed only because it stayed under this threshold, and past it each one failed for real —
/// a base backup dead on arrival, a table's schema erased from the log, a live consumer whose
/// cursor had been truncated away. A condition that almost never happens is a condition nothing is
/// tested against, so this exists to let a test set it to 1 and make every commit truncate.
///
/// Read once, because a value that changes underneath a running database would make checkpoint
/// timing depend on when the environment was last read rather than on how much work has happened.
fn checkpoint_interval() -> u64 {
    use std::sync::OnceLock;
    static INTERVAL: OnceLock<u64> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        std::env::var("FERRODB_CHECKPOINT_INTERVAL")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            // Zero would mean "checkpoint before every commit has happened", which is not a
            // meaningful setting; treat it as 1 rather than dividing the world by it.
            .map(|v| v.max(1))
            .unwrap_or(256)
    })
}


/// How many transaction ids a standalone node takes for itself at a time.
///
/// Invisible to everything durable: only the *issued* watermark reaches the WAL header, so a
/// self-grant of 256 that issues one leaves the header exactly where a `fetch_add` of one would.
/// Above one so that single-node running exercises the partially-consumed-range path.
const SELF_GRANT_TXN_IDS: u64 = 256;

pub struct TxnManager {
    pub wal: Arc<WalManager>,
    pub bp: Arc<BufferPoolManager>,
    /// **F4: transaction ids are cluster state.**
    ///
    /// This was `pub next_txn_id: AtomicU64` and a `fetch_add`. Two nodes both issue txn 5, and
    /// because the TEL's `stamp()` leads with `TxnId` to order writes across branches (ledger R8),
    /// the duplicate does not merely repeat an integer — it silently corrupts **merge ordering**,
    /// which is the one thing the merge engine cannot detect for itself.
    ///
    /// A standalone node grants itself and issues exactly what `fetch_add` issued, starting at the
    /// WAL header's id; a cluster member issues only from an applied
    /// [`crate::consensus::Command::TxnIdRange`] and **refuses** when it holds none. See
    /// [`crate::cluster`].
    pub txn_ids: GrantedCounter,
    /// **D59 — how many times the active-transaction table has CHANGED.**
    ///
    /// A snapshot is `(high_water, the set of active ids)`, and `read_snapshot` used to rebuild
    /// that set — one `HashMap` walk and a `HashSet` allocation — under `att`'s mutex, once per
    /// statement. Measured with 16 agent readers after D58 took the page latch off the read path
    /// (`bench/d58_profile_16T_after.sample.txt`): 17.4k of ~80k thread-samples were in
    /// `read_snapshot`, the single remaining blocking site, holding the arm at ×5.68 against a
    /// private-runtime control's ×10.
    ///
    /// This counter is Postgres 14's `xactCompletionCount` (Freund's GetSnapshotData scalability
    /// work) and D54's catalog `epoch` in this repo: a reader that already holds a snapshot taken
    /// at version V, and reads V again, **knows its snapshot is still exact** and takes no lock
    /// and allocates nothing. It is bumped by [`AttGuard`] on drop, INSIDE the critical section
    /// that changed the table — a bump after the unlock would let a reader cache a snapshot it
    /// took before a change under a version it read after one.
    att_version: AtomicU64,
    /// This manager's identity, for [`TxnManager::read_snapshot_cached`]'s thread-local cache.
    /// An address would not do: a dropped manager's address is reused, and the cache would then
    /// hand a new manager an old one's active set whenever the versions happened to agree.
    id: u64,
    /// **Private since D59.** Every borrow goes through [`TxnManager::att_read`] or
    /// [`TxnManager::att_write`], because the snapshot cache is only sound while `att_version`
    /// moves on every change to the active set — and a `pub` field let any module take the lock
    /// and mutate without one, which a review found recovery already doing.
    att: Mutex<HashMap<u64, TxnEntry>>,
    pub commits_since_checkpoint: AtomicU64,
    /// Every table's DDL, retained so a checkpoint can re-establish it at the head of the new log.
    ///
    /// A checkpoint truncates the WAL, which would otherwise discard the `CREATE TABLE` records a
    /// log reader needs — and `CREATE TABLE` itself checkpoints, so creating a second table wiped
    /// the first one's schema. Replaying them after each truncation keeps the log self-describing
    /// from its own base, which is the same reason an ARIES checkpoint re-records the dirty page
    /// table rather than assuming a reader saw the original entries.
    schema_log: Mutex<Vec<DdlRecord>>,
    /// Every agent run this database has been told about, retained for exactly the reason
    /// `schema_log` is: [`WalManager::truncate`] discards the log **whole** rather than by prefix,
    /// so a checkpoint erases every identity record in it. Replayed as declarations at the head of
    /// the new log by [`TxnManager::replay_runs`], so a reader starting at the new base can still
    /// name the database's writers.
    ///
    /// **Stated cost:** this grows with the number of distinct runs and is never pruned, and every
    /// checkpoint rewrites all of it — the same unbounded shape `schema_log` has for tables, where
    /// the bound is the schema and here it is the agent history. A database with a very large
    /// number of runs pays for that at each checkpoint. [`TxnManager::retained_runs`] is how a
    /// caller sees the size; nothing here caps it, because dropping declarations would silently
    /// make some writers unnameable and that is the failure this record exists to prevent.
    run_log: Mutex<Vec<RunEntity>>,
    /// Open transaction -> the run that will be named immediately before its `Commit`.
    ///
    /// Held here rather than written when it is bound, and that is the whole correctness property.
    /// See [`TxnManager::bind_run`].
    run_bindings: Mutex<HashMap<u64, RunEntity>>,
    /// Open transaction -> every primary-index entry it moved, oldest first. D202.
    ///
    /// Index pages are not logged, so the heap undo in [`TxnManager::abort`] cannot reach them,
    /// and a rolled-back write used to leave its key pointing at the slot `undo_insert` freed.
    /// Every later read of the key then failed with `SlotDeleted`, and so did every INSERT of it.
    /// See [`PrimaryWrite`] for what is recorded, and `abort` for how it is undone.
    ///
    /// In memory, beside `run_bindings` and for the same reason: it dies with the process, and so
    /// does every tree it could repair. After a crash, the trees are rebuilt from the recovered heap
    /// (`wal::recovery::rebuild_indexes`), which is correct whatever this held.
    index_undo: Mutex<HashMap<u64, Vec<PrimaryWrite>>>,
    /// Open transaction -> every heap slot its logged deletes RETIRED. D213; see
    /// `storage::heap_page::RETIRED`.
    ///
    /// A retired slot's bytes stay counted as occupied until its transaction ends, so the rollback
    /// can restore the tuple in place. [`TxnManager::commit`] frees them, each logged as a
    /// `HeapRelease` after the `Commit`. An abort needs nothing from here: its undo restores each
    /// slot from the log.
    ///
    /// In memory, like `index_undo`. After a crash, recovery rebuilds the same list from the log
    /// and finishes the releases a committed transaction did not get to log
    /// (`wal::recovery::recover`, [`TxnManager::finish_releases`]).
    retired: Mutex<HashMap<u64, Vec<RetiredSlot>>>,
    /// Releases that failed and are still owed, as `(committed transaction, slot)`. The adversary's
    /// F2 on `115f0b7`, and the lead's decision.
    ///
    /// A checkpoint retries them BEFORE it truncates the log, and refuses to truncate while any still
    /// fails ([`TxnManager::retry_pending_releases`]). The open's checkpoint is the one that does not
    /// retry: recovery has just attempted each (`TxnManager::checkpoint_after_frees`). So the log stays the durable record of what is
    /// owed, and a restart re-derives the same list from it (`wal::recovery::recover`). A sweep of
    /// every page at open is not needed, and would have cost every restart O(pages), the D216 shape.
    pending_releases: Mutex<Vec<(u64, RetiredSlot)>>,
    /// **Held across every attempt at a release and every checkpoint's truncation decision.**
    /// Review 2's C1: a retry takes the whole pending list out and puts back only what still fails,
    /// so without this lock a concurrent checkpoint could see nothing owed in that window and
    /// truncate the record of a release that then fails again.
    release_retry: Mutex<()>,
    /// Whether the last checkpoint decision kept the log because releases are owed. Review 3's
    /// decision 3: the stderr line is printed when this CHANGES (owed, then clear again), not at every
    /// checkpoint that keeps the log. `DEFERRED_CHECKPOINTS` counts every one.
    keeping_log: std::sync::atomic::AtomicBool,
    /// Owed releases that are page/log MISMATCHES whose quarantine record could not be written, so
    /// they stay owed (review 3's decision 6). Review 4's finding 4: a DROP must not discard one of
    /// these, because its truncation would remove the only record of it; see
    /// [`TxnManager::drop_checkpointed`]. Each keeps the quarantine line it could not write (review
    /// 6's caveat 1): a retry that fails before it can look at the slot writes that line, and the
    /// entry leaves only once a line for it is written or the slot is released.
    ///
    /// **Stated (review 7's F3): this list lives in memory and does not survive a restart.** After
    /// one, recovery re-derives the owed release from the log (`finish_releases`); if the page cannot
    /// be read then, that settle answers `Retry` with no stored line, and a DROP of the table can
    /// discard the release. A two-fault residual: a quarantine that could not be written, then a
    /// restart, then a page that cannot be read.
    unrecorded: Mutex<Vec<(u64, RetiredSlot, String)>>,
    /// **The DROP intents not yet carried out (D229 (a)),** in the order they were recorded. The
    /// durable copy is the intent file beside the log (`wal::free_intent`); every change to this
    /// list is written there first. Each intent's pages are quarantined in the `DiskManager` for
    /// as long as it is here.
    ///
    /// Lock order: `att`, `release_retry`, then this, then the buffer pool and the log.
    pending_frees: Mutex<Vec<PendingFree>>,
    /// The filesystem the intent file and the stale-indexes marker's fsyncs go through: `OsFileOps`,
    /// or a double that fails or records a step, so the durability orders (A4; the rebuild
    /// trigger's fsyncs) are testable at all (D229 review 1's R7). See [`TxnManager::new_with_file_ops`].
    file_ops: Arc<dyn FileOps + Send + Sync>,
}

/// One DROP intent, and whether its table's unlink is known to have happened.
///
/// Undecided: recorded by a DROP whose mutation then failed, or read back at an open before the
/// durable catalog was consulted. Such an intent is never carried out in this process. Decided: the
/// unlink happened (the DROP's mutation returned, or the durable catalog no longer names the table),
/// and the pages are freed at the next checkpoint, once it has synced.
struct PendingFree {
    intent: FreeIntent,
    decided: bool,
}

/// A heap slot retired by a logged delete, as the commit must release it. D213.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetiredSlot {
    pub dir_root: u32,
    pub page_id: u32,
    pub slot: u16,
}

/// Releases that failed and may succeed on a retry (an I/O error, a poisoned log), since process
/// start: the slot is still retired on its page, and the release is pending. D213. Each is counted
/// once, when it first fails; a failed retry is not counted again. A release that can NEVER succeed
/// is counted in [`RELEASE_MISMATCHES`] instead.
///
/// A release runs after the `Commit` is durable, so the transaction HAS committed and
/// [`TxnManager::commit`] must answer `Ok`: an `Err` makes the executor keep the session's
/// transaction, and a ROLLBACK would then undo committed work. A failure is counted here instead,
/// with one line on stderr, and the release waits in the pending list. What it holds is space,
/// not a wrong answer: the retired slot reads as deleted, as it should. It holds it only until a
/// retry succeeds, because no checkpoint truncates the log past it (the adversary's F2 on `115f0b7`).
/// Read twice and subtract to scope it to a phase.
pub static RELEASE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// See [`RELEASE_FAILURES`].
pub fn release_failures() -> u64 {
    RELEASE_FAILURES.load(Ordering::Relaxed)
}

/// Releases that succeeded, but whose page-directory update failed, since process start. Counted
/// apart from [`RELEASE_FAILURES`] (the adversary's F3 on `115f0b7`): the slot IS free, on the page
/// and in the log, and only `find_page_with_space` does not offer its bytes yet. They are offered
/// once the page's entry is next rewritten: by the next write to that page, or by recovery's
/// directory repair.
pub static DIRECTORY_UPDATE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// See [`DIRECTORY_UPDATE_FAILURES`].
pub fn directory_update_failures() -> u64 {
    DIRECTORY_UPDATE_FAILURES.load(Ordering::Relaxed)
}

/// Releases that can NEVER succeed, since process start: the page disagrees with the log (the slot is
/// past the page's slot array, or holds a live tuple). Review 2's Q3, and the lead's decision.
/// Retrying such a release is futile, so it is NOT kept pending: a pending release holds the log
/// against truncation, and this one would hold it for ever, with every restart replaying all of it.
/// Its bytes stay retired.
///
/// **Recorded durably BEFORE it is dropped (review 3's decision 6).** No legitimate schedule leaves a
/// LIVE slot where a retired one was expected, so one is evidence of corruption, and once the release
/// leaves the pending list the next checkpoint truncates the log that recorded it. So each is first
/// appended to [`release_quarantine`] beside the log and fsynced, and only then counted here and
/// printed. A write that fails leaves the release pending, so the log keeps it.
pub static RELEASE_MISMATCHES: AtomicU64 = AtomicU64::new(0);

/// See [`RELEASE_MISMATCHES`].
pub fn release_mismatches() -> u64 {
    RELEASE_MISMATCHES.load(Ordering::Relaxed)
}

/// Mismatch lines written from a STORED observation, since process start (review 7's F4): a
/// mismatch whose first quarantine write failed, recorded later by a retry that could not read the
/// page. Its release stays owed, so it is not in [`RELEASE_MISMATCHES`], which counts a mismatch when
/// it is dropped; a DROP may later discard it.
pub static STORED_MISMATCH_LINES: AtomicU64 = AtomicU64::new(0);

/// See [`STORED_MISMATCH_LINES`].
pub fn stored_mismatch_lines() -> u64 {
    STORED_MISMATCH_LINES.load(Ordering::Relaxed)
}

/// Checkpoints that did not truncate the log, since process start. Three kinds are counted:
/// - checkpoints that flushed every page and synced, but KEPT the log because releases are owed.
///   Any caller counts: the automatic trigger, a DDL, an open, or an explicit `checkpoint`, which
///   also returns its refusal.
/// - automatic checkpoints that FAILED.
/// - checkpoints whose truncation a WAL pin cancelled: `WalManager::truncate` keeps the log, and
///   answers `Ok`, while a pin is below its end (review 4's finding 5).
///
/// Review 2's C4. A deferral is not the commit's failure: `TxnManager::commit` has written `TxnEnd`
/// by then and answers `Ok`. **Review 3's decision 3:** an automatic deferral resets the trigger's
/// counter, so while a release is owed the retry runs once per `checkpoint_interval` commits. It does
/// not run at every commit, which would flush the whole pool and sync each time.
pub static DEFERRED_CHECKPOINTS: AtomicU64 = AtomicU64::new(0);

/// See [`DEFERRED_CHECKPOINTS`].
pub fn deferred_checkpoints() -> u64 {
    DEFERRED_CHECKPOINTS.load(Ordering::Relaxed)
}

/// DROP intents whose frees failed at an attempt (D229): a batch refused because a page was pinned,
/// or an I/O error in the frees, their sync or the intent's rewrite. The pages stay allocated and
/// quarantined, the intent stays on disk, and the next checkpoint or open tries again. Each attempt
/// also prints one line.
pub static FREE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// See [`FREE_FAILURES`].
pub fn free_failures() -> u64 {
    FREE_FAILURES.load(Ordering::Relaxed)
}

/// What a checkpoint did with the log. Every outcome has flushed every page and synced. Review 5's
/// F4: "a pin kept the log" must not read as "truncated", which is what a bare `Ok(0)` said.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointOutcome {
    /// The log was truncated. **Residual, stated at the detector in `checkpoint_or_keep_held`:** an
    /// empty log that gets an append between that detector's reads and `truncate` can be kept by a pin
    /// and still answer this.
    Truncated,
    /// The log was KEPT for this many releases still owed (F2).
    KeptForOwed(usize),
    /// A WAL pin below the log's end cancelled the truncation (`WalManager::truncate` keeps the log,
    /// and answers `Ok`, while one is held).
    KeptByPin,
}

/// DROPs whose checkpoint a WAL pin kept from truncating, since process start (review 5's F4). The
/// dropped table's records then stay in the log. On this branch (D250) that is expected under a pin,
/// and harmless: recovery skips every record a later DROP names. Counted so it is never silent.
pub static KEPT_LOG_DROPS: AtomicU64 = AtomicU64::new(0);

/// See [`KEPT_LOG_DROPS`].
pub fn kept_log_drops() -> u64 {
    KEPT_LOG_DROPS.load(Ordering::Relaxed)
}

/// Every failure counter this module keeps that is not zero, as one line, or `None` when all are
/// zero. The CLI prints it at exit (review 2: the counters had no reader outside tests).
///
/// pgserver has no exit path to print it from: `pgwire::serve` loops over `incoming()`, which never
/// ends, and returns only an accept error, which `serve(..).unwrap()` turns into a panic (review 3's
/// caveat 4). There the counters are readable only through this API and the readers above. Every
/// event they count also writes its own stderr line when it happens.
pub fn failure_counters_line() -> Option<String> {
    let counts = [
        ("index undo failures", index_undo_failures()),
        ("release failures", release_failures()),
        ("release mismatches", release_mismatches()),
        ("mismatches recorded from a stored observation", stored_mismatch_lines()),
        ("directory update failures", directory_update_failures()),
        ("deferred checkpoints", deferred_checkpoints()),
        ("DROP free failures", free_failures()),
        ("drops that kept the log", kept_log_drops()),
    ];
    let nonzero: Vec<String> = counts.iter().filter(|(_, n)| *n > 0).map(|(k, n)| format!("{k} {n}")).collect();
    (!nonzero.is_empty()).then(|| format!("ferrodb: {}", nonzero.join(", ")))
}

/// The release fault seam's production half: no release is ever failed on purpose. The test half,
/// [`FAIL_RELEASES`], is defined after the tests module at the end of this file.
#[cfg(not(test))]
fn injected_release_failure() -> bool {
    false
}

/// How a release failed. A mismatch between the page and the log can never succeed; anything else
/// (an I/O error, a poisoned log) may on a retry.
enum ReleaseError {
    /// The error, and what the page held at the slot: `live` or `absent` (past the slot array).
    Mismatch(FerroError, &'static str),
    /// The error, and what the page showed at the slot when it WAS read (`free` or `retired`, the
    /// releasable states), or `None` when the failure came before the page was read (review 7's F4).
    Retry(FerroError, Option<&'static str>),
}

/// Index undos at abort that failed, since process start. D205 (the adversary's C1).
///
/// A failure here is not returned by [`TxnManager::abort`], because the transaction has ended and
/// every caller reads `Err` as "the abort did not happen". It is counted instead, so it is never
/// silent. It is not the only surface: each failure also writes one line to stderr and a marker
/// file beside the log (`TxnManager::mark_indexes_stale`). What it leaves behind is fail-stop: the
/// entry it could not repair names a freed slot, so a reader of that key gets `SlotDeleted`, and
/// the next open rebuilds every tree because of the marker (`wal::recovery::open_recovered`). Read
/// twice and subtract to scope it to a phase.
pub static INDEX_UNDO_FAILURES: AtomicU64 = AtomicU64::new(0);

/// See [`INDEX_UNDO_FAILURES`].
pub fn index_undo_failures() -> u64 {
    INDEX_UNDO_FAILURES.load(Ordering::Relaxed)
}

/// One primary-index write, as a transaction's abort must undo it. D202.
///
/// `prev` is what the entry held before the write: `None` for a key that was absent, `Some(rid)`
/// for an entry that was repointed (a relocated UPDATE, or a reused key whose new row relocated).
/// So undo is "remove the key" or "point it back at `rid`".
///
/// Only writes that MOVE an entry are recorded. An in-place UPDATE, a DELETE and an in-place reuse
/// write no index entry, so a rollback of them has nothing in a tree to take back
/// (`tests/rollback_index_undo.rs::a_rollback_leaves_every_committed_key_where_it_was`).
///
/// The tree is held by its shared root cell, not by a page id, so a split during the transaction
/// cannot leave this pointing at a stale root. The cell cannot be retired under an open
/// transaction: DROP and ALTER refuse while any transaction is active (`drop_checkpointed`,
/// `Catalog::alter_table`), and `Catalog::sync_root_cells` keeps the existing cell for a live
/// table (`or_insert_with`).
///
/// D208 added two more writers of the cell map, and neither can reach a cell this holds, which is
/// always a table's PRIMARY cell `(t, None)`:
/// - `Catalog::rename_root_cells` MOVES cells on a column rename, and only index keys.
/// - `Catalog::install_fresh_cell` REPLACES the cell under a key a CREATE has just made. It touches
///   `(t, None)` only in CREATE TABLE, for a name absent from the catalog. No open transaction can
///   have written such a table, and CREATE TABLE runs under `ddl_checkpointed`, which refuses while
///   any transaction is active.
pub struct PrimaryWrite {
    pub root: Arc<AtomicU32>,
    pub key: Value,
    pub prev: Option<RecordId>,
}

/// A retained DDL record, replayed into the log after every checkpoint.
#[derive(Debug, Clone)]
pub struct DdlRecord {
    pub op: DdlOp,
    pub table: String,
    pub dir_root: u32,
    pub time_travel_root: u32,
    pub columns: Vec<(String, DataType, bool)>,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub high_water: u64,
    pub active: HashSet<u64>,
}

impl Snapshot {
    /// Whether a transaction's work is **already reflected in this snapshot**.
    ///
    /// The same rule [`ReadView::visible`] applies to a row version, stated over transaction ids
    /// instead. That restatement is what a change feed needs: an event carries the id of the
    /// transaction that produced it, not a version header, and the only question a snapshot-to-
    /// stream cutover has to answer is "did the snapshot already contain this transaction".
    ///
    /// Below the high water mark and not in flight means committed before the snapshot was taken,
    /// so every row that transaction wrote is in the snapshot's rows. In flight, or numbered at or
    /// above the high water mark, means it was not — and the stream owes the consumer those rows.
    pub fn includes(&self, txn_id: u64) -> bool {
        txn_id < self.high_water && !self.active.contains(&txn_id)
    }

    /// Whether a **change-feed event** attributed to `txn_id` was already delivered by this
    /// snapshot, and therefore must not be delivered again by the stream.
    ///
    /// This differs from [`Snapshot::includes`] in exactly one case, and that case is the reason it
    /// exists rather than callers being trusted to remember it. **Id 0 is not a transaction.** DDL
    /// is logged under it and `begin` never hands it out, but the arithmetic in `includes` is about
    /// timestamps and answers "yes, contained" for 0 — it is below every high water mark and in no
    /// active set. A stream that asked `includes` would suppress every schema declaration and leave
    /// a consumer without the shape of any table created after the cutover.
    ///
    /// The two questions are not the same question with a special case bolted on. `includes` asks
    /// about a *version timestamp*, where 0 is a legitimate value meaning "not written by any
    /// transaction this snapshot has to reason about", and MVCC visibility depends on it staying
    /// that way. This asks about an *event's author*, where 0 means there is no author to compare.
    /// Sharing the arithmetic is right; sharing the answer for 0 is not.
    pub fn already_delivered(&self, txn_id: u64) -> bool {
        txn_id != 0 && self.includes(txn_id)
    }
}

/// A snapshot taken so a change-feed consumer can cut over to the live stream.
///
/// The two fields together are what makes the cutover exact, and neither is sufficient alone:
///
/// - `resume_lsn` is early enough that **no** excluded transaction's records sit below it, so
///   nothing can be missed. It is not "the LSN when the scan started" — a transaction that was
///   already in flight then has records *earlier* than that, and it is excluded from the snapshot,
///   so a stream starting at the scan's LSN would see its `Commit` with none of its changes and
///   drop them silently. So the resume point is pulled back to the oldest in-flight transaction's
///   `Begin`.
/// - `snapshot` says exactly which transactions the rows already cover, so the stream can drop
///   their events instead of re-delivering them.
///
/// Resume position alone gives at-least-once; the transaction set is what removes the overlap.
///
/// **The pin is the third part, and it is here rather than left to the caller because leaving it to
/// the caller did not work.** A recipe documented in `replication::snapshot` said to call this,
/// read every table under the one reader, close it, and stream from `resume_lsn` — and omitted the
/// pin. At the default checkpoint interval of 256 commits nothing truncates in between, so it
/// passed; at `FERRODB_CHECKPOINT_INTERVAL=1` the very next commit truncates the log out from under
/// the resume point and the stream is refused with *"cannot pin lsn 2597: the log has already been
/// truncated to base 4260"*. Handing the claim back with the position it protects is the only shape
/// in which a caller cannot forget it.
#[derive(Debug)]
pub struct SnapshotHandoff {
    /// The reading transaction. Reads performed under it see exactly `snapshot`.
    pub txn_id: u64,
    /// The transactions whose work the read already contains.
    pub snapshot: Snapshot,
    /// Where a stream must resume. Durable by the time this is returned.
    pub resume_lsn: u64,
    /// A claim on the log at `resume_lsn`, held so a checkpoint cannot discard the records the
    /// stream is about to ask for. Released when this handoff is dropped, so a caller that intends
    /// to stream later must keep it alive until the subscription has taken its own.
    pub pin: WalPin,
}

pub struct TxnEntry {
    pub status: TxnStatus,
    /// **Atomic, and that is a D59 decision, not a concurrency flourish.**
    ///
    /// A snapshot is `(high_water, the SET of active ids)`; `last_lsn` is in neither. It is
    /// rewritten by `append_chained` for **every WAL record a transaction writes**, and while it
    /// lived behind a `&mut` borrow of the table, every one of those bumped `att_version` and
    /// invalidated every reader's cached snapshot — a review measured that as "the cache
    /// degenerates to the old behaviour under any concurrent writer". As an atomic it is updated
    /// through a READ borrow, so the version moves only when the active set actually changes.
    pub last_lsn: AtomicU64,
    /// LSN of this transaction's `Begin` record — its earliest record, and therefore the earliest
    /// point a reader would have to resume from in order to see everything it did.
    pub begin_lsn: u64,
    pub snapshot: Option<Snapshot>,
}

pub enum TxnStatus {
    Running, 
    Commiting,
    Aborting
}

#[derive(Debug, Clone)]
pub struct ReadView {
    /// **`Arc` since D59**, so a read that reuses a cached snapshot allocates nothing: the old
    /// `Snapshot` by value meant every statement copied the active-id set even when the set had
    /// not changed. Cloning the `Arc` is one refcount bump per statement against a `HashSet`
    /// allocation and fill; the pointer is shared, never mutated.
    pub snapshot: Arc<Snapshot>,
    pub txn_id: u64,
}

impl TxnManager {
    pub fn new(wal: Arc<WalManager>, bp: Arc<BufferPoolManager>) -> Self {
        Self::new_with_file_ops(wal, bp, Arc::new(OsFileOps))
    }

    /// [`TxnManager::new`] with the filesystem D229's intent file and marker fsyncs go through.
    /// Production passes `OsFileOps`; this crate's tests pass a double through `open_recovered`'s
    /// test seam.
    pub fn new_with_file_ops(wal: Arc<WalManager>, bp: Arc<BufferPoolManager>, file_ops: Arc<dyn FileOps + Send + Sync>) -> Self {
        let start = wal.header_txn_id;
        Self { wal, bp, txn_ids: GrantedCounter::new("txn-id", start, SELF_GRANT_TXN_IDS), att_version: AtomicU64::new(0), id: NEXT_TXN_MANAGER_ID.fetch_add(1, Ordering::Relaxed), att: Mutex::new(HashMap::new()), commits_since_checkpoint: AtomicU64::new(0), schema_log: Mutex::new(Vec::new()), run_log: Mutex::new(Vec::new()), run_bindings: Mutex::new(HashMap::new()), index_undo: Mutex::new(HashMap::new()), retired: Mutex::new(HashMap::new()), pending_releases: Mutex::new(Vec::new()), release_retry: Mutex::new(()), keeping_log: std::sync::atomic::AtomicBool::new(false), unrecorded: Mutex::new(Vec::new()), pending_frees: Mutex::new(Vec::new()), file_ops }
    }

    pub fn begin(&self) -> Result<u64, FerroError> {
        let mut att = self.att_write();
        Self::begin_locked(&self.txn_ids, &self.wal, &mut att)
    }

    /// The body of `begin`, with the active-transaction table already locked.
    ///
    /// Factored out so [`TxnManager::begin_snapshot_read`] can observe the table in the *same*
    /// critical section that allocates the id and writes the `Begin` — see the comment there for
    /// why doing it in two steps would be a race rather than a tidiness question.
    fn begin_locked(
        txn_ids: &GrantedCounter,
        wal: &WalManager,
        att: &mut HashMap<u64, TxnEntry>,
    ) -> Result<u64, FerroError> {
        // **F1: no transaction on a log written before D213** (`wal::log::VERSION`). Recovery replays
        // such a log with its own meaning and undoes its losers without beginning anything, and
        // `open_recovered` then checkpoints it, which rewrites it as version 3. A writer that opened
        // the log some other way would otherwise log records with D213's meaning into a log labelled
        // with the older one, and the next replay would read them wrongly. Every heap record needs a
        // transaction, so refusing here covers them all.
        if wal.is_legacy() {
            return Err(FerroError::Wal(
                "this log was written before D213 (format 2) and has not been replayed and upgraded \
                 yet; open the database through wal::recovery::open_recovered, which does both"
                    .into(),
            ));
        }
        // **Refuses; it does not fetch.** A cluster member holding no granted range fails here
        // rather than asking a leader, because this runs with the active-transaction table locked
        // — the lock every checkpoint and every snapshot handoff waits on — and a network
        // round-trip inside it would stall the whole engine on a partition. Refusing early is also
        // what keeps the failure clean: nothing has been inserted into `att` yet, so there is no
        // half-open transaction to leak.
        let txn_id = txn_ids.take(1)?;
        let lsn = wal.append(txn_id, 0, &RecKind::Begin)?;
        let snapshot = Snapshot { high_water: txn_id, active: att.keys().copied().collect() };
        att.insert(
            txn_id,
            TxnEntry {
                status: TxnStatus::Running,
                last_lsn: AtomicU64::new(lsn),
                begin_lsn: lsn,
                snapshot: Some(snapshot),
            },
        );
        Ok(txn_id)
    }

    /// Open a transaction to take a consistent snapshot from, and report where a stream must
    /// resume so the two meet **with no gap and no overlap**.
    ///
    /// # Why the whole thing is one critical section
    ///
    /// Three facts have to be sampled together or the cutover is wrong:
    ///
    /// 1. which transactions are in flight (they are excluded from the snapshot, so the stream owes
    ///    them),
    /// 2. the high water mark (everything numbered above it is excluded too),
    /// 3. the earliest `Begin` still in flight (the stream has to start at or before it, or the
    ///    excluded transactions' changes sit below the resume point and are lost).
    ///
    /// Sampling them separately loses rows. Read the in-flight set, then have one of them commit,
    /// then compute the earliest `Begin` from what is left: the resume point jumps forward past the
    /// records of a transaction the snapshot does not contain, and its rows reach nobody. Every
    /// transactional record in this engine is appended under this same lock, so holding it makes
    /// all three consistent with each other and freezes the log while they are taken.
    ///
    /// The `Begin`s of transactions that start *after* this call are necessarily above
    /// `resume_lsn`, for the same reason: they cannot be appended until this lock is released.
    ///
    /// The caller must finish with [`TxnManager::end_read_only`].
    pub fn begin_snapshot_read(&self) -> Result<SnapshotHandoff, FerroError> {
        let (txn_id, snapshot, resume_lsn) = {
            let mut att = self.att_write();
            let txn_id = Self::begin_locked(&self.txn_ids, &self.wal, &mut att)?;
            // Includes this reader's own `Begin`, which is the answer when nothing else is in
            // flight: there is then nothing below it that the snapshot does not already contain.
            let resume_lsn = att
                .values()
                .map(|e| e.begin_lsn)
                .min()
                .expect("the reader was just inserted, so the table cannot be empty");
            let snapshot = att[&txn_id]
                .snapshot
                .clone()
                .expect("begin_locked always records a snapshot");
            (txn_id, snapshot, resume_lsn)
        };

        // **The reader is in `att` from here on, and the caller does not have its id yet.** So a
        // `?` on either step below would leak it with nobody able to clean it up: `checkpoint`
        // refuses while `att` is non-empty, which means every later CREATE TABLE, CREATE INDEX and
        // CLI shutdown fails with "checkpoint with active txns" for the life of the process. One
        // failed snapshot would take the database with it. Both steps therefore report through
        // `abandon_reader` rather than through `?`.
        let prepared = (|| {
            // A resume point the log has not durably written is not a position anyone can resume
            // from: a crash moves the frontier backwards underneath it and the consumer is left
            // pointing into a range that will be rewritten by different records. `flush` drains
            // everything buffered, and `resume_lsn` is at or below the reader's own `Begin`, so
            // this covers it.
            self.wal.flush()?;
            // Claimed before this returns, so the resume point is never *published* unclaimed. See
            // `SnapshotHandoff::pin` for what leaving this to the caller cost.
            //
            // **It is not claimed from the moment it exists, and the gap is real rather than
            // theoretical.** The `Begin` above went in under the `att` lock, which was then
            // released; the pin is taken here, without it. A concurrent `checkpoint` samples
            // `att.is_empty()` and *drops that lock before truncating*, so one that found the table
            // empty a moment before this reader was inserted can truncate in between - and then
            // this `pin` fails. It is tempting to write that a checkpoint cannot truncate while a
            // transaction is open; it is not true as written, because the check and the truncation
            // are not one critical section.
            //
            // Which is precisely why the failure below closes the reader instead of returning `?`.
            // The race is narrow, its outcome is a clean refusal the caller can retry, and the
            // alternative - holding `att` across a checkpoint's flush, page writes and fsync - buys
            // atomicity here at the cost of blocking every begin and commit on disk IO. The error
            // path is the cheaper correct answer; it just has to not leak.
            self.wal.pin(resume_lsn)
        })();

        match prepared {
            Ok(pin) => Ok(SnapshotHandoff { txn_id, snapshot, resume_lsn, pin }),
            Err(e) => {
                self.abandon_reader(txn_id);
                Err(e)
            }
        }
    }

    /// Drop a snapshot reader nobody can close, because nobody else knows it exists.
    ///
    /// **The entry must leave `att`, and that is not negotiable** — a checkpoint refuses while any
    /// transaction is active, so an entry left behind here blocks every checkpoint for the life of
    /// the process. It is therefore removed directly if the ordinary close cannot write its
    /// `TxnEnd`, which is the likely case: the reason this is being called at all is usually that
    /// the WAL just refused a write.
    ///
    /// Removing it without a `TxnEnd` record is safe, and specifically because this is a *reader*.
    /// Recovery treats a transaction with no `Commit` or `TxnEnd` as a loser and aborts it
    /// (`wal::recovery`), and an abort walks the undo chain back from its last record — for a
    /// transaction whose `last_lsn` is still its `begin_lsn` there is nothing on that chain, so the
    /// abort is a no-op. A reader that had written would not reach this path: `end_read_only`
    /// refuses and rolls it back.
    fn abandon_reader(&self, txn_id: u64) {
        // The ordinary close first, so the common case still records its `TxnEnd` in the log.
        let _ = self.end_read_only(txn_id);
        self.att_write().remove(&txn_id);
    }

    /// Close a transaction that only read.
    ///
    /// Not `commit`, and not `abort`, and neither is a stylistic preference:
    ///
    /// - `commit` advances the checkpoint counter, and a checkpoint truncates the whole WAL. Ending
    ///   a snapshot read that way could discard the very records the handoff LSN points at, turning
    ///   a successful cutover into "cursor is below the log's base" on the consumer's first pump.
    /// - `abort` would write an `Abort` record, telling every log reader that a transaction rolled
    ///   back when nothing happened at all.
    ///
    /// A reader that turns out to have written is **refused and rolled back** rather than quietly
    /// ended, because its writes would be invisible to its own snapshot and it is not a snapshot
    /// reader at all.
    pub fn end_read_only(&self, txn_id: u64) -> Result<(), FerroError> {
        let wrote = {
            let att = self.att_read();
            let entry = att
                .get(&txn_id)
                .ok_or_else(|| FerroError::Txn(format!("txn {txn_id} is not active")))?;
            entry.last_lsn.load(Ordering::Acquire) != entry.begin_lsn
        };
        if wrote {
            self.abort(txn_id)?;
            return Err(FerroError::Txn(format!(
                "transaction {txn_id} was opened to take a snapshot and then wrote to the \
                 database; it has been rolled back"
            )));
        }
        // **The entry leaves `att` whether or not the `TxnEnd` could be written.** Ordering these
        // the other way round - append with `?`, then remove - returns an error to a caller who has
        // already handed over responsibility for this transaction and now has nothing useful to do
        // with it. Retrying calls back into a WAL that just failed; not retrying leaves the reader
        // active for ever, and an active transaction blocks every checkpoint. The error is still
        // reported; what changes is that reporting it no longer wedges the database. See
        // `abandon_reader` for why a reader with no `TxnEnd` record is safe for recovery.
        //
        // **Not reachable in production today, which is why the test forces it.** `append_chained`
        // fails two ways: the entry is missing from `att`, which cannot happen here because it was
        // just checked and would mean no leak anyway; or `WalManager::append` returns `Err`, which
        // it never does today - it extends an in-memory buffer and ends in `Ok(lsn)`. Rather than
        // leave the guard unproven, `WalManager::fail_next_append` (test-only) fires it on demand;
        // see `a_reader_is_not_leaked_when_its_txn_end_cannot_be_written`. The guard earns its
        // place the day `append` does any IO - a bounded buffer that flushes when full, a direct
        // write - because then the failure is real and its cost is the whole database.
        let appended = self.append_chained(txn_id, &RecKind::TxnEnd);
        self.att_write().remove(&txn_id);
        appended?;
        Ok(())
    }

    pub fn log_insert(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, tuple: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapInsert { dir_root, page_id, slot, tuple: tuple.to_vec() })
    }

    pub fn log_delete(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, old: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapDelete { dir_root, page_id, slot, old: old.to_vec() })
    }

    pub fn log_update(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, old: &[u8], new: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapUpdate { dir_root, page_id, slot, old: old.to_vec(), new: new.to_vec() })
    }

    pub fn append_chained(&self, txn_id: u64, kind: &RecKind) -> Result<u64, FerroError> {
        // A READ borrow: this changes `last_lsn`, which is not part of any snapshot, so it must
        // not move `att_version`. See `TxnEntry::last_lsn`. The entry itself cannot vanish while
        // this guard is held, and `last_lsn` is only ever written by the transaction that owns it.
        let att = self.att_read();
        let entry = att.get(&txn_id).ok_or_else(|| FerroError::Wal("txn not active".into()))?;
        let lsn = self.wal.append(txn_id, entry.last_lsn.load(Ordering::Acquire), kind)?;
        entry.last_lsn.store(lsn, Ordering::Release);
        Ok(lsn)
    }

    /// Name the run that wrote this transaction, so its `Commit` is attributable.
    ///
    /// # The record is written at COMMIT, and nowhere else
    ///
    /// This method records the binding and writes nothing. The identity record goes into the log in
    /// [`TxnManager::commit`], in the append immediately before the `Commit` record, and that
    /// position is a correctness property rather than a tidy choice.
    ///
    /// A change feed's cursor may not advance past the earliest **staged** record of any still-open
    /// transaction — `Decoded::open_from` — or that transaction's rows are stepped over and lost
    /// when it finally commits. An identity record written when the session begins sits *below*
    /// that point: it stages nothing, so it does not hold the cursor back, and the next pump starts
    /// above it. The record is then never read again, and every row of that transaction ships
    /// attributed to nobody while the feed reports a clean run.
    ///
    /// Written immediately before the `Commit` instead, the record cannot be separated from the
    /// commit it describes. If a batch boundary falls between the two, the transaction is still
    /// open at the end of that batch, so the cursor is clamped back below its first staged row —
    /// which is below the identity record — and the next batch reads both together.
    /// `tests/integration_run_identity_feed.rs` runs exactly that workload against both placements.
    ///
    /// **The precise claim is "in this transaction, after every row it staged, before its
    /// `Commit`" — not physical adjacency in the log.** The two appends are separate calls and
    /// nothing holds a lock across them, so another transaction's record, or a `Ddl` record (which
    /// bypasses the active-transaction table entirely), can land between them. That is harmless: the
    /// decoder keys both the staging and the binding by `txn_id`, and the cursor argument above needs
    /// only that the identity record sit *above* this transaction's earliest staged record and
    /// *below* its commit. An earlier version of this doc asserted adjacency, which would have made
    /// any test of it flaky under a concurrent workload.
    ///
    /// # Guards
    ///
    /// Refuses an unknown transaction (a binding nothing would ever write), `ProvId::NONE` (a
    /// record claiming a writer must name one), and a second, *different* run for one transaction —
    /// one transaction has one writer, and quietly keeping either of two would attribute rows to an
    /// actor that did not write them. Re-binding the identical run is a no-op.
    pub fn bind_run(&self, txn_id: u64, run: RunEntity) -> Result<(), FerroError> {
        if run.prov_id.is_none() {
            return Err(FerroError::Provenance(format!(
                "refusing to bind run {}/{} to txn {txn_id} with ProvId::NONE: that is the value \
                 meaning 'unattributed', so the identity record would claim a writer and name none. \
                 Intern the run first and bind the id the store assigned.",
                run.agent_id, run.run_id
            )));
        }
        // **Every named field must be named.**
        //
        // Refused here rather than counted downstream, because the consumer's contract is stricter
        // than this type: `cdc-consumer`'s `checkWriter` REFUSES a writer object with an empty
        // agent, run, model or model_version, and it refuses the whole LINE — so one such run would
        // make `validate`, `sink`, `diff` and `follow` all abort at the first row it wrote and land
        // nothing at all. `model_version` is also the field `retract` keys on, and an empty one is
        // indistinguishable from the NULL a row with no writer carries.
        //
        // A producer that can emit what its consumer must reject is a contract whose halves
        // disagree, and the half to fix is the one that can still refuse cheaply.
        for (field, value) in [
            ("agent_id", &run.agent_id),
            ("run_id", &run.run_id),
            ("model", &run.model),
            ("model_version", &run.model_version),
        ] {
            if value.trim().is_empty() {
                return Err(FerroError::Provenance(format!(
                    "refusing to bind run {} to txn {txn_id}: its {field} is empty. Every field of \
                     an identity record is part of the answer to 'who wrote this row', and the \
                     change-feed consumer refuses a writer object carrying an empty one - so this \
                     run would make the whole feed unreadable rather than merely vague. Use an \
                     explicit placeholder such as \"unspecified\" if there is genuinely nothing to \
                     name.",
                    run.describe()
                )));
            }
        }
        if !self.att_read().contains_key(&txn_id) {
            return Err(FerroError::Txn(format!(
                "cannot bind a run to txn {txn_id}: it is not active, so no identity record would \
                 ever be written for it"
            )));
        }
        {
            let bindings = self.run_bindings.lock().unwrap();
            match bindings.get(&txn_id) {
                // Compared with `same_actor`, NOT `==`: `started_at` is when a particular session
                // began and is deliberately not part of who the actor is, so two sessions of one run
                // legitimately differ in it. `MemProvenanceStore::intern` hands them the same
                // `ProvId` for exactly that reason, and a full-equality test here would refuse a
                // second session of the same run because the clock had moved.
                Some(existing) if !existing.same_actor(&run) => {
                    return Err(FerroError::Provenance(format!(
                        "txn {txn_id} is already bound to {}; refusing to rebind it to {}. One \
                         transaction has one writer.",
                        existing.describe(),
                        run.describe()
                    )));
                }
                Some(_) => return Ok(()),
                None => {}
            }
        }
        // **Declared BEFORE the binding is installed, and that order is the fix for a real defect.**
        //
        // It used to insert first. When `declare_run` then refused a slot collision, `bind_run`
        // returned `Err` and the binding STOOD — so the next `commit` appended an identity record
        // for that slot anyway, putting two different actors under one `prov_id` in the log. That is
        // precisely the state `LogicalDecoder` refuses outright, which would make the whole log
        // range undecodable: the guard's own failure path produced the disaster the guard names.
        self.declare_run(run.clone())?;
        self.run_bindings.lock().unwrap().insert(txn_id, run);
        Ok(())
    }

    /// Remember a run so a checkpoint can re-declare it, without binding it to a transaction.
    ///
    /// Refuses to hold two different actors under one `prov_id`: the slot is the reference every
    /// stamped version carries, so two meanings for it would make every attribution ambiguous.
    pub fn declare_run(&self, run: RunEntity) -> Result<(), FerroError> {
        let mut log = self.run_log.lock().unwrap();
        if let Some(existing) = log.iter().find(|r| r.prov_id == run.prov_id) {
            // `same_actor`, not `==`. Full equality compares `started_at`, which is when a session
            // began rather than part of who the actor is — so a second session of the same run
            // carries a different one and was being refused for having a later clock reading. The
            // in-memory store already draws the line here and hands both sessions one `ProvId`; two
            // definitions of "the same actor" in one system is how that becomes a bug.
            if !existing.same_actor(&run) {
                return Err(FerroError::Provenance(format!(
                    "provenance slot {} is already declared as {}; refusing to redeclare it as {}",
                    run.prov_id,
                    existing.describe(),
                    run.describe()
                )));
            }
            return Ok(());
        }
        log.push(run);
        Ok(())
    }

    /// How many run declarations a checkpoint would replay. See [`TxnManager::run_log`].
    pub fn retained_runs(&self) -> usize {
        self.run_log.lock().unwrap().len()
    }

    /// Re-declare every known run at the head of the log, after a truncation discarded them.
    ///
    /// Transaction id 0, matching [`TxnManager::append_ddl`]: a declaration says "this run exists",
    /// and transaction 0 never commits, so it binds nothing. `LogicalDecoder` relies on exactly
    /// that to tell a declaration from a binding.
    fn replay_runs(&self) -> Result<(), FerroError> {
        let runs = self.run_log.lock().unwrap().clone();
        if runs.is_empty() {
            return Ok(());
        }
        for run in runs {
            self.wal.append(0, 0, &RecKind::RunIdentity { run })?;
        }
        self.wal.flush()
    }

    pub fn commit(&self, txn_id: u64) -> Result<(), FerroError> {
        // D211: a transaction whose rollback began may not commit its half-undone rows.
        if self.is_aborting(txn_id) {
            return Err(Self::rolling_back(txn_id));
        }
        // **Immediately before the `Commit`, with no append between them.** See `bind_run` for what
        // any other position costs. Read rather than removed, so a failed append leaves the binding
        // intact for the abort that follows.
        let bound = self.run_bindings.lock().unwrap().get(&txn_id).cloned();
        if let Some(run) = bound {
            self.append_chained(txn_id, &RecKind::RunIdentity { run })?;
        }
        let commit_lsn = self.append_chained(txn_id, &RecKind::Commit)?;
        // **F4(i) (the adversary on `115f0b7`; the lead's decision): past this point the only
        // outcomes are "committed" and fail-stop.** The `Commit` record is in the log buffer. If
        // the flush fails, it may or may not be on disk, and a later flush can still make it
        // durable. So a ROLLBACK now would log an Abort after a `Commit` that may land, and
        // recovery would treat the transaction as committed with its pages rolled back, while the
        // change feed had already shipped its rows at the `Commit`. The log is poisoned instead:
        // every later write is refused, and the database must be reopened, where recovery decides
        // from what reached disk. This is PostgreSQL's PANIC on an fsync failure, for the same
        // reason. The session keeps its id (the transaction is not known to have ended), and its
        // ROLLBACK is refused with every other write.
        if let Err(e) = self.wal.flush_up_to(commit_lsn) {
            let why = format!("transaction {txn_id}'s Commit record could not be made durable ({e})");
            self.wal.poison(&why);
            return Err(FerroError::Wal(format!(
                "{why}. The log now refuses every write, and the database must be reopened: recovery \
                 decides from what reached disk whether transaction {txn_id} committed"
            )));
        }
        // **D213: decided, so the space its deletes held for their undo is free now.** After the
        // flush, never before: a slot freed while the `Commit` could still be lost would let
        // another transaction take the bytes this one's rollback needs. Before `att` lets go of the
        // transaction, so a checkpoint, which refuses to start while `att` is not empty, cannot
        // start between the `Commit` and its releases. Never fails the commit; see
        // `RELEASE_FAILURES`.
        let retired = self.retired.lock().unwrap().remove(&txn_id).unwrap_or_default();
        self.release_retired(txn_id, &retired);
        let _ = self.append_chained(txn_id, &RecKind::TxnEnd)?;
        self.att_write().remove(&txn_id);
        self.run_bindings.lock().unwrap().remove(&txn_id);
        // D202: committed, so nothing may ever undo these writes. Dropped here and not left for
        // a later abort to find: transaction ids restart from the log header after a restart, and
        // a stale list under a reissued id would repoint or remove a committed key.
        self.index_undo.lock().unwrap().remove(&txn_id);
        // **F4: the automatic checkpoint is a node-local decision, and on a cluster it is wrong.**
        //
        // `consensus::Command::Checkpoint` exists for this and says why in its own words:
        // "`TxnManager` checkpoints on a node-local counter and calls `wal.truncate`; two nodes
        // doing that at different moments have different WAL byte streams from then on. Since an
        // LSN is an offset into that stream, a follower promoted to leader would then append into
        // an offset space its own followers do not share."
        //
        // So a member counts and waits. The counter is deliberately **not** reset when the
        // checkpoint is withheld, so it stays over the threshold and the round that finally applies
        // `Command::Checkpoint` does the work — deferred, never dropped, and readable through
        // [`TxnManager::checkpoint_due`] so a leader loop knows to propose one.
        //
        // Withholding is the safe direction, and the asymmetry is the reason to choose it: a WAL
        // that was not truncated is replayed at recovery and costs disk, while a WAL truncated at a
        // different offset on each node is not repairable at all.
        //
        // Only the *automatic* trigger is guarded. Explicit `checkpoint()` calls — DDL in
        // `executor.rs`, clean exit in `cli.rs` — sit on paths that are themselves replicated
        // decisions (`Command::Catalog`), so they are already ordered by the log; guarding them
        // here would refuse DDL on a cluster member for a reason that does not apply.
        //
        // **Review 2's C4 (the lead's decision): the transaction has ENDED by here, so `commit`
        // answers `Ok` whatever the checkpoint does.** `TxnEnd` is written and `att` has let go. A
        // checkpoint that fails, or keeps the log because releases are owed (F2), is a DEFERRAL,
        // counted in `DEFERRED_CHECKPOINTS`. At `368d0e1` a failed checkpoint here was returned as
        // `Err`, and every caller but the executor's COMMIT arm read that as "not committed": the
        // implicit commit of an autocommit statement, and MERGE's publish.
        //
        // **Review 3's decision 3: a deferral RESETS `commits_since_checkpoint`.** At `7cede54` it did
        // not, so after one deferral every commit with nothing else open was due again, and each
        // flushed the whole pool and synced. For a release that never succeeds, that was an fsync at
        // every commit for the life of the process. Now the retry runs once per `checkpoint_interval`
        // commits, as a checkpoint would. The log-keeping state prints once when it begins and once
        // when it clears (`checkpoint_or_keep_held`). A FAILED checkpoint still prints each time, and
        // that is at most once per interval now.
        let due = self.commits_since_checkpoint.fetch_add(1, Ordering::SeqCst) + 1
            >= checkpoint_interval()
            && self.att_read().is_empty();
        if due && !crate::cluster::is_clustered() {
            use std::io::Write;
            match self.checkpoint_keeping_owed() {
                Ok(CheckpointOutcome::Truncated) => {}
                // Counted where the log was kept (`checkpoint_or_keep_held`).
                Ok(CheckpointOutcome::KeptForOwed(_) | CheckpointOutcome::KeptByPin) => {
                    self.commits_since_checkpoint.store(0, Ordering::SeqCst)
                }
                Err(e) => {
                    DEFERRED_CHECKPOINTS.fetch_add(1, Ordering::Relaxed);
                    self.commits_since_checkpoint.store(0, Ordering::SeqCst);
                    let _ = writeln!(
                        std::io::stderr(),
                        "ferrodb: transaction {txn_id} committed; the automatic checkpoint after it \
                         failed ({e}), and the next one is due in {} commits",
                        checkpoint_interval()
                    );
                }
            }
        }
        Ok(())
    }

    pub fn abort(&self, txn_id: u64) -> Result<(), FerroError> {
        let abort_lsn = self.append_chained(txn_id, &RecKind::Abort)?;
        let _ = abort_lsn;
        {
            self.att_write().get_mut(&txn_id).unwrap().status = TxnStatus::Aborting;
        }
        // D202: the index before the heap. For the common case, a rolled-back INSERT of a new
        // key, this removes the key while its slot still holds the (uncommitted, invisible) row,
        // so a lock-free reader sees "no such key" rather than a key pointing at a freed slot.
        // A relocated write has a window either way round: the entry and the slot it names are
        // two page writes.
        //
        // **D205 C1 — an index-undo failure is COUNTED, never returned.** Every caller reads this
        // function's `Err` as "the abort did not happen": `executor.rs` skips
        // `session.current = None`, and the caller's own error is replaced by this one. Returned
        // after `TxnEnd`, as it was at `c21eaff`, it left a session holding a dead id and hid the
        // statement's error. The transaction does end, so `Ok` is the true answer. The failure is
        // not silent: [`INDEX_UNDO_FAILURES`] counts it, a line goes to stderr, and a marker beside the
        // log makes the next open rebuild every tree from the heap even if the log is then empty
        // (`mark_indexes_stale`; this sentence claimed the rebuild without the marker until the
        // re-adversary's C1 correction). Until then, the entry it failed to repair names a freed
        // slot, so readers of that key fail with `SlotDeleted` rather than guessing.
        //
        // ⛔ **WITHDRAWN — the D205 C2 note that stood here (`d81f080`..`d7891d5`) was FALSE.** It said a
        // heap undo that failed after this was "recoverable", that `ROLLBACK` "resumes the heap undo at
        // `undo_next`", and that "nothing answers wrongly in between". The fresh-context re-adversary
        // (`frontier/d205_readversary.md`, `fffdc62`; ledger D211) showed all three wrong:
        // - the CLR was appended BEFORE its undo applied, so a resumed abort followed `undo_next`
        //   past the failed record and never retried it;
        // - `Aborting` was written and never read, so the stuck transaction could run statements or
        //   COMMIT;
        // - `ROLLBACK` dropped the session's id before aborting.
        //
        // **What is true since the D211 fix:**
        // - `apply_then_log` logs a CLR only for an undo that is on the page, so a retry reaches the
        //   failed record itself;
        // - an `Aborting` transaction is refused everything but ROLLBACK (`snapshot_of`, `commit`);
        // - the executor keeps the session's id when a rollback fails (`tests/undo_refused_is_held.rs`).
        //
        // **What was still NOT recoverable at `00f4c39`, and is unrepresentable since D213:** an undo
        // that never finds room, for instance because other transactions committed rows into the
        // page it needs. The transaction then stayed `Aborting` holding its rows, every checkpoint
        // was refused while it was open, and at the next open recovery's undo of this loser met the
        // same refusal and the open FAILED (ledger D213). The lead decided the design: space freed by
        // an uncommitted transaction is not reusable until it commits. A logged delete retires its
        // slot and a shrink keeps its capacity (`storage::heap_page::RETIRED`), so every undo below
        // writes into bytes this transaction still holds and needs no room
        // (`tests/undo_space_is_reserved.rs`).
        //
        // What can still fail an undo is an I/O error, a failed log append, or a page that
        // disagrees with the log. For those, everything above holds: the transaction stays
        // `Aborting`, refuses all but ROLLBACK, and a retry reaches the failed record itself
        // (`tests/undo_refused_is_held.rs`).
        let writes = self.index_undo.lock().unwrap().remove(&txn_id).unwrap_or_default();
        if let Err(e) = self.undo_primary_writes(writes) {
            INDEX_UNDO_FAILURES.fetch_add(1, Ordering::Relaxed);
            self.mark_indexes_stale(txn_id, &e);
        }
        let mut lsn = {
            self.att_read().get(&txn_id).unwrap().last_lsn.load(Ordering::Acquire)
        };
        loop {
            let (rec, _) = self.wal.read_record(lsn)?;
            match rec.kind {
                RecKind::Begin => break,
                RecKind::HeapInsert { dir_root, page_id, slot, .. } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn , undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapDelete{ dir_root, page_id, slot, old: Vec::new() })
                    };
                    self.apply_then_log(txn_id, page_id, &clr, true, |page| undo_insert(page, slot))?;
                }
                RecKind::HeapDelete { dir_root, page_id, slot, old } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn, undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapInsert { dir_root, page_id, slot, tuple: old.to_vec() })
                    };
                    self.apply_then_log(txn_id, page_id, &clr, true, |page| undo_delete(page, slot, &old))?;
                }
                RecKind::HeapUpdate { dir_root, page_id, slot, old, new } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn, undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapUpdate { dir_root, page_id, slot, old: new.clone(), new: old.clone() })
                    }; 
                    self.apply_then_log(txn_id, page_id, &clr, true, |page| undo_update(page, slot, &old))?;
                }
                RecKind::Clr {undo_next, .. } => {
                    if undo_next == 0 {
                        break;
                    }
                    lsn = undo_next;
                    continue;
                }
                _ => {}
            }
            if rec.prev_lsn == 0 {
                break;
            }
            lsn = rec.prev_lsn;
        }
        let _ = self.append_chained(txn_id, &RecKind::TxnEnd)?;
        self.att_write().remove(&txn_id);
        // D213: every slot this transaction retired has been restored by the undo above, so there
        // is nothing left to release.
        self.retired.lock().unwrap().remove(&txn_id);
        // The run bound to this transaction described work that has been rolled back. No identity
        // record was written — they are only written at commit — so there is nothing in the log to
        // retract, only a binding that must not outlive its transaction id.
        self.run_bindings.lock().unwrap().remove(&txn_id);
        Ok(())
    }

    /// Apply one undo to its page, and log its CLR ONLY IF it applied. Both happen under the page's
    /// write latch. D211. Returns the page as published.
    ///
    /// A commit's releases go through here too (D213, `HeapRelease` in `rec`): the obligation is
    /// the same, a record in the log only for a change that is on the page.
    ///
    /// This was "append the CLR, then apply the undo with `?`". A refused undo (no room, D210's
    /// `restore_at`, or an update growing back into a page others have filled) then returned with
    /// the CLR already in the log. That CLR says "undone" for a record that was not, so:
    /// - a resumed abort followed its `undo_next` straight PAST the failed record and never
    ///   retried it;
    /// - recovery's redo pass replayed the CLR onto the same page, met the same refusal, and failed
    ///   the open.
    ///
    /// The re-adversary's proposed fix, re-applying the chain-head CLR when the page's LSN is behind
    /// it, was not taken. That gate cannot tell "not applied" from "applied" once another
    /// transaction has written the same page after the failure, and nothing stops one doing so. So
    /// the skipped undo could still be skipped. Here instead no CLR exists unless its undo is on the
    /// page, and a retry reaches the failed record itself, with no gate to misjudge.
    ///
    /// The page is changed in a deserialised COPY and proven to serialise before the CLR is
    /// appended. After the append the only change is the LSN, so nothing can fail between logging
    /// the CLR and publishing the page. Holding the frame latch across the WAL append is the order
    /// `HeapFileManager::insert_into` already uses (frame, then `append_chained`).
    ///
    /// `chained`: an undo's CLR goes on the transaction's chain (`append_chained`), because a resumed
    /// abort walks it. A `HeapRelease` does not: nothing walks a committed transaction's chain, and a
    /// release may be logged after the transaction has left `att`, by a checkpoint's retry or by
    /// recovery (F2). It is appended under the transaction's id with no previous LSN.
    fn apply_then_log<F>(&self, txn_id: u64, page_id: u32, rec: &RecKind, chained: bool, undo: F) -> Result<Page, FerroError>
    where
        F: FnOnce(&mut Page) -> Result<(), FerroError>,
    {
        let frame_i = self.bp.fetch_page(page_id)?;
        let mut frame = self.bp.frame_write(frame_i);
        let undone = Page::deserialize(frame.data).and_then(|mut page| {
            undo(&mut page)?;
            page.serialize()?;
            Ok(page)
        });
        let mut page = match undone {
            Ok(page) => page,
            Err(e) => {
                drop(frame);
                self.bp.unpin_page(page_id, false);
                return Err(e);
            }
        };
        let appended = if chained { self.append_chained(txn_id, rec) } else { self.wal.append(txn_id, 0, rec) };
        let clr_lsn = match appended {
            Ok(lsn) => lsn,
            Err(e) => {
                drop(frame);
                self.bp.unpin_page(page_id, false);
                return Err(e);
            }
        };
        page.lsn = clr_lsn;
        let published = page.serialize().map(|data| frame.data = data);
        drop(frame);
        self.bp.unpin_page(page_id, published.is_ok());
        published.map(|()| page)
    }

    /// The durable and the visible half of an index-undo failure (the re-adversary's C1
    /// corrections).
    ///
    /// Durable: a marker beside the log, which the next `wal::recovery::open_recovered` honours by
    /// rebuilding every tree, even when the log it opens is empty. `recover` returns `false` for an
    /// empty log, and a clean restart can leave one, so without this "the next open rebuilds" was
    /// false. Visible: one line on stderr, written with `writeln!` and not `eprintln!` (which panics
    /// on a closed stderr; `branch::lease_thread` records why), plus [`INDEX_UNDO_FAILURES`].
    fn mark_indexes_stale(&self, txn_id: u64, e: &FerroError) {
        use std::io::Write;
        let marker = stale_indexes_marker(&self.wal.path);
        let written = write_stale_indexes_marker(&self.wal.path, &format!("txn {txn_id}: {e}"));
        let _ = writeln!(
            std::io::stderr(),
            "ferrodb: the rollback of transaction {txn_id} could not undo its primary-index writes ({e}); \
             the keys it moved fail with SlotDeleted until the indexes are rebuilt at the next open{}",
            match &written {
                Ok(()) => String::new(),
                Err(w) => format!(", but the marker {} that asks for that rebuild could not be written ({w})", marker.display()),
            }
        );
    }

    /// Record that `txn_id` is about to move the primary-index entry for `key` away from `prev`.
    /// D202; see [`PrimaryWrite`].
    ///
    /// Called BEFORE the write, so an abort that follows a half-finished write still restores
    /// `prev`. Undoing a write that never happened is harmless: it puts back what is already
    /// there, or removes a key that is not there.
    pub fn record_primary_write(&self, txn_id: u64, root: Arc<AtomicU32>, key: Value, prev: Option<RecordId>) {
        self.index_undo
            .lock()
            .unwrap()
            .entry(txn_id)
            .or_default()
            .push(PrimaryWrite { root, key, prev });
    }

    /// Record that `txn_id`'s logged delete retired `slot` of `page_id`, so its commit frees it.
    /// D213; see `storage::heap_page::RETIRED`. Called by `HeapFileManager` once the `HeapDelete`
    /// is in the log.
    pub fn record_retired(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16) {
        self.retired
            .lock()
            .unwrap()
            .entry(txn_id)
            .or_default()
            .push(RetiredSlot { dir_root, page_id, slot });
    }

    /// Free the slots a COMMITTED transaction retired. Each is applied and logged as a
    /// `HeapRelease` through [`TxnManager::apply_then_log`], and the page directory is told, because
    /// `HeapFileManager::find_page_with_space` reads the directory and not the page. D213.
    ///
    /// It never fails. The transaction has committed, so the only honest answer to its caller is
    /// `Ok`. A release that fails is counted in [`RELEASE_FAILURES`], with a line on stderr, and
    /// waits in the pending list for a checkpoint to retry it (F2). A directory update that fails
    /// after a release succeeded is counted in [`DIRECTORY_UPDATE_FAILURES`] instead (F3).
    ///
    /// **The cost this pays for, stated.** A page may fill sooner than before: a relocated tuple's
    /// old bytes stay occupied until its transaction ends, where they used to be free at once, so a
    /// long transaction holds on to every byte its relocations and deletes vacated. The bound is
    /// that transaction's own writes: the bytes held are at most the old images its `HeapDelete`
    /// records carry, which it has already paid for in the log. Nothing is held past the abort, and
    /// nothing past the commit EXCEPT a release that fails: its slot stays retired until a retry
    /// succeeds, and meanwhile no checkpoint truncates the log, which then grows as a pinned log
    /// does (the adversary's F2 on `115f0b7` corrected "nothing is held past the commit"). Freed bytes are only ever reusable when they are the lowest on their page, since
    /// nothing compacts a page, and that is unchanged. The log carries one more record per retired
    /// slot, written after the `Commit` and flushed with the next one. A shrink's kept capacity is
    /// not held by the transaction at all: those bytes were garbage before D213, and are now the
    /// slot's room to grow back into, so no page holds more than it did.
    fn release_retired(&self, txn_id: u64, retired: &[RetiredSlot]) {
        let _retry = self.release_retry.lock().unwrap();
        for r in retired {
            if self.settle_release(txn_id, *r, true) {
                self.pending_releases.lock().unwrap().push((txn_id, *r));
            }
        }
    }

    /// Attempt one release and account for the outcome. Returns whether it is still owed: a retryable
    /// failure, or a mismatch whose quarantine record could not be written. `first`: count and print a
    /// retryable failure only on the first attempt.
    ///
    /// **Blind spot, stated (review 2's Q3):** a page that PERMANENTLY fails to read, or a log that
    /// stays poisoned, is a retryable failure by this classification. Its release stays pending,
    /// every checkpoint keeps the log, and the log grows until the page is repaired. Only a mismatch
    /// the page itself shows (a slot past its slot array, or a live tuple) is known to be futile.
    fn settle_release(&self, txn_id: u64, r: RetiredSlot, first: bool) -> bool {
        use std::io::Write;
        match self.release_one(txn_id, &r) {
            Ok(page) => {
                // Review 5's F3: released after all, so no longer an unrecorded mismatch.
                self.unrecorded.lock().unwrap().retain(|(t, s, _)| (*t, *s) != (txn_id, r));
                self.tell_directory(&r, &page);
                false
            }
            Err(ReleaseError::Mismatch(e, found)) => {
                // Review 3's decision 6: the durable record first, the drop after. See
                // `RELEASE_MISMATCHES`.
                let quarantine = release_quarantine(&self.wal.path);
                let key = mismatch_key(txn_id, &r);
                let line = format!("{key}found={found} error={e}\n");
                if let Err(qe) = append_once_durably(&OsFileOps, &quarantine, &key, &line) {
                    // Review 6's caveat 1: the line is kept with the entry, the latest observation
                    // replacing an earlier one, so a later retry can write it without the page.
                    let mut unrecorded = self.unrecorded.lock().unwrap();
                    match unrecorded.iter().position(|(t, s, _)| (*t, *s) == (txn_id, r)) {
                        Some(i) => unrecorded[i].2 = line,
                        None => unrecorded.push((txn_id, r, line)),
                    }
                    drop(unrecorded);
                    if first {
                        let _ = writeln!(
                            std::io::stderr(),
                            "ferrodb: transaction {txn_id} committed, but slot {} of page {} does not match \
                             the log ({e}), and its record could not be written to {} ({qe}); it stays owed, \
                             so the log keeps the record of it, and every checkpoint tries again",
                            r.slot,
                            r.page_id,
                            quarantine.display()
                        );
                    }
                    return true;
                }
                self.unrecorded.lock().unwrap().retain(|(t, s, _)| (*t, *s) != (txn_id, r));
                RELEASE_MISMATCHES.fetch_add(1, Ordering::Relaxed);
                let _ = writeln!(
                    std::io::stderr(),
                    "ferrodb: transaction {txn_id} committed, but slot {} of page {} does not match the \
                     log ({e}; the slot is {found}), so it can never be released; it is recorded in {}, \
                     dropped, not retried, and its bytes stay out of use",
                    r.slot,
                    r.page_id,
                    quarantine.display()
                );
                false
            }
            Err(ReleaseError::Retry(e, seen)) => {
                // Review 6's caveat 1 (the lead's decision, narrowing review 5's F3): an unrecorded
                // mismatch is written by the retry, and its entry leaves only once that write
                // succeeds. Review 7's F4 decides WHICH observation is written:
                // - the page was NOT read (the seam, `fetch_page`, `Page::deserialize`): nothing new is
                //   known about the slot, so the stored line, the last observation, is written, and
                //   counted in `STORED_MISMATCH_LINES`;
                // - the page WAS read and the slot is releasable, and the failure came later
                //   (`serialize`, or the log append on a poisoned log): the mismatch is gone, so the
                //   line records what the page shows now.
                // The release stays owed either way, so neither is counted in `RELEASE_MISMATCHES`,
                // which counts a mismatch when it is dropped.
                let stored = self
                    .unrecorded
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|(t, s, _)| (*t, *s) == (txn_id, r))
                    .map(|(_, _, line)| line.clone());
                if let Some(stored) = stored {
                    let quarantine = release_quarantine(&self.wal.path);
                    let key = mismatch_key(txn_id, &r);
                    let line = match seen {
                        Some(now) => format!("{key}found={now} error={e}\n"),
                        None => stored,
                    };
                    if append_once_durably(&OsFileOps, &quarantine, &key, &line).is_ok() {
                        self.unrecorded.lock().unwrap().retain(|(t, s, _)| (*t, *s) != (txn_id, r));
                        let what = match seen {
                            Some(now) => format!(
                                "was read ({now}, releasable now) but could not be released ({e}); the page/log \
                                 mismatch it showed earlier is recorded as it is now"
                            ),
                            None => {
                                STORED_MISMATCH_LINES.fetch_add(1, Ordering::Relaxed);
                                format!("could not be read again ({e}); its earlier page/log mismatch is recorded")
                            }
                        };
                        let _ = writeln!(
                            std::io::stderr(),
                            "ferrodb: slot {} of page {} {what} in {}, and the release stays owed",
                            r.slot,
                            r.page_id,
                            quarantine.display()
                        );
                    }
                }
                if first {
                    RELEASE_FAILURES.fetch_add(1, Ordering::Relaxed);
                    let _ = writeln!(
                        std::io::stderr(),
                        "ferrodb: transaction {txn_id} committed, but slot {} of page {} could not be \
                         released ({e}); it is retried at every checkpoint, and none truncates the log \
                         until it succeeds",
                        r.slot,
                        r.page_id
                    );
                }
                true
            }
        }
    }

    /// Free one retired slot on its page, logged as a `HeapRelease` under `txn_id`, unchained. The
    /// error says whether the page disagrees with the log (futile to retry) or not.
    fn release_one(&self, txn_id: u64, r: &RetiredSlot) -> Result<Page, ReleaseError> {
        if injected_release_failure() {
            return Err(ReleaseError::Retry(FerroError::Io("injected release failure".into()), None));
        }
        let rec = RecKind::HeapRelease { dir_root: r.dir_root, page_id: r.page_id, slot: r.slot };
        let found = std::cell::Cell::new(None);
        // What the page showed when it was read, releasable or not (review 7's F4). `None` means the
        // closure never ran: the failure came before the page was read.
        let seen = std::cell::Cell::new(None);
        self.apply_then_log(txn_id, r.page_id, &rec, false, |page| {
            let state = match page.slot_arr.get(r.slot as usize) {
                None => "absent",
                Some(slot) if slot.is_retired() => "retired",
                Some(slot) if slot.is_free() => "free",
                Some(_) => "live",
            };
            seen.set(Some(state));
            found.set(matches!(state, "absent" | "live").then_some(state));
            page.release(r.slot as usize)
        })
        .map_err(|e| match found.get() {
            Some(what) => ReleaseError::Mismatch(e, what),
            None => ReleaseError::Retry(e, seen.get()),
        })
    }

    /// Tell the page directory a released page's free space. A failure is counted apart from a
    /// failed release, because the release itself stands (F3; see [`DIRECTORY_UPDATE_FAILURES`]).
    fn tell_directory(&self, r: &RetiredSlot, page: &Page) {
        use std::io::Write;
        let free = page.get_free_space_end() - page.get_free_space_start();
        if let Err(e) = HeapFileManager::open(r.dir_root, self.bp.clone()).update_directory_entry(r.page_id, free) {
            DIRECTORY_UPDATE_FAILURES.fetch_add(1, Ordering::Relaxed);
            let _ = writeln!(
                std::io::stderr(),
                "ferrodb: slot {} of page {} was released, but the page directory was not told ({e}); \
                 its bytes are offered again once the page's entry is next rewritten",
                r.slot,
                r.page_id
            );
        }
    }

    /// **A new database now stands at this log's path** (review 7's F6, the lead's decision): move its
    /// release quarantine aside and forget the replaced database's release state, together, under
    /// `release_retry`. The owed releases and the unrecorded mismatches describe the REPLACED
    /// database's pages; kept, a retry would release against the new pages, and the `Retry` arm would
    /// write the replaced database's line into the new quarantine, which is what the move exists to
    /// prevent. Called by the consensus snapshot install when a `TxnManager` is wired to its log
    /// (`PageStoreSnapshots::with_txn`).
    ///
    /// **D229's state goes with it** (D229 review 2's F11, PREREG amendment 19): the drop intent file moves
    /// aside with the quarantine ([`start_fresh_database`]), and the pending frees are forgotten with their
    /// `DiskManager` quarantine. They name the REPLACED database's page ids: kept in the file, the next open
    /// would free them under the new database; kept here, this manager's next checkpoint would.
    pub fn start_new_incarnation(&self) -> Result<(), FerroError> {
        let _retry = self.release_retry.lock().unwrap();
        start_fresh_database(&self.wal.path).map_err(|e| {
            FerroError::Wal(format!(
                "could not move the replaced database's release quarantine and drop intent aside ({e})"
            ))
        })?;
        self.pending_releases.lock().unwrap().clear();
        self.unrecorded.lock().unwrap().clear();
        // Lock order: `release_retry`, then `pending_frees`, then the pool's quarantine.
        let replaced = std::mem::take(&mut *self.pending_frees.lock().unwrap());
        for p in &replaced {
            self.bp.disk_manager.release_quarantine(&p.intent.pages);
        }
        Ok(())
    }

    /// Retry every pending release (F2), and return how many are still owed. A retry that fails
    /// again is not counted again; one that turns out to be a mismatch is dropped (Q3).
    pub fn retry_pending_releases(&self) -> usize {
        let _retry = self.release_retry.lock().unwrap();
        self.retry_pending_releases_held()
    }

    /// The body of [`TxnManager::retry_pending_releases`], with `release_retry` already held.
    fn retry_pending_releases_held(&self) -> usize {
        let pending = std::mem::take(&mut *self.pending_releases.lock().unwrap());
        let mut still = Vec::new();
        for (txn_id, r) in pending {
            if self.settle_release(txn_id, r, false) {
                still.push((txn_id, r));
            }
        }
        let owed = still.len();
        self.pending_releases.lock().unwrap().extend(still);
        owed
    }

    /// Finish, during recovery, the releases a committed transaction did not get to log. D213.
    ///
    /// Its `HeapRelease` records follow its `Commit` and wait in the log buffer for the next flush,
    /// so a crash in between loses them, and the slots stay retired: nothing would ever free them.
    /// `wal::recovery::recover` works out from the log which slots are still owed, and this
    /// releases them as the commit would have, logged. One that fails waits in the pending list.
    /// `open_recovered`'s checkpoint then still flushes every page and syncs, and keeps only the log,
    /// which is the durable record of it (F2, review 2's N1).
    pub fn finish_releases(&self, txn_id: u64, retired: &[RetiredSlot]) {
        self.release_retired(txn_id, retired);
    }

    /// Undo `writes` newest first, so a key moved twice in one transaction ends where it started.
    ///
    /// Unconditional: it does not check that the entry still holds what this transaction wrote.
    /// Nobody else can have moved it. While the row is uncommitted, another INSERT of the key is
    /// refused (the head is not deleted-for-them), and another UPDATE or DELETE of it is refused
    /// by `check_write_conflict` (the head's `begin_ts` is not committed for them). A check here
    /// could not be made to fire, and an unfireable guard is one no test can hold to account.
    /// Measured by `tests/rollback_index_undo.rs::nobody_else_can_move_a_key_between_its_uncommitted_write_and_its_rollback`.
    ///
    /// **⛔ PRECONDITION: statements on one database are serialised.** Every refusal above is a
    /// check one statement makes against state another statement left, so they hold only if no two
    /// statements interleave INSIDE each other. Today one mutex per database guarantees that:
    /// pgwire's `ServerContext::catalog: Mutex<Catalog>` (`src/pgwire/mod.rs`), which every
    /// connection's statement and the lease scan take, and the CLI's `CatalogLock` (`cli::run_cli`).
    /// `execution::executor::run` takes `&mut Catalog`, so no statement runs without one. A change
    /// that lets two statements on one database run concurrently (an `RwLock`, or a per-table lock)
    /// must re-establish this, or turn this undo into a compare-and-restore.
    ///
    /// **The same precondition makes a DROP's intent complete** (the D229 merge review's §6.1). The
    /// executor collects `Catalog::table_pages` BEFORE [`TxnManager::drop_checkpointed`] takes its
    /// barrier, so a statement interleaving between the two could allocate a page into the table
    /// that the intent does not name, and it would leak (never alias: the intent frees only what it
    /// names).
    ///
    /// Every write is attempted even if one fails, and the first error is returned.
    fn undo_primary_writes(&self, writes: Vec<PrimaryWrite>) -> Result<(), FerroError> {
        let mut first_err = None;
        for w in writes.into_iter().rev() {
            let tree = BPlusTreeManager::<Value, RecordId>::open_shared(w.root, self.bp.clone());
            let undone = match w.prev {
                // Replacing a present key never grows the leaf, so it does not split, EXCEPT on a
                // leaf already exactly full (4069 bytes of entries), which the count split
                // (`BPlusTreeLeafPage::split`, before D225) can leave. The same-size image is then
                // full, `try_write_without_split` answers `Ok(false)`, and the upsert splits once,
                // which can cascade and move the root. `open_shared` publishes a moved root through
                // the cell, so the move is safe; before D225 the split is the count split, with
                // D225's own defect. After D225 (`d225-byte-split`, its review 5 R6), a legacy full
                // leaf holding an entry over `MAX_ENTRY_BYTES` can find no cut, and the upsert
                // refuses: reported below as an undo failure (`first_err`, then `mark_indexes_stale`).
                Some(rid) => tree.upsert(w.key, rid),
                // A net removal, which nothing did before D202. `delete` never rebalances
                // (`handle_underflow` has no caller) and the descent already walks past an
                // empty leaf, so it leaves a sparse leaf and nothing worse.
                None => match tree.delete(&w.key) {
                    Err(FerroError::KeyNotFound) => Ok(()),
                    other => other,
                },
            };
            if let Err(e) = undone {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Remember a schema change and write it to the log.
    ///
    /// Retained rather than merely written, because the next checkpoint truncates whatever is
    /// there. A `DropTable` removes the table from the retained set as well as being logged, so a
    /// replay after truncation does not resurrect a table that no longer exists.
    ///
    /// # An `AlterColumn` is retained as a re-declaration, not as itself — B11
    ///
    /// This is what makes a column-level change survive a restart, and it is the one part of the
    /// design that is not obvious.
    ///
    /// The retained set is replayed at the head of the log after every truncation
    /// ([`Self::replay_schema`]), so whatever is in it is re-emitted to every consumer, repeatedly,
    /// forever. `CREATE_TABLE` is safe there because the feed documents it as a **declaration** —
    /// "this table has this shape" — which a consumer may apply any number of times. An `ALTER` is
    /// **news**: "this column just changed". Retaining the alter itself would re-deliver that news
    /// at every checkpoint, and a consumer applying a rename twice renames a column that no longer
    /// has the old name.
    ///
    /// So an alter updates the *declaration*: the retained record for this table becomes a
    /// `CreateTable` carrying the shape the alter produced. The alter is still appended to the log
    /// in its own right, in log order, exactly once. After a truncation the log re-declares the
    /// table with its **new** shape, which is precisely "the schema survives a restart".
    ///
    /// The record's `columns` must therefore be the table's FULL shape after the change. It is,
    /// for every op: `CreateTable` and `AlterColumn` both carry it, and `DropTable` carries none
    /// because there is no shape left to declare.
    pub fn log_ddl(&self, rec: DdlRecord) -> Result<(), FerroError> {
        {
            let mut log = self.schema_log.lock().unwrap();
            match &rec.op {
                DdlOp::CreateTable => {
                    log.retain(|r| r.dir_root != rec.dir_root);
                    log.push(rec.clone());
                }
                DdlOp::DropTable => log.retain(|r| r.dir_root != rec.dir_root),
                DdlOp::AlterColumn(_) => {
                    log.retain(|r| r.dir_root != rec.dir_root);
                    log.push(DdlRecord {
                        op: DdlOp::CreateTable,
                        table: rec.table.clone(),
                        dir_root: rec.dir_root,
                        time_travel_root: rec.time_travel_root,
                        columns: rec.columns.clone(),
                    });
                }
            }
        }
        self.append_ddl(&rec)?;
        self.wal.flush()
    }

    /// The shape the log would re-declare for `dir_root` after a truncation, if any.
    ///
    /// Exposed so a test can ask what survives a restart without having to truncate to find out.
    pub fn retained_shape(&self, dir_root: u32) -> Option<Vec<(String, DataType, bool)>> {
        self.schema_log
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.dir_root == dir_root)
            .map(|r| r.columns.clone())
    }

    /// Declare a completed DROP again, durably, after a checkpoint truncated past its record: the
    /// open's half of the rule `ddl_unit` follows after a DROP's truncation (D250 review 2's R2-4,
    /// `wal::recovery::open_recovered`). Not retained in the schema: the running process's next
    /// truncating checkpoint drops it, after the provenance forget it exists for has run.
    pub(crate) fn declare_drop_again(&self, r: &DdlRecord) -> Result<(), FerroError> {
        self.append_ddl(r)?;
        self.wal.flush()
    }

    fn append_ddl(&self, r: &DdlRecord) -> Result<(), FerroError> {
        self.wal.append(
            0,
            0,
            &RecKind::Ddl {
                op: r.op.clone(),
                table: r.table.clone(),
                dir_root: r.dir_root,
                time_travel_root: r.time_travel_root,
                columns: r.columns.clone(),
            },
        )?;
        Ok(())
    }

    /// Re-establish every known table's schema at the head of the log.
    ///
    /// Called immediately after a truncation. Without it the log is self-describing only until the
    /// first checkpoint, which is to say almost never.
    fn replay_schema(&self) -> Result<(), FerroError> {
        let records = self.schema_log.lock().unwrap().clone();
        if records.is_empty() {
            return Ok(());
        }
        for r in &records {
            self.append_ddl(r)?;
        }
        self.wal.flush()
    }

    pub fn checkpoint(&self) -> Result<(), FerroError> {
        // Deliberately a SHORT hold — the guard is a temporary and is released before the body
        // runs, which is exactly what this function did before `ddl_checkpointed` existed. Every
        // existing caller therefore keeps its old concurrency behaviour; only the DDL path below
        // needs the answer to stay true while it is acted on, and only it pays for that.
        if !self.att_read().is_empty() {
            return Err(FerroError::Wal("checkpoint with active txns".into()));
        }
        self.checkpoint_locked()
    }

    /// Do a DDL statement's irreversible catalog mutation and its checkpoint as ONE unit, with the
    /// attach table held shut for the whole of it.
    ///
    /// **A8: without this, a refused `CREATE TABLE` had already created the table.** The executor
    /// mutated the catalog and only then called `checkpoint`, which refuses while any transaction
    /// is attached — so the statement returned `Err` over a table that existed, was queryable, and
    /// accepted inserts. Worse, the `Ddl` record is logged *after* the checkpoint, and `log_ddl` is
    /// what puts a table into the retained `schema_log`, so no later checkpoint ever re-declared
    /// it: the table was invisible to every self-describing consumer PERMANENTLY, with its rows
    /// arriving as `unresolved`. The session's own `DDL not allowed in txn` guard does not catch
    /// this, because that guard is per-SESSION while checkpoint admissibility is global — the
    /// transaction that blocks the checkpoint can belong to any other session.
    ///
    /// Taking the decision before the mutation is what I19 did for the same class of defect (a
    /// refused `ALTER` that had already destroyed rows). Merely *asking* first would leave a window:
    /// `begin` takes this same lock, so a transaction starting between the question and the
    /// checkpoint would put the statement right back into the half-done state. Holding the guard
    /// across both closes it — the answer cannot go stale while it is being acted on.
    ///
    /// **While a release is owed, a create SUCCEEDS and only the truncation waits** (review 3's
    /// decision 1). The owed releases are retried before `f`. After `f`, the checkpoint flushes every
    /// page and syncs, keeps the log while anything is still owed, and counts the deferral in
    /// `DEFERRED_CHECKPOINTS`. A create frees no page, so the kept log holds nothing it could
    /// misdirect. At `7cede54` every DDL was refused while a release was owed, so one release that
    /// never succeeds blocked all DDL for ever (review 2's Q3). A DROP frees pages, and goes through
    /// [`TxnManager::drop_checkpointed`].
    ///
    /// Nothing reachable from `f` or from the checkpoint takes `att` or `release_retry`, so this
    /// cannot deadlock on itself. The lock order is `att`, then `release_retry`, then the buffer pool,
    /// and no path takes `att` while it holds `release_retry`. The retry's releases are appended
    /// unchained, straight to the WAL (`apply_then_log` with `chained = false`). The replays after a
    /// truncation append as transaction 0.
    pub fn ddl_checkpointed<T>(
        &self,
        f: impl FnOnce() -> Result<T, FerroError>,
    ) -> Result<T, FerroError> {
        self.ddl_unit(None, f)
    }

    /// [`TxnManager::ddl_checkpointed`] for a DROP. `record` is the DROP's `DropTable` record: it names
    /// the heaps the DROP gives up, the table's heap and its time-travel heap. `pages` is every page
    /// of the table (`Catalog::table_pages`), and `f` unlinks it.
    ///
    /// **D229 (a): the pages are named in a durable intent FIRST, and freed only after the checkpoint
    /// has flushed and synced the unlink** ([`TxnManager::free_pending_frees`]), whether or not that
    /// checkpoint truncated. Until then they are quarantined, and the intent file says which they are
    /// across a crash. `f` frees nothing, so a DROP that fails anywhere frees nothing (the D237
    /// review's F5). The intent is written before the `DropTable` record, so every crash state that
    /// has the record also has the intent, and a DROP the next open completes (below) frees its pages
    /// too.
    ///
    /// **D250: the record is DURABLE before `f`, and recovery skips every record a later DROP names**
    /// (`wal::recovery::recover`). Only a truncation removes the dropped table's records from the log,
    /// and a truncation does not always happen: a WAL pin cancels it, and the checkpoint can fail.
    /// Without the skip, the next open would replay those records onto pages the DROP freed. With it,
    /// a kept log is harmless, so a DROP neither waits for pins nor for releases owed on other tables,
    /// and D229 does not defer its frees either: one mechanism for one hazard (the lead, 10:05Z; a
    /// deferral would never end while a change-feed subscription pins the log, D252).
    ///
    /// - **Refused before anything happens:** while a transaction is open, on a poisoned log, and while
    ///   a page/log mismatch on one of the heaps it frees could not be recorded (review 4's finding 4:
    ///   the truncation would remove the only record of it).
    /// - **`f` fails after the record is durable:** the log is POISONED, and the error returned. The log
    ///   now calls a table dropped that the running catalog may still hold, and every later write to
    ///   it would sit behind a record recovery reads as "skip everything before me". The next open
    ///   completes the DROP (`wal::recovery::open_recovered`), and then carries out the intent.
    /// - **The releases owed on the dropped table** are discarded once the drop has succeeded: their
    ///   pages are freed after the checkpoint, and recovery skips their records.
    /// - **Readers of the change feed** still learn of the DROP when the checkpoint truncates past its
    ///   record: it is appended once more after the replays, where the executor used to log it.
    pub fn drop_checkpointed<T>(
        &self,
        record: DdlRecord,
        pages: Vec<u32>,
        f: impl FnOnce() -> Result<T, FerroError>,
    ) -> Result<T, FerroError> {
        self.ddl_unit(Some((record, pages)), f)
    }

    /// The body of [`TxnManager::ddl_checkpointed`] and [`TxnManager::drop_checkpointed`]. `dropping`
    /// is `None` for a create, and for a DROP its `DropTable` record and every page of the table.
    fn ddl_unit<T>(&self, dropping: Option<(DdlRecord, Vec<u32>)>, f: impl FnOnce() -> Result<T, FerroError>) -> Result<T, FerroError> {
        let att = self.att_read();
        if !att.is_empty() {
            return Err(FerroError::Wal("checkpoint with active txns".into()));
        }
        // Held from the retry to the truncation decision, as for every other retry (C1).
        let _retry = self.release_retry.lock().unwrap();
        // Retried HERE, before `f` (lane §21.2). A DROP frees nothing before its checkpoint (D229).
        self.retry_pending_releases_held();
        let Some((record, pages)) = dropping else {
            let out = f()?;
            self.checkpoint_or_keep_held(false)?;
            return Ok(out);
        };
        let frees = [record.dir_root, record.time_travel_root];
        if let Some(why) = self.wal.poisoned() {
            return Err(FerroError::Wal(format!(
                "DROP refused: the log is poisoned ({why}), so the checkpoint a DROP needs cannot flush \
                 it, and the table would be dropped and the statement reported failed; reopen the \
                 database first"
            )));
        }
        // Review 4's finding 4: the retry above was this DROP's attempt to record them.
        let unrecorded =
            self.unrecorded.lock().unwrap().iter().filter(|(_, r, _)| frees.contains(&r.dir_root)).count();
        if unrecorded > 0 {
            return Err(FerroError::Wal(format!(
                "DROP refused: {unrecorded} page/log mismatch(es) on this table could not be written to the \
                 quarantine at {}, and the DROP would discard them and truncate the log that holds the only \
                 record of them; make that path writable and retry",
                release_quarantine(&self.wal.path).display()
            )));
        }
        // D229 (a): the intent first, durable and its pages quarantined. It says only which pages; the
        // durable catalog, completed from the log by the next open, decides whether they are freed.
        self.record_free_intent(FreeIntent { table: record.table.clone(), dir_root: record.dir_root, pages })?;
        // D250: durable before the mutation. `log_ddl` appends and flushes, and takes the table out
        // of the retained schema.
        let logged_at = self.wal.next_lsn.load(Ordering::SeqCst);
        if let Err(e) = self.log_ddl(record.clone()) {
            // **D250 review 1's F1: fail-stop, as for a Commit whose flush failed.** A failed flush
            // keeps the record in the log buffer, so the next commit's flush would make the DROP
            // durable after the client was told it failed, and the next open would complete it over
            // every row committed since. Poisoned, this process writes nothing more; the next open
            // decides from what reached disk whether the table was dropped.
            self.wal.poison(&format!(
                "the DROP of `{}` could not make its record durable ({e}); the next open decides from what \
                 reached disk whether the table was dropped",
                record.table
            ));
            return Err(e);
        }
        let out = match f() {
            Ok(out) => out,
            Err(e) => {
                // The intent stays undecided: this process frees nothing more, and the next open
                // completes the DROP and then carries the intent out.
                self.wal.poison(&format!(
                    "the DROP of `{}` was logged but did not complete ({e}); the next open completes it",
                    record.table
                ));
                return Err(e);
            }
        };
        // The unlink happened: the checkpoint below carries the intent out once it has synced.
        self.decide_recorded(record.dir_root);
        self.discard_releases_on(&frees);
        let outcome = self.checkpoint_or_keep_held(false)?;
        if outcome == CheckpointOutcome::KeptByPin {
            // Review 5's F4: counted and printed. On this branch it is expected under a pin, and
            // harmless: recovery skips every record the DROP's durable record follows (D250).
            use std::io::Write;
            KEPT_LOG_DROPS.fetch_add(1, Ordering::Relaxed);
            let _ = writeln!(
                std::io::stderr(),
                "ferrodb: a WAL pin cancelled this DROP's truncation, so the dropped table's records stay in \
                 the log; recovery skips them, because the DROP's record follows them"
            );
        }
        if self.wal.base_lsn.load(Ordering::SeqCst) > logged_at {
            // The truncation discarded the record with the table's own. A reader starting at the new
            // base must still learn the table is gone.
            self.append_ddl(&record)?;
            self.wal.flush()?;
        }
        Ok(out)
    }

    /// Write `intent` into the durable intent file beside the other pending ones, then quarantine
    /// its pages and hold it, undecided. Before the DROP's `DropTable` record (D229 (a)).
    ///
    /// **Refused, before the intent, the quarantine and the DROP's record are written, when a pending
    /// intent already names one of its pages** (the D229 merge review's §4.3, the lead's decision). Not
    /// before everything: `ddl_unit` has already retried the owed releases, which can write pages and
    /// append their records (D229 review 2's F3). A live table's page in a pending intent is
    /// a state this build never makes: an intent's pages are quarantined until it is gone (A4), so no
    /// table created meanwhile holds one, and a DROP whose record or mutation fails poisons the log
    /// (D250), and a poisoned log refuses the next DROP before this runs. So a shared page means the
    /// intent file is damaged or stale, and carrying out both intents would free a page twice, or
    /// free it under its owner. This replaced a branch that superseded such an intent, which nothing
    /// could reach.
    fn record_free_intent(&self, intent: FreeIntent) -> Result<(), FerroError> {
        let mut pending = self.pending_frees.lock().unwrap();
        let mine: HashSet<u32> = intent.pages.iter().copied().collect();
        if let Some((other, shared)) = pending.iter().find_map(|p| {
            let shared = p.intent.pages.iter().filter(|page| mine.contains(*page)).count();
            (shared > 0).then_some((p, shared))
        }) {
            return Err(FerroError::Wal(format!(
                "DROP refused: {shared} page(s) of `{}` are already named by a pending DROP intent (for `{}`, \
                 whose heap starts at page {}), which this build never records for a live table; the intent \
                 file {} is damaged or stale, and carrying both out would free a page twice or under its owner",
                intent.table,
                other.intent.table,
                other.intent.dir_root,
                free_intent::intent_path(&self.wal.path).display()
            )));
        }
        let mut all: Vec<FreeIntent> = pending.iter().map(|p| p.intent.clone()).collect();
        all.push(intent.clone());
        free_intent::store(&*self.file_ops, &self.wal.path, &all)?;
        self.bp.disk_manager.quarantine(&intent.pages);
        pending.push(PendingFree { intent, decided: false });
        Ok(())
    }

    /// The DROP that recorded the intent for `dir_root` has unlinked its table.
    fn decide_recorded(&self, dir_root: u32) {
        let mut pending = self.pending_frees.lock().unwrap();
        if let Some(p) = pending.iter_mut().rev().find(|p| p.intent.dir_root == dir_root && !p.decided) {
            p.decided = true;
        }
    }

    /// **D229's A1, at an open: read the intents a crashed process left, and quarantine their pages,
    /// BEFORE `recover` runs.** Recovery's directory repair allocates pages. A DROP whose frees had
    /// begun (or failed part-way) left some of its pages clear on disk, and without the quarantine the
    /// repair could hand one to a live table, which the intent would then free a second time.
    /// Returns how many intents were read. They stay undecided until
    /// [`TxnManager::decide_free_intents`].
    pub fn adopt_free_intents(&self) -> Result<usize, FerroError> {
        let intents = free_intent::load(&self.wal.path)?;
        let n = intents.len();
        let mut pending = self.pending_frees.lock().unwrap();
        for intent in intents {
            self.bp.disk_manager.quarantine(&intent.pages);
            pending.push(PendingFree { intent, decided: false });
        }
        Ok(n)
    }

    /// **Decide every undecided intent by the durable catalog, after `Catalog::open`.** `present`
    /// answers whether a table whose heap starts at the given directory page is in the catalog.
    ///
    /// - Present: the DROP's unlink never reached the disk, and nothing was freed, because the frees
    ///   wait for the checkpoint that makes the unlink durable. The intent is removed from the file
    ///   (durably, with its directory synced) and only then are its pages released (A4).
    /// - Absent: the unlink is durable, so the intent is decided, and carried out after the open's
    ///   checkpoint ([`TxnManager::free_pending_frees`]).
    ///
    /// Refuses the open if the file cannot be rewritten: an intent left on disk for a table that is
    /// present would be carried out after a later DROP of a table recreated under it.
    pub fn decide_free_intents(&self, present: impl Fn(u32) -> bool) -> Result<(), FerroError> {
        let mut pending = self.pending_frees.lock().unwrap();
        let (keep, back): (Vec<PendingFree>, Vec<PendingFree>) =
            pending.drain(..).partition(|p| p.decided || !present(p.intent.dir_root));
        let kept: Vec<FreeIntent> = keep.iter().map(|p| p.intent.clone()).collect();
        if !back.is_empty() {
            if let Err(e) = free_intent::store(&*self.file_ops, &self.wal.path, &kept) {
                pending.extend(keep);
                pending.extend(back);
                return Err(e);
            }
            for p in &back {
                self.bp.disk_manager.release_quarantine(&p.intent.pages);
            }
        }
        *pending = keep.into_iter().map(|p| PendingFree { decided: true, ..p }).collect();
        Ok(())
    }

    /// Fsync the file at `path` and its directory through this manager's file ops, so a file an earlier
    /// process wrote without syncing (a stale-indexes marker written before review 3's decision 7) is
    /// durable before anything rests on it: D229's recovery reset frees pages only once the trigger
    /// that makes every later open rebuild cannot vanish (the design review's caveat 1).
    pub(crate) fn sync_file_and_directory(&self, path: &Path) -> std::io::Result<()> {
        self.file_ops.sync_file(path)?;
        self.file_ops.sync_dir(crate::storage::atomic_file::parent_dir(path))
    }

    /// Every page of every intent not yet carried out: the recovery reset keeps them (the review's
    /// A2). A reset that freed one would let the rebuild take it, and the intent free it again.
    pub fn pending_free_pages(&self) -> Vec<u32> {
        self.pending_frees.lock().unwrap().iter().flat_map(|p| p.intent.pages.iter().copied()).collect()
    }

    /// **Carry out every decided intent** (D229 (a)). Returns how many intents are still pending.
    ///
    /// Per intent: the pages are freed in one batch (refused whole if one is pinned), the page file
    /// is synced, the intent file is rewritten without it (its directory synced when it goes empty,
    /// A4), and only then is the quarantine released. A failure at any step keeps the intent and its
    /// quarantine, counts in [`FREE_FAILURES`], prints a line, and is retried at the next checkpoint or
    /// open: a free is idempotent, and a quarantined page cannot have been handed out meanwhile.
    ///
    /// **It does not ask whether the log still names the pages** (the lead's decision, 10:05Z: the
    /// review's A3 is carried by D250 instead, whose recovery skips every record of a dropped heap).
    /// Only a DROP's heap and time-travel pages have log records at all.
    ///
    /// Called right after every checkpoint's sync, whatever the checkpoint then does with the log
    /// (every page is flushed and synced by then), and by `open_recovered` after its own checkpoint
    /// or in place of one.
    pub fn free_pending_frees(&self) -> usize {
        use std::io::Write;
        let mut pending = self.pending_frees.lock().unwrap();
        if !pending.iter().any(|p| p.decided) {
            return pending.len();
        }
        let (ready, mut wait): (Vec<PendingFree>, Vec<PendingFree>) = pending.drain(..).partition(|p| p.decided);
        // The unlink every ready intent rests on is durable before anything is freed. A checkpoint
        // has just synced, and an open that ran none read its catalog from a file nothing synced yet.
        if !ready.is_empty() {
            if let Err(e) = self.bp.disk_manager.sync() {
                FREE_FAILURES.fetch_add(1, Ordering::Relaxed);
                let _ = writeln!(std::io::stderr(), "ferrodb: a DROP's pages wait: the page file could not be synced ({e})");
                wait.extend(ready);
                *pending = wait;
                return pending.len();
            }
        }
        let mut freed = Vec::new();
        for p in ready {
            match self.bp.free_pages(&p.intent.pages) {
                Ok(()) => freed.push(p),
                Err(e) => {
                    FREE_FAILURES.fetch_add(1, Ordering::Relaxed);
                    let _ = writeln!(
                        std::io::stderr(),
                        "ferrodb: the {} page(s) of dropped table {} were not freed ({e}); they stay allocated, and the next checkpoint or open tries again",
                        p.intent.pages.len(),
                        p.intent.table
                    );
                    wait.push(p);
                }
            }
        }
        if !freed.is_empty() {
            let remaining: Vec<FreeIntent> = wait.iter().map(|p| p.intent.clone()).collect();
            match self.bp.disk_manager.sync().and_then(|()| free_intent::store(&*self.file_ops, &self.wal.path, &remaining)) {
                Ok(()) => {
                    for p in &freed {
                        self.bp.disk_manager.release_quarantine(&p.intent.pages);
                    }
                }
                Err(e) => {
                    FREE_FAILURES.fetch_add(1, Ordering::Relaxed);
                    let _ = writeln!(
                        std::io::stderr(),
                        "ferrodb: a DROP's pages were freed, but their intent could not be made to say so ({e}); they stay out of use, and the next checkpoint or open frees them again"
                    );
                    wait.extend(freed);
                }
            }
        }
        *pending = wait;
        pending.len()
    }

    /// Discard the owed releases on the heaps whose directories start at `roots`, which a DROP has
    /// just unlinked: its pages are freed after its checkpoint (D229), so there is nothing left for
    /// them to release. `release_retry` must be held.
    fn discard_releases_on(&self, roots: &[u32]) {
        use std::io::Write;
        let discarded = {
            let mut pending = self.pending_releases.lock().unwrap();
            let before = pending.len();
            pending.retain(|(_, r)| !roots.contains(&r.dir_root));
            before - pending.len()
        };
        if discarded > 0 {
            let _ = writeln!(
                std::io::stderr(),
                "ferrodb: DROP discarded {discarded} release(s) owed on the dropped table: its pages are freed, \
                 so nothing is left to release"
            );
        }
    }

    /// The checkpoint after a DDL that frees no page and mutates first: CREATE INDEX and CREATE
    /// FULLTEXT INDEX (review 3's caveat 1 and decision 1).
    ///
    /// While a release is owed it flushes every page and syncs, keeps the log, counts the deferral,
    /// and answers `Ok`. At `7cede54` those statements called `checkpoint`, which then refused after
    /// the index was built and flushed. The client was told the DDL failed over an index that existed
    /// and was used, which is A8's shape.
    ///
    /// Refuses while a transaction is open, as `checkpoint` does. They are not moved under
    /// [`TxnManager::ddl_checkpointed`]: that would hold `att` shut across the index's backfill, so
    /// every `begin`, and every reader whose cached snapshot missed, would wait O(rows). An open
    /// transaction elsewhere therefore still refuses them after the index is built. That is A8's
    /// shape for indexes, it predates this lane, and it is unchanged here.
    pub fn ddl_checkpoint(&self) -> Result<(), FerroError> {
        if !self.att_read().is_empty() {
            return Err(FerroError::Wal("checkpoint with active txns".into()));
        }
        self.checkpoint_or_keep_locked(true).map(|_| ())
    }

    /// The body of `checkpoint`: a checkpoint that keeps the log for owed releases is REFUSED here,
    /// after it flushed. Takes no `att`; `checkpoint` asks it first.
    fn checkpoint_locked(&self) -> Result<(), FerroError> {
        match self.checkpoint_or_keep_locked(true)? {
            CheckpointOutcome::Truncated | CheckpointOutcome::KeptByPin => Ok(()),
            CheckpointOutcome::KeptForOwed(owed) => Err(FerroError::Wal(format!(
                "checkpoint refused: {owed} release(s) owed by committed transactions still fail, and \
                 truncating the log would lose the record of the bytes they hold; every page was \
                 flushed and the log is kept, and the next checkpoint retries them"
            ))),
        }
    }

    /// A checkpoint that answers what it did with the log instead of refusing (a [`CheckpointOutcome`]):
    /// `Truncated`, `KeptForOwed(n)` when every page was flushed and the log KEPT for `n` owed
    /// releases (retried first), or `KeptByPin` when a WAL pin cancelled the truncation. The automatic
    /// trigger uses it; `checkpoint` turns `KeptForOwed` into a refusal. Refuses while a transaction
    /// is open, as `checkpoint` does. (It answered `Ok(0)` for both "truncated" and "kept by a pin"
    /// until review 5's F4.)
    pub fn checkpoint_keeping_owed(&self) -> Result<CheckpointOutcome, FerroError> {
        if !self.att_read().is_empty() {
            return Err(FerroError::Wal("checkpoint with active txns".into()));
        }
        self.checkpoint_or_keep_locked(true)
    }

    /// [`TxnManager::checkpoint_keeping_owed`] WITHOUT the retry, for `open_recovered`. It runs right
    /// after the index rebuild freed and reallocated pages on disk, which is D229's window.
    /// `finish_releases` has just tried every owed release, so a retry here would only put page reads
    /// and log writes between those frees and the sync. The window then holds what `9aa6968`'s
    /// checkpoint held: the log flush, the pool flush and the sync (lane §21.2).
    pub fn checkpoint_after_frees(&self) -> Result<CheckpointOutcome, FerroError> {
        if !self.att_read().is_empty() {
            return Err(FerroError::Wal("checkpoint with active txns".into()));
        }
        self.checkpoint_or_keep_locked(false)
    }

    /// How many releases are owed now: committed, failed, and waiting for a retry (F2). Reads only;
    /// [`TxnManager::retry_pending_releases`] retries them.
    pub fn owed_releases(&self) -> usize {
        self.pending_releases.lock().unwrap().len()
    }

    /// [`TxnManager::checkpoint_or_keep_held`], taking `release_retry` for it.
    fn checkpoint_or_keep_locked(&self, retry: bool) -> Result<CheckpointOutcome, FerroError> {
        let _retry = self.release_retry.lock().unwrap();
        self.checkpoint_or_keep_held(retry)
    }

    /// **Review 2's N1 (the lead's decision): refuse the TRUNCATION, never the flush.**
    ///
    /// A release still owed must not be truncated out of the log, which is the only durable record of
    /// it (F2). But the flush must still happen. At `368d0e1` the refusal returned before it. The
    /// open's index rebuild had then freed every old tree and reallocated its pages, which
    /// `deallocate` and `new_page` do ON DISK at once. The new nodes and the catalog stayed in the
    /// pool, so the next open walked an old root that was now a zero page, and failed at every open
    /// from then on. So: retry (when `retry`), then flush the log and every page and sync, and skip
    /// only the truncation and the replays after it. The caller holds `release_retry` across the retry
    /// and the decision (C1).
    ///
    /// ⚠ **The reason above is history since D229:** the rebuild walks no old tree (it resets them by
    /// identity, `wal::recovery`), so an unflushed rebuild no longer bricks the next open, which
    /// simply rebuilds again. The flush is still owed to everything else a kept log leaves in the
    /// pool, and to D229's frees, which may run only once what they free is durably unnamed.
    ///
    /// **Carries out every decided DROP intent right after its sync** (D229,
    /// [`TxnManager::free_pending_frees`]), whatever happens to the truncation after. Their failures
    /// are counted, not returned.
    ///
    /// A kept log is counted in `DEFERRED_CHECKPOINTS`. The stderr line is printed only when the
    /// log-keeping state CHANGES: once when a checkpoint first keeps the log, and once when one
    /// truncates again (review 3's decision 3).
    fn checkpoint_or_keep_held(&self, retry: bool) -> Result<CheckpointOutcome, FerroError> {
        use std::io::Write;
        let owed = if retry { self.retry_pending_releases_held() } else { self.owed_releases() };
        self.wal.flush()?;
        self.bp.flush_all()?;
        self.bp.disk_manager.sync()?;
        // D229: every page is durable, the unlink of any decided DROP with it. Whether this checkpoint
        // then truncates, keeps the log, or has its truncation cancelled by a pin makes no difference
        // to the frees: a record of a dropped heap left in the log is D250's to skip at redo.
        self.free_pending_frees();
        if owed > 0 {
            DEFERRED_CHECKPOINTS.fetch_add(1, Ordering::Relaxed);
            if !self.keeping_log.swap(true, Ordering::SeqCst) {
                let _ = writeln!(
                    std::io::stderr(),
                    "ferrodb: checkpoints now flush every page but keep the log: {owed} release(s) owed by \
                     committed transactions still fail. The automatic checkpoint retries them once per {} \
                     commits, every DDL and explicit checkpoint retries them, and every open attempts them \
                     again from the log. This is printed again when they clear; `deferred_checkpoints` counts \
                     every checkpoint that kept the log meanwhile",
                    checkpoint_interval()
                );
            }
            return Ok(CheckpointOutcome::KeptForOwed(owed));
        }
        // Read before `truncate`, which keeps the log, and answers `Ok`, while a WAL pin is below its
        // end (review 4's finding 5).
        let base = self.wal.base_lsn.load(Ordering::SeqCst);
        let end = self.wal.next_lsn.load(Ordering::SeqCst);
        // The issued watermark is exactly what the old counter held, so the WAL header — 24 bytes
        // with no spare room — keeps its meaning and its format.
        self.wal.truncate(self.txn_ids.issued_through())?;
        self.commits_since_checkpoint.store(0, Ordering::SeqCst);
        if base < end && self.wal.base_lsn.load(Ordering::SeqCst) == base {
            // CANCELLED by a pin: nothing was discarded, so nothing is replayed (a replay would put a
            // second copy of every declaration into the kept log), the owed state is not "settled",
            // and it is counted as the deferral it is. **Residual, stated:** an empty log that gets an
            // append between the reads above and `truncate` can be misread as truncated;
            // `truncate` does not report its decision. The log IS empty after every restart until the
            // first DDL (review 5's F5), and `consensus/snapshot.rs` detects the same fact with the
            // opposite residual: D216's `Truncation` result is to replace both.
            DEFERRED_CHECKPOINTS.fetch_add(1, Ordering::Relaxed);
            return Ok(CheckpointOutcome::KeptByPin);
        }
        if self.keeping_log.swap(false, Ordering::SeqCst) {
            let _ = writeln!(std::io::stderr(), "ferrodb: the owed releases are settled, and checkpoints truncate the log again");
        }
        // The truncation just discarded every DDL record. Put them back, or a log reader starting
        // at the new base has no way to know what any table is.
        self.replay_schema()?;
        // And every run declaration, for the same reason: a reader starting at the new base would
        // otherwise have no way to name the database's writers.
        self.replay_runs()?;
        Ok(CheckpointOutcome::Truncated)
    }

    /// The transaction's snapshot, which every read and write inside it goes through, so this is also
    /// where an `Aborting` transaction is refused (D211). A transaction whose rollback did not finish
    /// holds half-undone rows; letting it keep reading or writing is how it would act on them.
    pub fn snapshot_of(&self, txn_id: u64) -> Result<Snapshot, FerroError> {
        let att = self.att_read();
        let entry = att.get(&txn_id).ok_or_else(|| FerroError::Txn("no snapshot for txn".into()))?;
        if matches!(entry.status, TxnStatus::Aborting) {
            return Err(Self::rolling_back(txn_id));
        }
        entry.snapshot.clone().ok_or_else(|| FerroError::Txn("no snapshot for txn".into()))
    }

    /// Whether `txn_id` is open and its rollback began but did not finish. D211.
    pub fn is_aborting(&self, txn_id: u64) -> bool {
        self.att_read().get(&txn_id).is_some_and(|e| matches!(e.status, TxnStatus::Aborting))
    }

    fn rolling_back(txn_id: u64) -> FerroError {
        FerroError::Txn(format!(
            "transaction {txn_id} is rolling back and its undo has not finished; only ROLLBACK is \
             accepted, and it retries the undo"
        ))
    }

    /// Apply a committed [`crate::consensus::Command::TxnIdRange`].
    ///
    /// Refuses a range addressed to another node, and any range at all on a standalone node. A
    /// re-delivered range is a no-op — a committed round may arrive twice, and re-offering ids this
    /// node has already issued is how two transactions get the same id on ONE node.
    pub fn apply_txn_id_grant(
        &self,
        node: crate::consensus::NodeId,
        lo: u64,
        hi: u64,
    ) -> Result<(), FerroError> {
        self.txn_ids.apply_grant(node, lo, hi)?;
        Ok(())
    }

    /// Raise the id watermark to `at_least`, discarding any granted range below it.
    ///
    /// Recovery's half of the seed. `TxnManager::new` takes the WAL header's id, and the header is
    /// only advanced at checkpoint, so between checkpoints it lags what was actually issued;
    /// `wal::recovery::recover` scans the retained records and calls this with one past the
    /// highest id it saw. Monotone, because both inputs are lower bounds on what was issued and a
    /// counter that could be lowered would re-issue.
    ///
    /// It does **not** create a grant. A cluster member that recovers a log full of ids still
    /// holds no range and still refuses to begin a transaction until the leader grants one — the
    /// watermark says what was used, never what may be used.
    pub fn raise_next_txn_id(&self, at_least: u64) {
        // **`high_water` is half of a snapshot and the ATT does not cover it**, so a raise must
        // move the version — and it must move it ATOMICALLY with the watermark, under the same
        // lock `read_snapshot_cached`'s miss path reads the watermark under.
        //
        // The first fix raised the watermark and then bumped the version, unlocked. The
        // strengthened race test caught it on the next suite run: a reader loaded version V, hit
        // its cache (high_water H), and between the raise and the bump the locked read already
        // saw H+1000 while the version still said V — "the version did not move (3758) but the
        // cached high_water is 626254 against the locked 627254". Bumping first is wrong the other
        // way (a miss at V+1 can read the watermark before the raise and cache it as V+1). Only
        // doing both inside one critical section makes (watermark, version) a single fact.
        //
        // The only other writer of the watermark is `begin`, which `take`s an id inside the
        // `att_write` critical section that also inserts it — already atomic by construction.
        // `apply_txn_id_grant` moves the ACCEPTED range, never `issued`.
        let _att = self.att_read();
        self.txn_ids.raise_issued_through(at_least);
        self.att_version.fetch_add(1, Ordering::Release);
    }

    /// The id watermark: everything below it has been issued. Diagnostic and recovery-facing.
    pub fn next_txn_id(&self) -> u64 {
        self.txn_ids.issued_through()
    }

    /// Apply a committed [`crate::consensus::Command::Checkpoint`].
    ///
    /// The replicated entry point for the work the commit counter drives on a single node. Every
    /// node applies this at the same round, so every node truncates its WAL at the same *logical*
    /// point even though the byte offset differs — which is the whole reason the decision has to
    /// travel in the log rather than be taken locally.
    pub fn apply_checkpoint(&self) -> Result<(), FerroError> {
        self.checkpoint()
    }

    /// Whether this node has accumulated enough commits that it wants a checkpoint.
    ///
    /// How a leader loop knows to propose [`crate::consensus::Command::Checkpoint`]. On a standalone
    /// node this is transiently true at most until the next commit, because there the automatic
    /// trigger fires and resets it.
    pub fn checkpoint_due(&self) -> bool {
        self.commits_since_checkpoint.load(Ordering::SeqCst) >= checkpoint_interval()
    }

    /// Read-only borrow of the active-transaction table. Bumps no version: see [`AttGuard`].
    ///
    /// Returns a `Deref`-ONLY guard, deliberately. A `MutexGuard` is `DerefMut`, so
    /// `att_read().remove(&id)` would compile inside this module and change the active set
    /// without moving `att_version` — the property this file rests on, reduced to a comment. A
    /// review called that out while the guard still handed back the raw `MutexGuard`; now the
    /// type says it. `TxnEntry`'s own interior mutability (`last_lsn`) is still reachable, which
    /// is the point: that field is in no snapshot.
    pub fn att_read(&self) -> AttReadGuard<'_> {
        AttReadGuard { guard: self.att.lock().unwrap() }
    }

    /// Write borrow of the active-transaction table. **Every mutation goes through here** —
    /// see [`AttGuard`] and `tests/d59_snapshot_cache.rs`.
    pub fn att_write(&self) -> AttGuard<'_> {
        AttGuard { guard: Some(self.att.lock().unwrap()), version: &self.att_version, dirty: false }
    }

    /// The active-transaction table's version. See [`TxnManager::att_version`].
    pub fn att_version(&self) -> u64 {
        self.att_version.load(Ordering::Acquire)
    }

    /// **D59 — a snapshot without the lock when nothing has changed.**
    ///
    /// On a hit — the active-transaction table has not changed since this thread last built a
    /// snapshot of THIS manager — no lock is taken and nothing is allocated: one `Acquire` load,
    /// which is D51's ×7.8 class rather than the mutex's ×0.12. On a miss it does exactly what
    /// [`TxnManager::read_snapshot`] does, and remembers the result.
    ///
    /// **A thread-local rather than a per-connection field** because the cached value is not
    /// connection-specific: a snapshot is `(high_water, active set)`, a global fact about the
    /// manager at a version, so any thread may reuse any snapshot labelled with the version it
    /// observes. No call site has to thread a cache through `ReadCtx`, and a server that moves a
    /// connection between threads cannot desynchronise anything.
    ///
    /// The version is read FIRST and stored with the snapshot it labels; `AttGuard` publishes the
    /// bump inside the critical section, so a snapshot labelled V reflects every change up to V.
    /// A cache that is behind simply misses and rebuilds — the failure direction is a slow read,
    /// never a stale one.
    pub fn read_snapshot_cached(&self) -> Arc<Snapshot> {
        let v = self.att_version();
        let hit = SNAPSHOT_CACHE.with(|c| match &*c.borrow() {
            Some((id, cached_v, snap)) if *id == self.id && *cached_v == v => Some(Arc::clone(snap)),
            _ => None,
        });
        if let Some(snap) = hit {
            return snap;
        }
        // **A PRECISION step, not a guard — and it was documented as a guard until its mutant
        // survived.** The safety comes from `AttGuard` bumping INSIDE the critical section that
        // changed the table, so a label can never be newer than the content it names; that is the
        // only direction that could serve a stale snapshot, and moving the bump after the unlock
        // fails `d59_snapshot_cache`'s race test. Re-reading the version here only avoids
        // labelling this snapshot with a version a writer has already moved past, which would cost
        // the next reader a needless miss. Slower without it, never wrong.
        let att = self.att_read();
        let v = self.att_version.load(Ordering::Acquire);
        let snap = Arc::new(Snapshot {
            high_water: self.txn_ids.issued_through(),
            active: att.keys().copied().collect(),
        });
        drop(att);
        SNAPSHOT_CACHE.with(|c| *c.borrow_mut() = Some((self.id, v, Arc::clone(&snap))));
        snap
    }

    pub fn read_snapshot(&self) -> Snapshot {
        let att = self.att_read();
        // A read of the watermark, not a take: a snapshot's high water is "everything below this
        // was issued", which the watermark answers without consuming anything and therefore
        // without ever refusing. That is why `read_snapshot` stays infallible.
        Snapshot { high_water: self.txn_ids.issued_through(), active: att.keys().copied().collect() }
    }
}

/// Undo a `HeapInsert` on its page: free the slot. Page-level only. `TxnManager::apply_then_log`
/// owns the latch, the ordering against the CLR, and the LSN (D211).
pub fn undo_insert(page: &mut Page, slot: u16) -> Result<(), FerroError> {
    page.delete(slot as usize)
}

/// Undo a `HeapDelete` (the relocation arm of an update): put the old bytes back in the slot.
/// Since D213 the slot is RETIRED, so this is in place and needs no room. It can still be REFUSED
/// when the page disagrees with the log, or, for a free slot on a page from before D213, for want
/// of room (`Page::restore_at`, D210), and then nothing is logged.
pub fn undo_delete(page: &mut Page, slot: u16, old: &[u8]) -> Result<(), FerroError> {
    page.restore_at(slot as usize, old)
}

/// Undo a `HeapUpdate`: write the old image back into the slot. The old image is the slot's whole
/// capacity as it was when the update read it, and since D213 a slot's capacity never shrinks
/// (`Page::update` keeps it on a shrink), so this is in place and needs no room.
pub fn undo_update(page: &mut Page, slot: u16, old: &[u8]) -> Result<(), FerroError> {
    page.update(slot as usize, Tuple::new(old.to_vec()))
}

pub fn stamp_page_lsn(bp: &BufferPoolManager, page_id: u32, lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, lsn, |_| Ok(()))
}

/// Write the stale-indexes marker beside the log at `wal_path`, holding `why`. One writer for both
/// callers: a failed index undo (`TxnManager::mark_indexes_stale`) and a poisoned log
/// (`WalManager::poison`, review 2's C3).
///
/// **Durable (review 3's decision 7).** The file is fsynced, and so is its directory on unix, before
/// this returns. Unsynced, a power loss could keep an evicted index page the marker was written to
/// repair and lose the marker, and the next open would then skip the rebuild.
pub(crate) fn write_stale_indexes_marker(wal_path: &Path, why: &str) -> std::io::Result<()> {
    use std::io::Write;
    let path = stale_indexes_marker(wal_path);
    let mut file = std::fs::File::create(&path)?;
    file.write_all(format!("{why}\n").as_bytes())?;
    file.sync_all()?;
    sync_directory_of(&path)
}

/// A mismatch's line in the quarantine starts with this, and `append_once_durably` records each once.
fn mismatch_key(txn_id: u64, r: &RetiredSlot) -> String {
    format!("txn={txn_id} dir_root={} page={} slot={} ", r.dir_root, r.page_id, r.slot)
}

/// Where a failed index undo leaves its marker: beside the log, `<wal path>.stale-indexes`. D205's
/// C1 correction. `wal::recovery::open_recovered` rebuilds every tree when it finds one.
pub fn stale_indexes_marker(wal_path: &Path) -> PathBuf {
    let mut marker = wal_path.as_os_str().to_os_string();
    marker.push(".stale-indexes");
    PathBuf::from(marker)
}

/// **Review 5's F6: a fresh log starts a fresh quarantine.** The dedupe key (`append_once_durably`)
/// has no database incarnation, and a database created again at the same path reissues transaction
/// ids, so an earlier database's quarantine would make a colliding record of the new one look
/// already recorded. The earlier file is moved aside to [`aside_path`], not deleted: it is evidence.
///
/// Called, through [`start_fresh_database`], where a new incarnation starts at this path, BEFORE
/// anything marks it started:
/// - `WalManager::with_storage`, before it writes a fresh log's header (review 6's caveat 2), so a
///   failed move leaves the log empty and the next open tries again;
/// - the consensus snapshot install, before it discards the replaced database's log (review 6's F6).
///
/// **Stated, not covered:** a database restored together with its non-empty log keeps its quarantine,
/// which is that incarnation's own. An in-place restore outside the install
/// (`replication::backup::restore` on its own) does not carry the quarantine and does not move it, so
/// the file left at the path is the REPLACED database's.
pub(crate) fn start_fresh_quarantine(wal_path: &Path) -> std::io::Result<()> {
    start_fresh_quarantine_at(wal_path, now_nanos())
}

/// [`start_fresh_quarantine`] with the clock passed in: the seam a test uses to move twice at one
/// clock reading through the caller (review 7's F5).
fn start_fresh_quarantine_at(wal_path: &Path, nanos: u128) -> std::io::Result<()> {
    move_aside_at(&release_quarantine(wal_path), nanos)
}

/// **A new database at this path: every file beside the log that describes the replaced one goes
/// aside** (D229 review 2's F11). The one fresh-database transition for the files, called by a fresh log
/// (`WalManager::with_storage` at length 0) and by the snapshot install when no transaction manager is
/// wired ([`TxnManager::start_new_incarnation`] is the install's door when one is), so neither can move
/// one of these and keep the other:
/// - the release quarantine ([`start_fresh_quarantine`], review 6's F6);
/// - D229's drop intent (`wal::free_intent`). It names page ids of the REPLACED database. The next open
///   would adopt it, find its table absent from the new catalog, and free those ids after its
///   checkpoint, which may be live pages of the new database.
///
/// Moved, not deleted: both are evidence. In this order, at one clock reading, and idempotent: a retry
/// after a failure finds the quarantine already gone and moves the intent. The quarantine's stated
/// restore gap (an in-place `replication::backup::restore` outside the install moves neither) holds
/// for the intent too.
pub(crate) fn start_fresh_database(wal_path: &Path) -> std::io::Result<()> {
    let nanos = now_nanos();
    start_fresh_quarantine_at(wal_path, nanos)?;
    move_aside_at(&free_intent::intent_path(wal_path), nanos)
}

/// The clock reading an aside name carries (see [`aside_path`]).
fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Move `current` to an unused [`aside_path`] and sync its directory; nothing to do when it does not
/// exist.
fn move_aside_at(current: &Path, nanos: u128) -> std::io::Result<()> {
    match std::fs::symlink_metadata(current) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
        Ok(_) => {}
    }
    std::fs::rename(current, aside_path(current, nanos)?)?;
    sync_directory_of(current)
}

/// Where [`move_aside_at`] moves `current`: the first of `<current>.before-<nanos>`,
/// `<current>.before-<nanos>-1`, `-2`, ... that does not exist. Review 6's finding 5: a clock before
/// 1970 gives `nanos == 0` every time, and `rename` silently replaces an existing target, which would
/// lose the earlier copy.
///
/// **Only `NotFound` is a free name** (review 7's F5). Any other stat error is returned, so the move
/// refuses visibly: an over-long candidate (`ENAMETOOLONG`) used to count as free, and the rename
/// then failed on it.
///
/// **What guards the window between the check and the rename** is `rename`'s own refusal, not a
/// lock (review 7's F5: not every opener holds the `DbLock`, for instance
/// `examples/outer_runtime_lock.rs` and every test). Two fresh opens racing: once the first has
/// moved `current`, the second's `rename(current, ..)` fails with `ENOENT`, which fails that open. A
/// copy could be replaced only if one opener's check-then-rename spanned another's whole move, a
/// new database's lifetime writing a new quarantine, and the same `nanos`.
fn aside_path(current: &Path, nanos: u128) -> std::io::Result<PathBuf> {
    for n in 0u32.. {
        let mut aside = current.as_os_str().to_os_string();
        aside.push(if n == 0 { format!(".before-{nanos}") } else { format!(".before-{nanos}-{n}") });
        let aside = PathBuf::from(aside);
        match std::fs::symlink_metadata(&aside) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(aside),
            Err(e) => return Err(e),
            Ok(_) => continue,
        }
    }
    Err(std::io::Error::other("no unused name below u32::MAX"))
}

/// Where a release's page/log MISMATCH is recorded: beside the log, `<wal path>.release-quarantine`,
/// one line per release. Review 3's decision 6; see [`RELEASE_MISMATCHES`]. Most lines are written
/// just before the release is dropped as futile. A line written by a retry (review 6's caveat 1)
/// records a mismatch whose first write failed, and its release stays owed: it records the stored
/// observation when the page could not be read ([`STORED_MISMATCH_LINES`]), or what the page shows
/// now when it could (review 7's F4). It is evidence for a person. It is read back only to record
/// each mismatch once (`append_once_durably`, review 4's finding 3), and a new incarnation at the
/// path moves it aside ([`start_fresh_quarantine`]); otherwise it only grows.
pub fn release_quarantine(wal_path: &Path) -> PathBuf {
    let mut path = wal_path.as_os_str().to_os_string();
    path.push(".release-quarantine");
    PathBuf::from(path)
}

/// [`append_durably`], unless a line starting with `key` is already in the file: then make that
/// content durable. Review 4's finding 3: a dropped mismatch writes no `HeapRelease`, so every open
/// before the log truncates finds it again, and it is recorded once. A file that cannot be read
/// (not merely absent) is an error, so the caller keeps the release owed.
///
/// **The line found may not be durable yet**, because it is read from the page cache: an earlier
/// append's fsync can have failed after its write. So the file is REWRITTEN with the content just
/// read, through [`crate::storage::atomic_file::replace_atomically`] (temporary, fsync, rename,
/// directory fsync), and a second fsync of the same file is not trusted (review 7's F2, the lead's
/// decision: after a reported writeback error, Linux can answer a second fsync `Ok` without the data,
/// PostgreSQL's 2018 "fsyncgate"). That also retires review 7's F1, a re-fsync through a READ-ONLY
/// handle, which Windows refuses. Cost, stated: this path runs only for a mismatch found again, and
/// the file is small by construction.
fn append_once_durably(ops: &dyn FileOps, path: &Path, key: &str, line: &str) -> std::io::Result<()> {
    match std::fs::read_to_string(path) {
        Ok(existing) if existing.lines().any(|l| l.starts_with(key)) => {
            crate::storage::atomic_file::replace_atomically(ops, path, existing.as_bytes())
        }
        Ok(_) => append_durably(path, line),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => append_durably(path, line),
        Err(e) => Err(e),
    }
}

/// Append `line` to the file at `path`, creating it, and make both durable: the file's data, and on
/// unix the directory entry of a file just created.
fn append_durably(path: &Path, line: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(line.as_bytes())?;
    file.sync_all()?;
    sync_directory_of(path)
}

/// Fsync the directory holding `path`, so a file created there survives a power loss.
///
/// **Blind spot, stated:** unix only. Elsewhere the directory is not synced, so a newly created
/// file's entry is as durable as the filesystem makes it on its own, and no more.
fn sync_directory_of(path: &Path) -> std::io::Result<()> {
    if cfg!(unix) {
        let dir = match path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

pub fn with_page<F>(bp: &BufferPoolManager, page_id: u32, lsn: u64, f: F) -> Result<(), FerroError> 
where F: FnOnce(&mut Page) -> Result<(), FerroError> {
    let frame_i = bp.fetch_page(page_id)?;
    let mut frame = bp.frame_write(frame_i);
    let mut page = Page::deserialize(frame.data)?;
    f(&mut page)?;
    page.lsn = lsn;
    frame.data = page.serialize()?;
    drop(frame);
    bp.unpin_page(page_id, true);
    Ok(())
}

/// Hands every `TxnManager` a distinct id; see [`TxnManager::read_snapshot_cached`].
static NEXT_TXN_MANAGER_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// `(manager id, att version, snapshot)` — the last snapshot this thread built, reusable
    /// until the table changes. See [`TxnManager::read_snapshot_cached`].
    static SNAPSHOT_CACHE: std::cell::RefCell<Option<(u64, u64, Arc<Snapshot>)>> =
        const { std::cell::RefCell::new(None) };
}

/// A read borrow of the active-transaction table: `Deref`, never `DerefMut`. See
/// [`TxnManager::att_read`].
pub struct AttReadGuard<'a> {
    guard: MutexGuard<'a, HashMap<u64, TxnEntry>>,
}

impl std::ops::Deref for AttReadGuard<'_> {
    type Target = HashMap<u64, TxnEntry>;
    fn deref(&self) -> &HashMap<u64, TxnEntry> {
        &self.guard
    }
}

/// A write borrow of the active-transaction table that **cannot forget to publish the change**.
///
/// D59's snapshot cache is only sound while `att_version` moves on every mutation of the table.
/// There are sixteen production `att.lock()` sites in this file and more outside it, so "remember
/// to bump the counter" is the kind of discipline that has failed in this repo before (the page
/// latch's ordering contract, `stage_all`'s single funnel). Instead: `att_write()` hands out this
/// guard, `DerefMut` marks it dirty, and `Drop` bumps the version **while the lock is still
/// held** — so a reader cannot observe the new version with the old table, or the old version
/// with the new table. A read-only borrow goes through [`TxnManager::att_read`] and bumps nothing.
///
/// Same shape as D58's `FrameWriteGuard`, for the same reason.
pub struct AttGuard<'a> {
    guard: Option<MutexGuard<'a, HashMap<u64, TxnEntry>>>,
    version: &'a AtomicU64,
    dirty: bool,
}

impl std::ops::Deref for AttGuard<'_> {
    type Target = HashMap<u64, TxnEntry>;
    fn deref(&self) -> &HashMap<u64, TxnEntry> {
        self.guard.as_ref().expect("att guard used after drop")
    }
}

impl std::ops::DerefMut for AttGuard<'_> {
    fn deref_mut(&mut self) -> &mut HashMap<u64, TxnEntry> {
        self.dirty = true;
        self.guard.as_mut().expect("att guard used after drop")
    }
}

impl Drop for AttGuard<'_> {
    fn drop(&mut self) {
        if self.dirty {
            // Release: a reader that sees this version also sees the table that produced it.
            self.version.fetch_add(1, Ordering::Release);
        }
        self.guard = None;
    }
}

impl ReadView {
    pub fn visible(&self, h: &VersionHeader) -> bool {
        if !self.is_commited_for_me(h.begin_ts) {
            return false;
        }
        let ended = h.end_ts != 0 && self.is_commited_for_me(h.end_ts);
        !ended
    }

    /// Stated once, and used by [`ReadView::visible`] and by
    /// [`Snapshot::includes`] rather than spelled out again in either. A cutover that decides
    /// "the snapshot already has this transaction" by a *different* rule than the one the snapshot
    /// was read with is a duplicate or a hole, depending on which way the two drift.
    pub fn is_commited_for_me(&self, ts: u64) -> bool {
        ts == self.txn_id || self.snapshot.includes(ts)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{OpenOptions, metadata};

use crate:: {catalog::catalog::Catalog, execution::{executor::{Outcome, run}, session::Session}, parser::{parser::Parser, scanner::Scanner}, storage::{disk_manager::DiskManager, heap_file_manager::HeapFileManager}, wal::log::{LogRecord, pwrite_all}};

use super::*;

    fn setup() -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.path().join("txn.db")).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let wal = Arc::new(WalManager::new(dir.path().join("txn.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        (bp, wal, txn, dir)
    }

    fn walk_log(wal: &WalManager) -> Vec<LogRecord> {
        let mut out = Vec::new();
        let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
        let end = wal.next_lsn.load(Ordering::SeqCst);
        while lsn < end {
            let (rec, next) = wal.read_record(lsn).unwrap();
            out.push(rec);
            lsn = next;
        }
        out
    }

    fn a_run(prov: u32, agent: &str) -> RunEntity {
        RunEntity::new(
            crate::provenance::ProvId(prov),
            agent,
            "run-1",
            "claude-opus",
            "2026-05",
            [0xcd; 32],
            1_700_000_000_000,
            crate::branch::types::BranchId::new(1, 0),
        )
    }

    /// **The identity record is this transaction's last record before its `Commit`.**
    ///
    /// Its position is a correctness property of the change feed rather than a matter of taste —
    /// see [`TxnManager::bind_run`] — so it is asserted on the log's own record order and not only
    /// through the decoder that reads it.
    ///
    /// Filtered to **this transaction's** records on purpose. Nothing holds a lock across the two
    /// appends, so a concurrent transaction's record, or a `Ddl` (which bypasses the active-txn
    /// table), can land between them in the byte stream. Asserting raw adjacency would be asserting
    /// single-threadedness; what the feed needs is that this record follow every row this transaction
    /// staged and precede its commit.
    #[test]
    fn the_run_identity_record_is_the_last_record_before_its_own_commit() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        txn.bind_run(t1, a_run(1, "restock-agent")).unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![1, 2, 3, 4])).unwrap();
        txn.commit(t1).unwrap();

        let all = walk_log(&wal);
        let mine: Vec<&LogRecord> = all.iter().filter(|r| r.txn_id == t1).collect();
        let commit_at = mine
            .iter()
            .position(|r| matches!(r.kind, RecKind::Commit))
            .expect("no commit record for this transaction");
        assert!(commit_at > 0, "the commit is this transaction's first record");
        match &mine[commit_at - 1].kind {
            RecKind::RunIdentity { run } => assert_eq!(run.agent_id, "restock-agent"),
            other => panic!(
                "this transaction's record before its commit is {other:?}, not the run identity. \
                 Anything of this transaction's between them can be separated from the commit by a \
                 batch boundary, and the feed then ships the rows attributed to nobody."
            ),
        }
        // And it is AFTER every row this transaction staged, which is the other half of the cursor
        // argument: below the earliest staged record it would be stepped over.
        let last_row = mine
            .iter()
            .rposition(|r| matches!(r.kind, RecKind::HeapInsert { .. }))
            .expect("no row record");
        assert!(
            last_row < commit_at - 1,
            "the identity record sits at or below a row this transaction staged"
        );

        // Anti-vacuity: an unbound transaction writes no identity record at all, so the assertion
        // above is about the binding and not about some record that is always there.
        let t2 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.insert(Tuple::new(vec![5, 6])).unwrap();
        txn.commit(t2).unwrap();
        let identities = walk_log(&wal)
            .iter()
            .filter(|r| matches!(r.kind, RecKind::RunIdentity { .. }))
            .count();
        assert_eq!(identities, 1, "an unbound transaction wrote an identity record");
    }

    /// **A second session of the same run is not a different actor, and the clock must not decide.**
    ///
    /// `started_at` is when a particular session began. `MemProvenanceStore::intern` deliberately
    /// excludes it from `same_actor` — its doc records that including it was a real bug CI caught,
    /// where the same input was refused or accepted depending on whether the system clock had ticked
    /// — and hands two sessions of one run the SAME `ProvId`.
    ///
    /// `declare_run` and `bind_run` reintroduced that bug by comparing with derived `==`, which does
    /// compare `started_at`. Two definitions of "the same actor" in one system is how this becomes a
    /// defect, and it is worse here than in the store: a refused declaration meant a legitimate
    /// second session could not commit at all.
    ///
    /// **Breaking shape:** the same run bound twice with a later `started_at` — that is, any agent
    /// that opens a second session, which is the ordinary case. A workload where each run commits
    /// exactly once never produces it.
    #[test]
    fn a_second_session_of_one_run_is_the_same_actor_however_the_clock_moved() {
        let (bp, _wal, txn, _dir) = setup();
        let first = a_run(1, "restock-agent");
        let mut later = a_run(1, "restock-agent");
        later.started_at = first.started_at + 5_000;
        assert_ne!(first, later, "the two entities must differ, or this test proves nothing");
        assert!(first.same_actor(&later), "same_actor changed meaning; this test is measuring it");

        txn.declare_run(first.clone()).unwrap();
        txn.declare_run(later.clone())
            .expect("a second session of the same run was refused for having a later clock reading");
        assert_eq!(txn.retained_runs(), 1, "the second session became a second declaration");

        // And it can actually commit, which is what the refusal was blocking.
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        let t1 = txn.begin().unwrap();
        txn.bind_run(t1, later.clone()).expect("bind_run refused a second session");
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![1, 2, 3, 4])).unwrap();
        txn.commit(t1).unwrap();

        // Anti-vacuity: a genuinely different actor under the same slot is still refused.
        let err = txn
            .declare_run(a_run(1, "auditor-agent"))
            .expect_err("a different agent under one slot was accepted");
        assert!(format!("{err}").contains("already declared"), "{err}");
    }

    /// **A refused `bind_run` must leave NO binding, or the refusal causes the thing it prevents.**
    ///
    /// The insert used to happen before `declare_run` could refuse. On a slot collision `bind_run`
    /// returned `Err` and the binding stood, so the next `commit` appended an identity record for
    /// that slot anyway — putting two different actors under one `prov_id` in the log, which is
    /// exactly what `LogicalDecoder` refuses outright. The guard's failure path produced the disaster
    /// the guard is named after.
    ///
    /// **Breaking shape:** a caller that ignores `bind_run`'s error and commits anyway — which is
    /// what any `?`-less call site does, and what a caller that logs and continues does deliberately.
    #[test]
    fn a_refused_binding_leaves_nothing_behind_for_the_commit_to_write() {
        let (bp, wal, txn, _dir) = setup();
        // Slot 1 already means `restock-agent`.
        txn.declare_run(a_run(1, "restock-agent")).unwrap();

        let t1 = txn.begin().unwrap();
        let err = txn
            .bind_run(t1, a_run(1, "auditor-agent"))
            .expect_err("a colliding slot was bound");
        assert!(format!("{err}").contains("already declared"), "{err}");

        // Commit anyway, as a caller that ignored the error would. No identity record may appear.
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![9, 9])).unwrap();
        txn.commit(t1).unwrap();

        let records = walk_log(&wal);
        let identities: Vec<&RecKind> = records
            .iter()
            .map(|r| &r.kind)
            .filter(|k| matches!(k, RecKind::RunIdentity { .. }))
            .collect();
        assert!(
            identities.is_empty(),
            "a refused binding still put an identity record in the log: {identities:?}"
        );

        // Anti-vacuity: an accepted binding does write one.
        let t2 = txn.begin().unwrap();
        txn.bind_run(t2, a_run(1, "restock-agent")).unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.insert(Tuple::new(vec![8, 8])).unwrap();
        txn.commit(t2).unwrap();
        let n = walk_log(&wal)
            .iter()
            .filter(|r| matches!(r.kind, RecKind::RunIdentity { .. }))
            .count();
        assert_eq!(n, 1, "an accepted binding wrote no identity record either");
    }

    /// **An identity record with an empty field is refused at the producer.**
    ///
    /// The consumer's contract is stricter than `RunEntity`: `cdc-consumer`'s `checkWriter` refuses a
    /// writer object with an empty agent, run, model or model_version, and it refuses the whole LINE
    /// — so one such run makes `validate`, `sink`, `diff` and `follow` abort at the first row it
    /// wrote and land nothing. `model_version` is also the field `retract` keys on, where an empty
    /// string is indistinguishable from the NULL a row with no writer carries.
    ///
    /// **Breaking shape:** any caller that defaults a field to `""` rather than to an explicit
    /// placeholder. `begin_session_with_model` already defaults to the literal `unspecified`, which is
    /// why this was reachable but not yet reached.
    #[test]
    fn binding_a_run_with_an_empty_named_field_is_refused() {
        let (_bp, _wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mutations: [(&str, fn(&mut RunEntity)); 4] = [
            ("agent_id", |r| r.agent_id = String::new()),
            ("run_id", |r| r.run_id = String::new()),
            ("model", |r| r.model = String::new()),
            // Whitespace only, because "   " is not a name and `trim` is what decides that.
            ("model_version", |r| r.model_version = "   ".into()),
        ];
        for (field, mutate) in mutations {
            let mut run = a_run(1, "restock-agent");
            mutate(&mut run);
            let err = txn
                .bind_run(t1, run)
                .unwrap_err();
            assert!(
                format!("{err}").contains(&format!("its {field} is empty")),
                "an empty {field} was not refused by this guard: {err}"
            );
            assert_eq!(txn.retained_runs(), 0, "a refused binding declared the run anyway");
        }
        // Anti-vacuity: a fully named run binds.
        txn.bind_run(t1, a_run(1, "restock-agent")).expect("a fully named run was refused");
    }

    /// One provenance slot cannot mean two actors. The slot is the reference every stamped version
    /// carries, so two meanings for it make every attribution ambiguous, and a checkpoint would
    /// replay both declarations into the log for a decoder to choose between.
    #[test]
    fn declaring_one_slot_as_two_actors_is_refused() {
        let (_bp, _wal, txn, _dir) = setup();
        txn.declare_run(a_run(1, "restock-agent")).unwrap();
        // Anti-vacuity: the same declaration again is a no-op, which is what a checkpoint replay
        // and a repeated session both produce.
        txn.declare_run(a_run(1, "restock-agent")).unwrap();
        assert_eq!(txn.retained_runs(), 1);

        let err = txn
            .declare_run(a_run(1, "auditor-agent"))
            .expect_err("one slot was declared as two actors");
        assert!(format!("{err}").contains("already declared"), "{err}");
        assert_eq!(txn.retained_runs(), 1, "the refused declaration was retained anyway");

        // A different slot is fine, so the refusal is about the collision and not about declaring.
        txn.declare_run(a_run(2, "auditor-agent")).unwrap();
        assert_eq!(txn.retained_runs(), 2);
    }

    #[test]
    fn test_commit_writes_chain_and_flushes() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![1,2,3,4])).unwrap();
        txn.commit(t1).unwrap();
        let recs = walk_log(&wal);
        assert_eq!(recs.len(), 4);
        assert!(matches!(recs[0].kind, RecKind::Begin));
        assert!(matches!(recs[1].kind, RecKind::HeapInsert { .. }));
        assert!(matches!(recs[2].kind, RecKind::Commit));
        assert!(matches!(recs[3].kind, RecKind::TxnEnd));
        assert_eq!(recs[1].prev_lsn, recs[0].lsn);
        assert_eq!(recs[2].prev_lsn, recs[1].lsn);
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) > recs[2].lsn);
        if let RecKind::HeapInsert { dir_root, tuple, .. } = &recs[1].kind {
            assert_eq!(*dir_root, heap.first_directory_page_id);
            assert_eq!(tuple, &vec![1, 2, 3, 4]);
        }

        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_abort_insert_removes_rows() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);

        for i in 0..3u8 {
            heap.insert(Tuple::new(vec![i,i,i])).unwrap();
        }
        txn.abort(t1).unwrap();
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        // begin, hi, hi, hi, abort, clr, clr, clr, txnend
        let recs = walk_log(&wal);
        let undone: Vec<u64> = recs[5..8].iter().map(|r| match &r.kind {
            RecKind::Clr { undone_lsn, .. } => *undone_lsn,
            _ => panic!()
        }).collect();
        assert!(rows.is_empty());
        assert_eq!(recs.len(), 9);
        assert!(matches!(recs[4].kind, RecKind::Abort));
        assert!(matches!(recs[8].kind, RecKind::TxnEnd));
        assert_eq!(undone, vec![recs[3].lsn, recs[2].lsn, recs[1].lsn]);
    }

    #[test]
    fn test_abort_delete_restores_row() {
        let (bp, _wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![8,8,8])).unwrap();
        txn.commit(t1).unwrap();

        let t2 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.delete(rid).unwrap();
        assert!(heap.read(rid).is_err());
        txn.abort(t2).unwrap();
        
        assert_eq!(heap.read(rid).unwrap().data, vec![8,8,8]);
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn sql_insert_commits_through_run() {
        let (bp, wal, txn, _dir) = setup();
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let exec = |sql: &str, catalog: &mut Catalog| -> Outcome {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty());
            let mut session = Session::new();
            run(stmts.remove(0), catalog, bp.clone(), txn.clone(), &mut session).unwrap()
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR(20));", &mut catalog);
        let out = exec("INSERT INTO t VALUES (1, 'a');", &mut catalog);
        assert!(matches!(out, Outcome::Affected(1)));
        let recs = walk_log(&wal);
        assert!(recs.iter().any(|r| matches!(r.kind, RecKind::HeapInsert { .. })));
        assert!(recs.iter().any(|r| matches!(r.kind, RecKind::Commit)));
    }

    #[test]
    fn gate_flushed_wal_before_page_write() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![1, 2, 3])).unwrap();
        let target = wal.next_lsn.load(Ordering::SeqCst);
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) < target);
        bp.flush_page(rid.page_id).unwrap();
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) >= target);
        txn.abort(t1).unwrap();
    }

    #[test]
    fn gate_covers_flush_all() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![9,9])).unwrap();

        let target = wal.next_lsn.load(Ordering::SeqCst);
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) < target);
        bp.flush_all().unwrap();
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) >= target);
        txn.abort(t1).unwrap();
    }

    #[test]
    fn checkpoint_truncates_and_preserves_data() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![4,5,6])).unwrap();
        txn.commit(t1).unwrap();
        txn.checkpoint().unwrap();
        let next = wal.next_lsn.load(Ordering::SeqCst);
        assert!(walk_log(&wal).is_empty());
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), next);
        assert_eq!(metadata(&wal.path).unwrap().len(), 24);
        assert_eq!(heap.read(rid).unwrap().data, vec![4,5,6]);

        let t2 = txn.begin().unwrap();
        let recs = walk_log(&wal);
        assert_eq!(recs[0].lsn, next);
        txn.abort(t2).unwrap();
    }

    #[test]
    fn interrupted_truncation_self_heals_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.wal");
        let new_base;
        {
            let wal = WalManager::new(path.clone()).unwrap();
            wal.append(1, 0, &RecKind::Begin).unwrap();
            wal.append(1, 0, &RecKind::Commit).unwrap();
            wal.flush().unwrap();
            new_base = wal.next_lsn.load(Ordering::SeqCst);

            let mut header = [0u8; 24];
            header[0..4].copy_from_slice(&0xF3_EE_DB_01u32.to_be_bytes());
            header[4..8].copy_from_slice(&2u32.to_be_bytes());
            header[8..16].copy_from_slice(&new_base.to_be_bytes());
            header[16..24].copy_from_slice(&1u64.to_be_bytes());
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            pwrite_all(&f, &mut header, 0).unwrap();
        }
        let wal = WalManager::new(path.clone()).unwrap();
        assert_eq!(metadata(&path).unwrap().len(), 24);
        assert_eq!(wal.next_lsn.load(Ordering::SeqCst), new_base);
        let lsn = wal.append(2, 0, &RecKind::Begin).unwrap();
        assert_eq!(lsn, new_base);
    }

    #[test]
    fn torn_tail_trimmed_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path =  dir.path().join("torn.wal");
        let (l0, l1);
        {
            let wal = WalManager::new(path.clone()).unwrap();
            l0 = wal.append(1, 0, &RecKind::Begin).unwrap();
            l1 = wal.append(1, l0, &RecKind::HeapInsert { dir_root: 1, page_id: 1, slot: 0, tuple: vec![1,2,3,4,5,6] }).unwrap();
            wal.flush().unwrap();
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            let len = f.metadata().unwrap().len();
            f.set_len(len - 3).unwrap();
        }
        let wal = WalManager::new(path.clone()).unwrap();
        assert_eq!(wal.next_lsn.load(Ordering::SeqCst), l1);
        assert!(wal.read_record(l0).is_ok());
    }

    #[test]
    fn test_begin_creates_snapshot() {
        let (_bp, _wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let t2 = txn.begin().unwrap();
        let att = txn.att.lock().unwrap();
        let snapshot = att[&t2].snapshot.as_ref().unwrap();
        assert_eq!(snapshot.high_water, t2);
        assert!(snapshot.active.contains(&t1));
        assert!(!snapshot.active.contains(&t2));
    }

    /// **The resume point must reach back over a transaction that was already in flight.**
    ///
    /// Its records sit below the point the read was taken at, and its work is *not* in the
    /// snapshot, so a stream told to start at the read would meet its `Commit` having never seen
    /// its changes and drop them without a trace.
    #[test]
    fn a_snapshot_read_resumes_at_the_oldest_transaction_it_excluded() {
        let (bp, wal, txn, _dir) = setup();

        // Committed before anything else: in the snapshot, and behind the resume point.
        let early = txn.begin().unwrap();
        txn.commit(early).unwrap();

        // Still open when the snapshot is taken: excluded from it, and its records are older than
        // the read.
        let open = txn.begin().unwrap();
        let open_begin = txn.att.lock().unwrap()[&open].begin_lsn;
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), open);
        heap.insert(Tuple::new(vec![1, 2, 3])).unwrap();

        let handoff = txn.begin_snapshot_read().unwrap();

        assert_eq!(
            handoff.resume_lsn, open_begin,
            "the resume point did not reach back to the open transaction's Begin, so its changes \
             sit below where the stream would start"
        );
        assert!(
            !handoff.snapshot.includes(open),
            "an in-flight transaction was reported as already in the snapshot"
        );
        assert!(
            handoff.snapshot.includes(early),
            "a transaction that committed before the snapshot was not reported as in it"
        );
        assert!(
            !handoff.snapshot.includes(handoff.txn_id + 1),
            "a transaction that has not started yet was reported as already in the snapshot"
        );
        assert!(
            wal.flushed_lsn.load(Ordering::SeqCst) >= handoff.resume_lsn,
            "the resume point is not durable, so a crash would move the frontier back over it"
        );

        txn.end_read_only(handoff.txn_id).unwrap();
        txn.abort(open).unwrap();
    }

    /// With nothing in flight there is nothing to reach back for, and the resume point is the
    /// reader's own `Begin`. Without this, the test above would pass just as well against a resume
    /// point that always ran to the start of the log.
    #[test]
    fn a_quiet_database_resumes_at_the_readers_own_begin() {
        let (_bp, wal, txn, _dir) = setup();
        let before = wal.next_lsn.load(Ordering::SeqCst);
        let handoff = txn.begin_snapshot_read().unwrap();
        assert_eq!(
            handoff.resume_lsn, before,
            "a quiet database resumed somewhere other than the reader's own Begin"
        );
        assert!(handoff.snapshot.active.is_empty());
        txn.end_read_only(handoff.txn_id).unwrap();
    }

    /// A snapshot reader that writes is refused, and rolled back rather than left open. Its writes
    /// would be invisible to its own snapshot, so the rows it wrote out would not be the state it
    /// claims to describe.
    #[test]
    fn a_snapshot_reader_that_writes_is_refused_and_rolled_back() {
        let (bp, _wal, txn, _dir) = setup();
        let handoff = txn.begin_snapshot_read().unwrap();

        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), handoff.txn_id);
        heap.insert(Tuple::new(vec![7, 7])).unwrap();

        let err = txn.end_read_only(handoff.txn_id).expect_err("a writing reader was accepted");
        assert!(format!("{err}").contains("rolled back"), "wrong reason: {err}");
        assert!(
            !txn.att.lock().unwrap().contains_key(&handoff.txn_id),
            "the refused reader was left open, which blocks every future checkpoint"
        );
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert!(rows.is_empty(), "the reader's write survived its rollback");
    }

    /// **A snapshot read that fails must not leave its reader behind.**
    ///
    /// This is the expensive one. The reader is inserted into `att` before anything that can fail,
    /// and its id is not handed out until the call succeeds — so a failure after the insert leaves
    /// an active transaction nobody can name, let alone close. `checkpoint` refuses while `att` is
    /// non-empty, so the database never checkpoints again: every later CREATE TABLE, CREATE INDEX
    /// and CLI shutdown fails with "checkpoint with active txns" for the life of the process. One
    /// failed snapshot takes the whole database with it.
    ///
    /// Reaching the failure needs the log truncated out from under an open transaction, which
    /// `checkpoint` will not do — defending `att` is precisely its job — so the WAL is told
    /// directly. That is fault injection rather than a scenario, and it is the point: this path is
    /// rare, and a rare path that wedges the process permanently is worth a deliberate test.
    #[test]
    fn a_snapshot_reader_is_not_leaked_when_its_handoff_cannot_be_pinned() {
        let (_bp, wal, txn, _dir) = setup();

        // Its `Begin` is what `begin_snapshot_read` will choose as the resume point, being the
        // oldest still in flight.
        let open = txn.begin().unwrap();

        // The log's base moves to `next_lsn`, which is already past that `Begin`, so pinning the
        // resume point must fail.
        wal.truncate(txn.txn_ids.issued_through()).unwrap();
        assert!(
            wal.base_lsn.load(Ordering::SeqCst) > txn.att.lock().unwrap()[&open].begin_lsn,
            "the log was not truncated past the open transaction, so this test proves nothing"
        );

        let err = txn.begin_snapshot_read().expect_err("a handoff below the log's base was given out");
        assert!(format!("{err}").contains("truncated"), "wrong reason: {err}");

        // The failed reader is gone; only the transaction this test opened is still active.
        let live: Vec<u64> = txn.att.lock().unwrap().keys().copied().collect();
        assert_eq!(
            live,
            vec![open],
            "the failed snapshot left its reader in the active transaction table"
        );

        // And the consequence, asserted as a consequence rather than as internal state: with the
        // reader leaked this refuses for ever. Committed rather than aborted only because an abort
        // walks its undo chain, and this test just truncated that chain away.
        txn.commit(open).unwrap();
        txn.checkpoint()
            .expect("a failed snapshot left a reader open, so checkpoints are blocked for good");
    }

    /// **The other half of the same leak: closing the reader fails, and it still has to go.**
    ///
    /// `end_read_only` used to append its `TxnEnd` with `?` and only then remove the entry, so a
    /// failed append returned an error to a caller who had already handed the transaction over.
    /// Retrying means calling back into a WAL that just refused; not retrying leaves the reader
    /// active for ever. Either way `checkpoint` refuses from then on and the database never
    /// truncates its log again.
    ///
    /// `WalManager::append` cannot fail on its own — it extends a buffer and returns `Ok` — so this
    /// drives it through the test-only one-shot lever rather than pretending the path is reachable
    /// by ordinary means. Without the lever the guard could not be shown to work at all.
    #[test]
    fn a_reader_is_not_leaked_when_its_txn_end_cannot_be_written() {
        let (_bp, wal, txn, _dir) = setup();
        let handoff = txn.begin_snapshot_read().unwrap();
        let reader = handoff.txn_id;

        // Arm the failure for exactly the `TxnEnd` this close is about to write.
        wal.fail_next_append.store(true, Ordering::SeqCst);

        let err = txn.end_read_only(reader).expect_err("a failed TxnEnd was reported as success");
        assert!(format!("{err}").contains("injected"), "wrong reason: {err}");

        // **The error is reported AND the reader is gone.** Reporting it is not the hard part.
        assert!(
            !txn.att.lock().unwrap().contains_key(&reader),
            "the reader survived a failed close, so every later checkpoint is refused"
        );

        // The consequence that makes it matter. The handoff still holds a pin at the resume point,
        // so it is dropped first - otherwise this fails for the unrelated reason that the log is
        // legitimately claimed, and would pass just as well with the reader leaked.
        drop(handoff);
        txn.checkpoint().expect("a leaked reader is blocking every checkpoint");
    }

    /// **Transaction id 0 is never handed out, and something now depends on that.**
    ///
    /// The change feed treats id 0 as "not attributed to a transaction" — DDL is logged under it —
    /// and the snapshot-to-stream filter exempts those events, because a snapshot's transaction set
    /// says nothing about a schema declaration. If `begin` ever returned 0, that exemption would
    /// silently start applying to a real transaction's rows and re-deliver them after a cutover.
    /// The invariant comes from the WAL header's initial `next_txn_id` of 1, which is a long way
    /// from here, so it is asserted here rather than assumed.
    #[test]
    fn transaction_ids_never_start_at_zero() {
        let (_bp, _wal, txn, _dir) = setup();
        let first = txn.begin().unwrap();
        assert_ne!(first, 0, "the first transaction was given the id the feed reserves for DDL");
        txn.commit(first).unwrap();

        let handoff = txn.begin_snapshot_read().unwrap();
        assert_ne!(handoff.txn_id, 0);

        // The raw rule genuinely does say "contained" for 0 — it is arithmetic over timestamps, and
        // MVCC visibility needs it to keep saying that. The cutover asks a different question.
        assert!(handoff.snapshot.includes(0));
        assert!(
            !handoff.snapshot.already_delivered(0),
            "an event with no transaction author was treated as already delivered by the snapshot; \
             a stream would suppress every schema declaration"
        );
        assert!(
            handoff.snapshot.already_delivered(first),
            "a transaction that committed before the snapshot was not treated as delivered by it"
        );
        txn.end_read_only(handoff.txn_id).unwrap();
    }

    /// **The high water mark is exclusive, and nothing in the matrix below pins that.**
    ///
    /// Every case there that sits exactly on the mark is also the view's own transaction, so it
    /// passes through the `ts == txn_id` branch and an off-by-one in the comparison goes unseen.
    /// A statement-level view has `txn_id` 0 and `high_water` set to the *next* id to be handed
    /// out, so making the bound inclusive would show a reader the work of a transaction that had
    /// not even begun, let alone committed.
    #[test]
    fn the_high_water_mark_is_exclusive() {
        let snapshot = Snapshot { high_water: 10, active: HashSet::new() };
        assert!(!snapshot.includes(10), "the transaction at the mark was reported as included");
        assert!(snapshot.includes(9), "the transaction below the mark was reported as excluded");

        // Through a view whose own id is not the mark, so the `ts == txn_id` branch cannot mask it.
        let view = ReadView { snapshot: Arc::new(snapshot), txn_id: 0 };
        let h = |b, e| VersionHeader { begin_ts: b, end_ts: e, prev_page: 0, prev_slot: 0 };
        assert!(
            !view.visible(&h(10, 0)),
            "a version written by the next transaction to begin was visible before it existed"
        );
        assert!(view.visible(&h(9, 0)));
    }

    #[test]
    fn test_visibility_matrix() {
        let view = ReadView { snapshot: Arc::new(Snapshot { high_water: 10, active: HashSet::from([7])}), txn_id: 10};
        let h = |b, e| VersionHeader { begin_ts: b, end_ts: e, prev_page: 0, prev_slot: 0};
        assert!(view.visible(&h(10, 0)));
        assert!(view.visible(&h(5, 0)));
        assert!(!view.visible(&h(7, 0)));
        assert!(!view.visible(&h(12, 0)));
        assert!(!view.visible(&h(5,6)));
        assert!(view.visible(&h(5, 12)));
        assert!(view.visible(&h(5, 7)));
        assert!(!view.visible(&h(5, 10)));
    }

    /// **D202 — abort takes back the primary-index writes a transaction recorded, and commit
    /// forgets them.**
    ///
    /// The SQL tests in `tests/rollback_index_undo.rs` see only the outcome. Three things here have
    /// no SQL shape: undo runs newest first (a key moved twice must end where it STARTED, not at
    /// its middle position), a new key is removed rather than left pointing anywhere, and a
    /// committed list is dropped. That last one matters because transaction ids restart from the
    /// log header after a restart, so a list kept under a reissued id would undo committed work.
    #[test]
    fn abort_undoes_recorded_primary_writes_newest_first_and_commit_forgets_them() {
        use crate::catalog::column::Value;
        use crate::storage::{heap_file_manager::RecordId, index::BPlusTreeManager};

        let (bp, _wal, txn, _dir) = setup();
        let tree = BPlusTreeManager::<Value, RecordId>::create(bp.clone()).unwrap();
        let (a, b, c) = (RecordId::new(40, 1), RecordId::new(41, 2), RecordId::new(42, 3));
        tree.insert(Value::Integer(7), a).unwrap();

        let t = txn.begin().unwrap();
        txn.record_primary_write(t, tree.root_cell(), Value::Integer(5), None);
        tree.upsert(Value::Integer(5), c).unwrap();
        txn.record_primary_write(t, tree.root_cell(), Value::Integer(7), Some(a));
        tree.upsert(Value::Integer(7), b).unwrap();
        txn.record_primary_write(t, tree.root_cell(), Value::Integer(7), Some(b));
        tree.upsert(Value::Integer(7), c).unwrap();
        txn.abort(t).unwrap();
        assert_eq!(tree.search(&Value::Integer(5)).unwrap(), None, "a key the rollback added survived it");
        assert_eq!(
            tree.search(&Value::Integer(7)).unwrap(),
            Some(a),
            "a key moved twice did not end where it started (undo ran oldest first, or not at all)"
        );
        assert!(txn.index_undo.lock().unwrap().get(&t).is_none(), "an aborted transaction's list outlived it");

        let u = txn.begin().unwrap();
        txn.record_primary_write(u, tree.root_cell(), Value::Integer(6), None);
        tree.upsert(Value::Integer(6), b).unwrap();
        txn.commit(u).unwrap();
        assert_eq!(tree.search(&Value::Integer(6)).unwrap(), Some(b), "a committed write was undone");
        assert!(
            txn.index_undo.lock().unwrap().get(&u).is_none(),
            "a committed transaction's list was kept; an abort under a reissued id would undo committed work"
        );
    }

    /// **D205 C1 — an index undo that fails must not fail an abort that ended the transaction.**
    ///
    /// Every caller reads `abort`'s `Err` as "the abort did not happen": `executor.rs` does
    /// `txn.abort(id)?; session.current = None;`, so an `Err` skips the reset. At `c21eaff`, `abort`
    /// returned the index-undo error AFTER writing `TxnEnd` and removing the transaction, so the
    /// session kept a dead id and the statement's own error was replaced (the adversary's C1).
    ///
    /// Forced here with a recorded write whose tree root lies past the end of the file. Every read
    /// of it fails in `DiskManager::read` ("eof before finished reading"), so the undo fails for
    /// certain, with nothing timed.
    #[test]
    fn an_index_undo_failure_does_not_fail_an_abort_that_ended_the_transaction() {
        use crate::catalog::column::Value;

        let (_bp, _wal, txn, _dir) = setup();
        let t = txn.begin().unwrap();
        txn.record_primary_write(t, Arc::new(AtomicU32::new(1_000_000)), Value::Integer(5), None);
        txn.abort(t).expect("the transaction ended, so its abort must report success");
        assert!(txn.snapshot_of(t).is_err(), "premise failed: the transaction is still active after its abort");
    }

    /// **D205 C1, the other half: the failure `abort` no longer returns is counted, not dropped.**
    /// `>= before + 1` rather than `==`, because the counter is process-wide and the test above
    /// forces the same failure concurrently.
    #[test]
    fn an_index_undo_failure_is_counted() {
        use crate::catalog::column::Value;

        let (_bp, _wal, txn, _dir) = setup();
        let before = index_undo_failures();
        let t = txn.begin().unwrap();
        txn.record_primary_write(t, Arc::new(AtomicU32::new(1_000_000)), Value::Integer(5), None);
        txn.abort(t).unwrap();
        assert!(
            index_undo_failures() >= before + 1,
            "an index undo failed and nothing counted it: the failure is silent"
        );
    }

    /// A committed delete whose release fails `failures` times as an I/O error would: the heap, the
    /// retired slot's rid, and the transaction manager's pieces. Row A goes in first and commits;
    /// then a second transaction deletes it (retiring the slot) and commits.
    fn owed_release(failures: u32) -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>, HeapFileManager, RecordId, tempfile::TempDir) {
        let (bp, wal, txn, dir) = setup();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        let t1 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![7; 40])).unwrap();
        txn.commit(t1).unwrap();
        let t2 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.delete(rid).unwrap();
        FAIL_RELEASES.with(|f| f.set(failures));
        txn.commit(t2).expect("a commit whose release failed was reported as failed");
        (bp, wal, txn, heap, rid, dir)
    }

    /// **F2 with review 2's N1: a release that fails RETRYABLY stays pending, and the checkpoint
    /// still FLUSHES every page while it refuses to truncate.** Then, once the release can succeed,
    /// the next checkpoint releases it and truncates. This is the pending path that
    /// `tests/failed_release_is_kept.rs` covered before review 2's Q3 made its staged failure (a LIVE
    /// slot) a mismatch. Its red is mutant-only: the seam is new.
    #[test]
    fn a_release_that_fails_retryably_is_kept_pending_and_the_checkpoint_flushes_but_keeps_the_log() {
        // Two failures: the commit's release, then the first checkpoint's retry.
        let (bp, wal, txn, heap, rid, _dir) = owed_release(2);
        let base = wal.base_lsn.load(Ordering::SeqCst);

        assert!(txn.checkpoint().is_err(), "the checkpoint truncated the log past a release that is still owed");
        assert_eq!(FAIL_RELEASES.with(|f| f.get()), 0, "premise: the injected failures were not both consumed");
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "a refused checkpoint truncated the log");
        // The flush happened anyway: the heap page is on disk, retired slot and all. Before the
        // checkpoint it was only in the pool, and the file held the zero page `new_page` wrote.
        let on_disk = bp.disk_manager.read(rid.page_id).unwrap();
        assert_eq!(on_disk[1..5], rid.page_id.to_be_bytes(), "the refused checkpoint did not flush the heap page");
        let page = Page::deserialize(on_disk).unwrap();
        assert!(page.slot_arr[rid.slot_num as usize].is_retired(), "the flushed page does not hold the retired slot");
        assert_eq!(wal.flushed_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst), "the refused checkpoint did not flush the log");

        txn.checkpoint().expect("the checkpoint was refused although the release could now succeed");
        assert!(wal.base_lsn.load(Ordering::SeqCst) > base, "the checkpoint did not truncate once nothing was owed");
        assert!(matches!(heap.read(rid), Err(FerroError::SlotDeleted)), "the released slot reads as live");
        FAIL_RELEASES.with(|f| f.set(0));
    }

    /// **Review 3's decision 1: a create runs while a release is owed, and only the truncation waits.**
    /// Its checkpoint still flushes and syncs, keeps the log for the release, and counts the deferral.
    /// At `7cede54` every DDL was refused before its mutation instead, so one release that never
    /// succeeds blocked all DDL for ever (review 2's Q3). Replaces
    /// `ddl_is_refused_before_its_mutation_while_a_release_is_owed` (lane §21.1).
    #[test]
    fn a_create_runs_while_a_release_is_owed_and_defers_the_truncation() {
        // Every attempt fails: the commit's release and every retry after it.
        let (_bp, wal, txn, _heap, _rid, _dir) = owed_release(u32::MAX);
        let base = wal.base_lsn.load(Ordering::SeqCst);
        let deferred = deferred_checkpoints();
        let mutated = std::cell::Cell::new(false);
        let out = txn.ddl_checkpointed(|| {
            mutated.set(true);
            Ok(7)
        });
        assert_eq!(out.expect("a create was refused while a release is owed"), 7);
        assert!(mutated.get(), "the create reported success without running its mutation");
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: the release stopped being owed");
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "the create's checkpoint truncated the log past a release that is still owed");
        assert!(deferred_checkpoints() > deferred, "the create's checkpoint kept the log without counting a deferral");
        FAIL_RELEASES.with(|f| f.set(0));
    }

    /// One SQL statement through the executor.
    fn sql(text: &str, catalog: &mut Catalog, bp: &Arc<BufferPoolManager>, txn: &Arc<TxnManager>, session: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(text.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse errors in `{text}`: {:?}", p.errors);
        run(stmts.remove(0), catalog, bp.clone(), txn.clone(), session)
    }

    /// Tables `other` and `t (id, v, note)`, where a committed relocating UPDATE of `t`'s row 1 owes a
    /// release that fails at every attempt. Row 2 (3900 B of note) goes in first and row 1 after it,
    /// so row 1's 200 B note does not fit beside them and the UPDATE relocates it, retiring its slot.
    fn table_owing_a_release() -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>, Catalog, tempfile::TempDir) {
        let (bp, wal, txn, dir) = setup();
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let mut s = Session::new();
        for text in [
            "CREATE TABLE other (id INTEGER NOT NULL);".to_string(),
            "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, note VARCHAR(4000));".to_string(),
            format!("INSERT INTO t VALUES (2, 20, '{}');", "x".repeat(3900)),
            "INSERT INTO t VALUES (1, 10, 'a');".to_string(),
        ] {
            sql(&text, &mut catalog, &bp, &txn, &mut s).unwrap_or_else(|e| panic!("`{text}` failed: {e}"));
        }
        FAIL_RELEASES.with(|f| f.set(u32::MAX));
        let update = format!("UPDATE t SET note = '{}' WHERE id = 1;", "y".repeat(200));
        sql(&update, &mut catalog, &bp, &txn, &mut s).unwrap_or_else(|e| panic!("the UPDATE failed: {e}"));
        let owed = txn.pending_releases.lock().unwrap().clone();
        assert_eq!(owed.len(), 1, "premise: the relocating UPDATE's release is not owed");
        let t_heap = catalog.get_table("t").expect("t").first_directory_page_id;
        assert_eq!(owed[0].1.dir_root, t_heap, "premise: the owed release is not on t's heap");
        (bp, wal, txn, catalog, dir)
    }

    /// **Review 3's caveat 1 and decision 1: CREATE INDEX succeeds while a release is owed.** At
    /// `7cede54` it built and flushed the index, and then its checkpoint answered "checkpoint refused",
    /// so the client was told the DDL failed over an index that existed and was used (A8's shape).
    /// CREATE TABLE is checked on the same database.
    #[test]
    fn create_index_succeeds_while_a_release_is_owed_and_the_index_serves_lookups() {
        let (bp, wal, txn, mut catalog, _dir) = table_owing_a_release();
        let mut s = Session::new();
        let base = wal.base_lsn.load(Ordering::SeqCst);
        let deferred = deferred_checkpoints();
        sql("CREATE INDEX ix ON t (v);", &mut catalog, &bp, &txn, &mut s)
            .unwrap_or_else(|e| panic!("CREATE INDEX was reported failed while a release is owed: {e}"));
        let root = catalog.get_table("t").expect("t").indexes.first().expect("CREATE INDEX recorded no index").root_page_id;
        let tree = BPlusTreeManager::<(Value, Value), ()>::open(root, bp.clone());
        for (v, id) in [(10, 1), (20, 2)] {
            assert!(
                tree.search(&(Value::Integer(v), Value::Integer(id))).unwrap().is_some(),
                "the index built while a release is owed has no entry for row {id}"
            );
        }
        match sql("SELECT id FROM t WHERE v = 10;", &mut catalog, &bp, &txn, &mut s).unwrap() {
            Outcome::Rows(rows) => assert_eq!(rows, vec![vec![Value::Integer(1)]], "a lookup by v after CREATE INDEX"),
            _ => panic!("SELECT did not return rows"),
        }
        sql("CREATE TABLE u (id INTEGER NOT NULL);", &mut catalog, &bp, &txn, &mut s)
            .unwrap_or_else(|e| panic!("CREATE TABLE was refused while a release is owed: {e}"));
        assert!(catalog.get_table("u").is_some(), "CREATE TABLE answered Ok without creating the table");
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: the release stopped being owed");
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "a DDL checkpoint truncated the log past a release that is still owed");
        assert!(deferred_checkpoints() >= deferred + 2, "the two DDL checkpoints kept the log without counting two deferrals");
        FAIL_RELEASES.with(|f| f.set(0));
    }

    /// **A DROP of the table that owes a release discards the release and truncates** (lane §21: a
    /// DROP frees every page of its table, so the release could only ever write into pages the table
    /// no longer owns, and the log's record of it must go with the table). At `7cede54` the DROP was
    /// refused, so a table holding a page that permanently fails could never be dropped.
    #[test]
    fn dropping_the_table_that_owes_a_release_discards_it_and_truncates() {
        let (bp, wal, txn, mut catalog, _dir) = table_owing_a_release();
        let base = wal.base_lsn.load(Ordering::SeqCst);
        sql("DROP TABLE t;", &mut catalog, &bp, &txn, &mut Session::new())
            .unwrap_or_else(|e| panic!("DROP of the table that owes the release was refused: {e}"));
        assert!(catalog.get_table("t").is_none(), "DROP answered Ok without dropping the table");
        assert!(txn.pending_releases.lock().unwrap().is_empty(), "the dropped table's release is still owed, and would write into pages the DROP freed");
        assert!(wal.base_lsn.load(Ordering::SeqCst) > base, "the DROP's checkpoint kept the log, with the dropped table's records in it");
        FAIL_RELEASES.with(|f| f.set(0));
    }

    /// **D250 (lane `lane_d250_drop_logged.md` §2 test 3; replaces the parent's
    /// `a_drop_is_refused_before_its_mutation_while_another_table_owes_a_release`): a DROP while
    /// ANOTHER table owes a release succeeds, and the truncation waits.** The owed release keeps the
    /// log, so the dropped table's records stay in it. That is harmless once the DROP's record is
    /// durable before its frees and recovery skips every record a later DROP names.
    #[test]
    fn a_drop_while_another_table_owes_a_release_succeeds_and_keeps_the_log() {
        let (bp, wal, txn, mut catalog, _dir) = table_owing_a_release();
        let base = wal.base_lsn.load(Ordering::SeqCst);
        sql("DROP TABLE other;", &mut catalog, &bp, &txn, &mut Session::new())
            .unwrap_or_else(|e| panic!("DROP was refused while another table owes a release: {e}"));
        assert!(catalog.get_table("other").is_none(), "DROP answered Ok without dropping the table");
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "the DROP discarded another table's release");
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "the DROP truncated the log past a release that is still owed");
        FAIL_RELEASES.with(|f| f.set(0));
    }

    /// A table `t` with one row, and what its DROP would free: its heap's directory root, its
    /// time-travel root and its primary root.
    fn table_to_drop() -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>, Catalog, [u32; 3], tempfile::TempDir) {
        let (bp, wal, txn, dir) = setup();
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let mut s = Session::new();
        for text in ["CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", "INSERT INTO t VALUES (1, 10);"] {
            sql(text, &mut catalog, &bp, &txn, &mut s).unwrap_or_else(|e| panic!("`{text}` failed: {e}"));
        }
        let owned = {
            let e = catalog.get_table("t").expect("t");
            [e.first_directory_page_id, e.time_travel_root, e.primary_index_root]
        };
        (bp, wal, txn, catalog, owned, dir)
    }

    /// **D250 (lane §2 test 4; replaces the parent's
    /// `a_drop_is_refused_before_its_mutation_while_a_pin_would_keep_the_log`): a DROP under a WAL pin
    /// succeeds, and the pin keeps the log.** What makes the kept log harmless after a crash is
    /// tested in `wal::recovery::tests::after_a_pinned_drop_and_a_crash_redo_writes_no_page_the_drop_freed`.
    #[test]
    fn a_drop_under_a_pin_succeeds_and_keeps_the_log() {
        let (bp, wal, txn, mut catalog, _owned, _dir) = table_to_drop();
        let base = wal.base_lsn.load(Ordering::SeqCst);
        let _pin = wal.pin(base).expect("pin the log at its base");
        assert!(base < wal.next_lsn.load(Ordering::SeqCst), "premise: the pin is not below the log's end, so it keeps nothing");
        let kept = kept_log_drops();
        sql("DROP TABLE t;", &mut catalog, &bp, &txn, &mut Session::new())
            .unwrap_or_else(|e| panic!("DROP was refused under a pin: {e}"));
        // §3.3: at least, as the counter is process-wide.
        assert!(kept_log_drops() > kept, "a DROP whose truncation a pin cancelled was not counted");
        assert!(catalog.get_table("t").is_none(), "DROP answered Ok without dropping the table");
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "premise: the pin did not keep the log");
    }

    /// **A DROP is refused BEFORE its mutation on a poisoned log** (lane §21.6). A poisoned log
    /// refuses the flush, so the DROP's checkpoint would fail after the drop: the table gone, its
    /// pages freed, the statement reported failed, and the log holding the table's records.
    #[test]
    fn a_drop_is_refused_before_its_mutation_on_a_poisoned_log() {
        let (bp, wal, txn, mut catalog, owned, _dir) = table_to_drop();
        wal.poison("test: a Commit record could not be made durable");
        let e = match sql("DROP TABLE t;", &mut catalog, &bp, &txn, &mut Session::new()) {
            Err(e) => e,
            Ok(_) => panic!("DROP succeeded on a poisoned log"),
        };
        assert!(catalog.get_table("t").is_some(), "the refused DROP had already dropped the table: {e}");
        assert!(e.to_string().contains("poison"), "the DROP was refused, but not for the poisoned log: {e}");
        let next = bp.disk_manager.allocate().unwrap();
        assert!(!owned.contains(&next), "the refused DROP freed page {next} of `t`, which still names it");
    }

    /// A committed delete whose retired slot was rewritten LIVE in the pool before its COMMIT, so the
    /// release at COMMIT is a page/log mismatch (review 2's Q3). With `block_quarantine`, a directory
    /// holds the quarantine file's path (`<wal>.release-quarantine`, spelled out here), so the mismatch
    /// cannot be recorded and stays owed. Returns the committing transaction, its retired slot and
    /// that path.
    fn committed_mismatch(block_quarantine: bool) -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>, u64, RetiredSlot, PathBuf, tempfile::TempDir) {
        let (bp, wal, txn, dir) = setup();
        let quarantine = PathBuf::from(format!("{}.release-quarantine", wal.path.display()));
        if block_quarantine {
            std::fs::create_dir(&quarantine).unwrap();
        }
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        let t1 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![7; 40])).unwrap();
        txn.commit(t1).unwrap();
        let t2 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.delete(rid).unwrap();
        let retired = txn.retired.lock().unwrap().get(&t2).cloned().unwrap_or_default();
        assert_eq!(retired.len(), 1, "premise: the delete retired no slot");
        let r = retired[0];
        let frame_i = bp.fetch_page(r.page_id).unwrap();
        {
            let mut frame = bp.frame_write(frame_i);
            let mut page = Page::deserialize(frame.data).unwrap();
            let slot = &mut page.slot_arr[r.slot as usize];
            assert!(slot.is_retired(), "premise: the deleted slot is not retired");
            slot.length &= !crate::storage::heap_page::RETIRED;
            frame.data = page.serialize().unwrap();
        }
        bp.unpin_page(r.page_id, true);
        txn.commit(t2).expect("a commit whose release is a mismatch was reported as failed");
        (bp, wal, txn, t2, r, quarantine, dir)
    }

    /// **Review 5's F4: a checkpoint that a pin kept answers `KeptByPin`, not "truncated".** Until
    /// then it answered `Ok(0)` for both, so a DROP reached the kept-log state with the detector's
    /// answer in hand and threw it away. Red only under a mutant: the type is new.
    #[test]
    fn a_checkpoint_that_a_pin_kept_answers_kept_by_pin() {
        let (_bp, wal, txn, _catalog, _owned, _dir) = table_to_drop();
        let base = wal.base_lsn.load(Ordering::SeqCst);
        let _pin = wal.pin(base).expect("pin the log at its base");
        assert!(base < wal.next_lsn.load(Ordering::SeqCst), "premise: the pin is not below the log's end");
        assert_eq!(
            txn.checkpoint_keeping_owed().expect("a checkpoint under a pin failed"),
            CheckpointOutcome::KeptByPin,
            "a checkpoint that a pin kept answered as if it had truncated"
        );
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "premise: the pin did not keep the log");
    }

    /// **Review 5's F3: a mismatch that is later released does not keep refusing the DROP of its
    /// table.** An unrecorded mismatch stays owed and is retried at every checkpoint. If its slot
    /// later becomes free (its live tuple deleted, committed and released), the retry releases it and
    /// the `Ok` arm drops it. At `246f14f` the entry stayed in `unrecorded`, so every DROP of the table
    /// was refused, for the rest of the process, with a message saying a mismatch could not be written.
    #[test]
    fn a_mismatch_that_later_releases_does_not_block_a_drop() {
        let (bp, _wal, txn, _t2, r, quarantine, _dir) = committed_mismatch(true);
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: the unrecorded mismatch is not owed");
        let frame_i = bp.fetch_page(r.page_id).unwrap();
        {
            let mut frame = bp.frame_write(frame_i);
            let mut page = Page::deserialize(frame.data).unwrap();
            page.slot_arr[r.slot as usize] = crate::storage::heap_page::SlotEntry { offset: 0, length: 0 };
            frame.data = page.serialize().unwrap();
        }
        bp.unpin_page(r.page_id, true);
        assert_eq!(txn.retry_pending_releases(), 0, "premise: the retry did not release the slot, which is free now");
        std::fs::remove_dir(&quarantine).unwrap();
        let ran = std::cell::Cell::new(false);
        // D250 (lane `lane_d250_drop_logged.md` §3.3): the record names the raw heap's one root twice.
        let record = DdlRecord {
            op: DdlOp::DropTable,
            table: "raw".into(),
            dir_root: r.dir_root,
            time_travel_root: r.dir_root,
            columns: Vec::new(),
        };
        // D229: the pages the DROP would free; this raw heap's are not what the test is about.
        txn.drop_checkpointed(record, Vec::new(), || {
            ran.set(true);
            Ok(())
        })
        .unwrap_or_else(|e| panic!("a mismatch that has since been released still refuses the DROP of its table: {e}"));
        assert!(ran.get(), "the DROP answered Ok without running its mutation");
    }

    /// **Review 5's F6: a fresh log starts a fresh quarantine.** The dedupe key (`txn=`, `dir_root=`,
    /// `page=`, `slot=`) has no database incarnation, and a database created again at the same path
    /// reissues transaction ids. So a quarantine left by the earlier database would make a colliding
    /// record of the new one look already recorded. The earlier file is moved aside, not deleted: it
    /// is evidence.
    #[test]
    fn a_fresh_log_starts_a_fresh_quarantine() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("fresh.wal");
        let quarantine = dir.path().join("fresh.wal.release-quarantine");
        let old = "txn=5 dir_root=3 page=7 slot=1 found=live error=an earlier database's mismatch\n";
        std::fs::write(&quarantine, old).unwrap();
        let _wal = WalManager::new(wal_path).unwrap();
        assert!(
            !quarantine.exists(),
            "a fresh log kept an earlier database's quarantine at its path, so a colliding record of the new \
             database would be taken as already recorded"
        );
        let aside: Vec<PathBuf> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("fresh.wal.release-quarantine.before-"))
            .collect();
        assert_eq!(aside.len(), 1, "the earlier quarantine was not moved aside once: {aside:?}");
        assert_eq!(std::fs::read_to_string(&aside[0]).unwrap(), old, "the earlier quarantine's evidence was not kept");
    }

    /// **Review 4's finding 3: a mismatch is recorded once, however often it is found.** A dropped
    /// mismatch writes no `HeapRelease`, so every open before the log truncates re-derives the release
    /// and finds it again (`finish_releases`). At `8d492bf` each of those appended another line, so
    /// the file grew by one per open while anything kept the log.
    #[test]
    fn a_mismatch_found_again_is_recorded_once() {
        let (_bp, _wal, txn, t2, r, quarantine, _dir) = committed_mismatch(false);
        let key = format!("txn={t2} dir_root={} page={} slot={} ", r.dir_root, r.page_id, r.slot);
        let lines = |q: &Path| {
            std::fs::read_to_string(q)
                .expect("the mismatch was not recorded")
                .lines()
                .filter(|l| l.starts_with(&key))
                .count()
        };
        assert_eq!(lines(&quarantine), 1, "premise: the COMMIT's mismatch was not recorded once");
        txn.finish_releases(t2, &[r]);
        assert_eq!(
            lines(&quarantine),
            1,
            "the same mismatch was recorded twice, so the file grows by one line per open until the log truncates"
        );
        // Review 7's F1 (the lead's decision): counting lines cannot see a dedupe branch that FAILS. At
        // `fe2fd84` it fsynced a read-only handle, which Windows refuses, so the mismatch found again
        // stayed owed and unrecorded for good while this test still counted one line.
        assert_eq!(
            txn.owed_releases(),
            0,
            "the mismatch found again is still owed: recording it once failed, so no checkpoint truncates the log"
        );
    }

    /// **Review 7's F4 (lane §21.16 test F4): a `Retry` after a SUCCESSFUL read records what the page
    /// shows now, not the stored observation.** The stored line is the last observation of a page
    /// that could not be read. When the retry DID read the page and found the slot releasable, and
    /// failed later (here at the log append, on a poisoned log), the mismatch is gone. At `fe2fd84`
    /// the `Retry` arm wrote the stored `found=live` line anyway.
    #[test]
    fn a_retry_after_a_successful_read_records_what_the_page_shows_now() {
        let (bp, wal, txn, t2, r, quarantine, _dir) = committed_mismatch(true);
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: the unrecorded mismatch is not owed");
        std::fs::remove_dir(&quarantine).unwrap();
        let frame_i = bp.fetch_page(r.page_id).unwrap();
        {
            let mut frame = bp.frame_write(frame_i);
            let mut page = Page::deserialize(frame.data).unwrap();
            page.slot_arr[r.slot as usize] = crate::storage::heap_page::SlotEntry { offset: 0, length: 0 };
            frame.data = page.serialize().unwrap();
        }
        bp.unpin_page(r.page_id, true);
        wal.poison("injected: the release reads the page, then its append is refused");
        txn.retry_pending_releases();
        let key = format!("txn={t2} dir_root={} page={} slot={} ", r.dir_root, r.page_id, r.slot);
        let recorded = std::fs::read_to_string(&quarantine).expect("the retry recorded nothing");
        let line = recorded
            .lines()
            .find(|l| l.starts_with(&key))
            .unwrap_or_else(|| panic!("the quarantine does not record `{key}`:\n{recorded}"));
        assert!(
            line.contains("found=free") && !line.contains("found=live"),
            "a retry that read the page and found the slot free recorded an out-of-date observation: {line}"
        );
    }

    /// **Review 7's F4 (lane §21.16 test F4b): a stored line written from a retry is counted.** The
    /// page is not read (`FAIL_RELEASES`), so the stored observation is the latest, and it is written.
    /// Its release stays owed, so it is not a dropped mismatch; `STORED_MISMATCH_LINES` counts it.
    #[test]
    fn a_stored_line_written_from_a_retry_is_counted() {
        let (_bp, _wal, txn, t2, r, quarantine, _dir) = committed_mismatch(true);
        std::fs::remove_dir(&quarantine).unwrap();
        let before = stored_mismatch_lines();
        FAIL_RELEASES.with(|f| f.set(1));
        txn.retry_pending_releases();
        assert_eq!(FAIL_RELEASES.with(|f| f.replace(0)), 0, "premise: the retry did not meet the injected failure");
        let key = format!("txn={t2} dir_root={} page={} slot={} ", r.dir_root, r.page_id, r.slot);
        let recorded = std::fs::read_to_string(&quarantine).expect("the retry recorded nothing");
        assert!(
            recorded.lines().any(|l| l.starts_with(&key) && l.contains("found=live")),
            "the stored observation was not written:\n{recorded}"
        );
        assert!(stored_mismatch_lines() > before, "a mismatch recorded from its stored observation was counted nowhere");
    }

    /// **Review 7's F2 (lane §21.16 test F2): a mismatch found again rewrites the quarantine instead of
    /// trusting a second fsync.** Its line was read from the page cache and may not be durable yet;
    /// after a reported writeback error a second fsync can answer `Ok` without the data. So the file
    /// is replaced atomically with the same bytes: temporary, fsync, rename, directory fsync.
    #[test]
    fn a_mismatch_found_again_rewrites_the_quarantine_instead_of_trusting_a_second_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let q = dir.path().join("x.wal.release-quarantine");
        let key = "txn=5 dir_root=3 page=7 slot=1 ";
        let content = format!("{key}found=live error=earlier\n");
        std::fs::write(&q, &content).unwrap();
        let ops = crate::storage::atomic_file::RecordingOps::new();
        append_once_durably(&ops, &q, key, &format!("{key}found=live error=again\n")).unwrap();
        let tmp = crate::storage::atomic_file::temp_path(&q).unwrap();
        assert_eq!(
            ops.shape(),
            vec![
                ("write", tmp.clone()),
                ("sync_file", tmp.clone()),
                ("rename", tmp),
                ("sync_dir", crate::storage::atomic_file::parent_dir(&q).to_path_buf()),
            ],
            "a mismatch found again did not rewrite the quarantine durably"
        );
        match &ops.ops()[0] {
            crate::storage::atomic_file::Op::Write(_, bytes) => {
                assert_eq!(bytes.as_slice(), content.as_bytes(), "the rewrite changed the quarantine's content")
            }
            other => panic!("the first operation is not the write: {other:?}"),
        }
    }

    /// **Review 7's F5 (lane §21.16 test F5): two moves at the same clock reading keep both copies,
    /// through the caller.** Test 4 calls `aside_path` directly, so a caller that built the name
    /// itself would pass it; this moves twice at `nanos = 0` through `start_fresh_quarantine_at`.
    #[test]
    fn two_moves_at_the_same_clock_reading_keep_both_copies() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("x.wal");
        let q = release_quarantine(&wal_path);
        std::fs::write(&q, "the first database's line\n").unwrap();
        start_fresh_quarantine_at(&wal_path, 0).unwrap();
        std::fs::write(&q, "the second database's line\n").unwrap();
        start_fresh_quarantine_at(&wal_path, 0).unwrap();
        assert!(!q.exists(), "the second quarantine was not moved aside");
        let mut copies: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("x.wal.release-quarantine.before-0"))
            .map(|p| std::fs::read_to_string(p).unwrap())
            .collect();
        copies.sort();
        assert_eq!(
            copies,
            vec!["the first database's line\n".to_string(), "the second database's line\n".to_string()],
            "a second move at the same clock reading replaced the first copy"
        );
    }

    /// **Review 7's F5 (lane §21.16 test F5b): a candidate name that cannot be statted is refused, not
    /// taken as free.** A 250-byte quarantine name gives a `.before-0` candidate past NAME_MAX, so its
    /// stat fails with `ENAMETOOLONG`; only `NotFound` means free.
    #[cfg(unix)]
    #[test]
    fn a_name_that_cannot_be_statted_is_refused_not_taken_as_free() {
        let dir = tempfile::tempdir().unwrap();
        let q = dir.path().join("q".repeat(250));
        match aside_path(&q, 0) {
            Err(e) => assert_ne!(e.kind(), std::io::ErrorKind::NotFound, "premise: the refusal is not the stat's error: {e}"),
            Ok(p) => panic!("a candidate whose stat failed was taken as a free name: {}", p.display()),
        }
    }

    /// **Review 7's F6 (lane §21.16 test F6): a new incarnation forgets the release state together with
    /// the quarantine.** The owed releases and unrecorded mismatches describe the replaced database.
    #[test]
    fn a_new_incarnation_forgets_the_release_state_with_the_quarantine() {
        let (_bp, _wal, txn, _t2, _r, quarantine, _dir) = committed_mismatch(true);
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: nothing is owed");
        assert_eq!(txn.unrecorded.lock().unwrap().len(), 1, "premise: nothing is unrecorded");
        txn.start_new_incarnation().expect("the new incarnation failed");
        assert!(txn.pending_releases.lock().unwrap().is_empty(), "the replaced database's owed releases survived the new incarnation");
        assert!(txn.unrecorded.lock().unwrap().is_empty(), "the replaced database's unrecorded mismatches survived the new incarnation");
        assert!(std::fs::symlink_metadata(&quarantine).is_err(), "the replaced database's quarantine is still at its path");
    }

    /// **D229 review 2's F11 with a transaction manager wired: a new incarnation also moves the drop intent
    /// aside and forgets the pending frees** (PREREG amendment 19). An intent pending at an install names
    /// the REPLACED database's page ids. Kept in the file, the next open frees them under the installed
    /// database; kept in memory, this manager's next checkpoint does. Their quarantine goes too: it held
    /// those ids back from the allocator for the replaced database only.
    #[test]
    fn a_new_incarnation_moves_the_drop_intent_aside_and_forgets_the_pending_frees() {
        let (bp, wal, txn, dir) = setup();
        let pages = vec![bp.new_page().unwrap(), bp.new_page().unwrap()];
        free_intent::store(&OsFileOps, &wal.path, &[FreeIntent { table: "replaced".into(), dir_root: pages[0], pages: pages.clone() }])
            .unwrap();
        assert_eq!(txn.adopt_free_intents().unwrap(), 1, "premise: the replaced database's intent was not adopted");
        assert_eq!(bp.disk_manager.quarantined(), pages, "premise: the intent's pages are not quarantined");
        let intent = free_intent::intent_path(&wal.path);
        let old = std::fs::read(&intent).unwrap();
        txn.start_new_incarnation().expect("the new incarnation failed");
        assert!(
            std::fs::symlink_metadata(&intent).is_err(),
            "the replaced database's drop intent is still at its path, so the next open would free its pages under \
             the new database"
        );
        let prefix = format!("{}.before-", intent.file_name().unwrap().to_string_lossy());
        let aside: Vec<PathBuf> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with(&prefix))
            .collect();
        assert_eq!(aside.len(), 1, "the replaced database's intent was not moved aside once: {aside:?}");
        assert_eq!(std::fs::read(&aside[0]).unwrap(), old, "the replaced database's intent was not kept as evidence");
        assert_eq!(
            txn.pending_free_pages(),
            Vec::<u32>::new(),
            "the replaced database's pending frees survived the new incarnation, so this manager's next checkpoint \
             would free them under the new database"
        );
        assert_eq!(
            bp.disk_manager.quarantined(),
            Vec::<u32>::new(),
            "the replaced database's quarantine still holds pages back from the new database's allocator"
        );
    }

    /// **Review 4's finding 4: a DROP must not discard a mismatch it could not record.** The DROP
    /// discards the releases owed on the heaps it frees, and then truncates: for a mismatch whose
    /// quarantine write failed, that truncation removes the only record of it (review 3's decision 6).
    /// So the DROP retries first, and is refused while such a mismatch is still unrecorded.
    #[test]
    fn a_drop_refuses_to_discard_a_mismatch_it_could_not_record() {
        let (_bp, _wal, txn, t2, r, quarantine, _dir) = committed_mismatch(true);
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: the unrecorded mismatch is not owed");
        let ran = std::cell::Cell::new(false);
        // D250 (lane `lane_d250_drop_logged.md` §2): `drop_checkpointed` takes the DROP's record now.
        // This raw heap has no time-travel heap, so the record names its one root twice.
        let record = || DdlRecord {
            op: DdlOp::DropTable,
            table: "raw".into(),
            dir_root: r.dir_root,
            time_travel_root: r.dir_root,
            columns: Vec::new(),
        };
        // D229: the pages the DROP would free; this raw heap's are not what the test is about.
        let e = match txn.drop_checkpointed(record(), Vec::new(), || {
            ran.set(true);
            Ok(())
        }) {
            Err(e) => e,
            Ok(()) => panic!("the DROP discarded a mismatch it could not record, and truncated the log that held the only record of it"),
        };
        assert!(!ran.get(), "the DROP ran its mutation before it was refused: {e}");
        assert!(e.to_string().contains("quarantine"), "the DROP was refused, but not for the unrecorded mismatch: {e}");

        std::fs::remove_dir(&quarantine).unwrap();
        txn.drop_checkpointed(record(), Vec::new(), || {
            ran.set(true);
            Ok(())
        })
        .unwrap_or_else(|e| panic!("the DROP was refused once the mismatch could be recorded: {e}"));
        assert!(ran.get(), "the DROP answered Ok without running its mutation");
        let recorded = std::fs::read_to_string(&quarantine).expect("the DROP's retry did not record the mismatch");
        let key = format!("txn={t2} dir_root={} page={} slot={} ", r.dir_root, r.page_id, r.slot);
        assert!(recorded.lines().any(|l| l.starts_with(&key)), "the quarantine does not record `{key}`:\n{recorded}");
    }

    /// **Review 6's caveat 1 (lane §21.14 test 1): a retry that fails before it reads the page does not
    /// let a DROP discard an unrecorded mismatch.** At `ed6e901` the `Retry` arm removed the
    /// `unrecorded` entry on any retryable failure, including one that comes before the slot is looked
    /// at (the seam, `fetch_page`, `Page::deserialize`). One such failure in the DROP's own retry let
    /// its count pass, and the DROP discarded the mismatch and truncated the only record of it: review
    /// 4's finding 4, back. The entry now keeps the line it could not write, a `Retry` writes that line,
    /// and the entry leaves only once it is written. So the DROP is refused while the quarantine is
    /// unwritable, and goes ahead once it is writable, although the page still cannot be read.
    #[test]
    fn a_retry_that_fails_before_reading_the_page_does_not_let_a_drop_discard_an_unrecorded_mismatch() {
        let (_bp, _wal, txn, t2, r, quarantine, _dir) = committed_mismatch(true);
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: the unrecorded mismatch is not owed");
        let ran = std::cell::Cell::new(false);
        // D250 (lane `lane_d250_drop_logged.md` §2): `drop_checkpointed` takes the DROP's record, as in
        // `a_drop_refuses_to_discard_a_mismatch_it_could_not_record`.
        let record = || DdlRecord {
            op: DdlOp::DropTable,
            table: "raw".into(),
            dir_root: r.dir_root,
            time_travel_root: r.dir_root,
            columns: Vec::new(),
        };
        FAIL_RELEASES.with(|f| f.set(1));
        // D229: the pages the DROP would free; this raw heap's are not what the test is about.
        let refused = txn.drop_checkpointed(record(), Vec::new(), || {
            ran.set(true);
            Ok(())
        });
        assert_eq!(FAIL_RELEASES.with(|f| f.replace(0)), 0, "premise: the DROP's retry did not meet the injected failure");
        let e = match refused {
            Err(e) => e,
            Ok(()) => panic!(
                "one release retry that failed before reading the page let the DROP discard a mismatch it \
                 could not record, and truncate the log that held the only record of it"
            ),
        };
        assert!(!ran.get(), "the DROP ran its mutation before it was refused: {e}");
        assert!(e.to_string().contains("quarantine"), "the DROP was refused, but not for the unrecorded mismatch: {e}");

        std::fs::remove_dir(&quarantine).unwrap();
        FAIL_RELEASES.with(|f| f.set(u32::MAX));
        let written = txn.drop_checkpointed(record(), Vec::new(), || {
            ran.set(true);
            Ok(())
        });
        FAIL_RELEASES.with(|f| f.set(0));
        written.unwrap_or_else(|e| {
            panic!(
                "the DROP was refused although the mismatch's last observation could be written, so a page \
                 that cannot be read would block the DROP for good: {e}"
            )
        });
        assert!(ran.get(), "the DROP answered Ok without running its mutation");
        let recorded = std::fs::read_to_string(&quarantine).expect("the mismatch was never written to the quarantine");
        let key = format!("txn={t2} dir_root={} page={} slot={} ", r.dir_root, r.page_id, r.slot);
        assert!(recorded.lines().any(|l| l.starts_with(&key)), "the quarantine does not record `{key}`:\n{recorded}");
    }

    /// **Review 6's caveat 2 (lane §21.14 test 2): a fresh log whose quarantine could not be moved
    /// moves it at the next open.** At `ed6e901` the fresh header was written and synced before the
    /// move, so a failed move left a 24-byte log that no later open counts as fresh, and the earlier
    /// database's quarantine stayed current for good. The move now comes first: until it succeeds the
    /// log stays empty, and every open retries it.
    #[cfg(unix)]
    #[test]
    fn a_fresh_log_whose_quarantine_could_not_be_moved_moves_it_at_the_next_open() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("x.wal");
        let quarantine = dir.path().join("x.wal.release-quarantine");
        std::fs::write(&wal_path, b"").unwrap();
        std::fs::write(&quarantine, "txn=5 dir_root=3 page=7 slot=1 found=live error=an earlier database's mismatch\n").unwrap();
        let set_mode = |mode: u32| std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(mode)).unwrap();
        set_mode(0o555);
        let refused = WalManager::new(wal_path.clone());
        set_mode(0o755);
        assert!(
            refused.is_err(),
            "premise failed: a read-only directory did not refuse the move (running as root?), so nothing here is tested"
        );
        let _wal = WalManager::new(wal_path).expect("the second open of the log failed");
        assert!(
            !quarantine.exists(),
            "a log whose first open could not move the earlier quarantine aside never moved it: the earlier \
             database's file stays current, and a colliding record of the new one is taken as already recorded"
        );
    }

    /// **Review 6's finding 5 (lane §21.14 test 4): a moved quarantine never replaces an earlier copy.**
    /// A clock before 1970 gives every move the name `.before-0`, and `rename` silently replaces an
    /// existing target, so the earlier copy, the evidence the move exists to keep, would be lost.
    #[test]
    fn a_moved_quarantine_never_replaces_an_earlier_copy() {
        let dir = tempfile::tempdir().unwrap();
        let q = dir.path().join("x.wal.release-quarantine");
        let earlier = dir.path().join("x.wal.release-quarantine.before-0");
        std::fs::write(&earlier, "the earlier copy\n").unwrap();
        // `.unwrap()`: `aside_path` answers `io::Result` since review 7's F5. The assertions are unchanged.
        let aside = aside_path(&q, 0).unwrap();
        assert_ne!(aside, q, "the quarantine would be moved onto itself");
        assert_ne!(
            aside, earlier,
            "a second move at the same clock reading takes the earlier copy's name, and the rename would replace it"
        );
        assert!(std::fs::symlink_metadata(&aside).is_err(), "the name chosen for the move is already taken: {}", aside.display());
    }

    /// **Review 4's finding 5: a truncation a pin cancelled is not a truncation.** `WalManager::truncate`
    /// keeps the log, and answers `Ok`, while a pin is below its end. At `8d492bf` the checkpoint then
    /// behaved as if it had truncated: it counted no deferral, and it re-appended the schema into the
    /// kept log, a second copy of every declaration.
    #[test]
    fn a_checkpoint_that_a_pin_kept_is_counted_and_replays_nothing() {
        fn ddl_records(w: &WalManager) -> usize {
            walk_log(w).iter().filter(|r| matches!(r.kind, RecKind::Ddl { .. })).count()
        }
        let (_bp, wal, txn, _catalog, _owned, _dir) = table_to_drop();
        let base = wal.base_lsn.load(Ordering::SeqCst);
        let _pin = wal.pin(base).expect("pin the log at its base");
        let before = ddl_records(&wal);
        assert!(before >= 1, "premise: the log holds no declaration a truncation would replay");
        let deferred = deferred_checkpoints();
        txn.checkpoint().expect("a checkpoint under a pin failed");
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "premise: the pin did not keep the log");
        assert_eq!(ddl_records(&wal), before, "a checkpoint the pin kept replayed the schema into the log, as if it had truncated it");
        assert!(deferred_checkpoints() > deferred, "a checkpoint the pin kept was not counted as a deferral");
    }

    /// A page file that counts its syncs. Only a checkpoint syncs the page file
    /// (`bp.disk_manager.sync()`), so on one database this counts checkpoint flushes.
    struct SyncCountingFile {
        file: std::fs::File,
        syncs: Arc<AtomicU64>,
    }

    impl crate::storage::storage::Storage for SyncCountingFile {
        fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
            crate::storage::storage::Storage::pwrite(&self.file, buf, offset)
        }
        fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            crate::storage::storage::Storage::pread(&self.file, buf, offset)
        }
        fn sync_all(&self) -> std::io::Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            crate::storage::storage::Storage::sync_all(&self.file)
        }
        fn sync_data(&self) -> std::io::Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            crate::storage::storage::Storage::sync_data(&self.file)
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            crate::storage::storage::Storage::set_len(&self.file, len)
        }
        fn len(&self) -> std::io::Result<u64> {
            crate::storage::storage::Storage::len(&self.file)
        }
    }

    /// **Review 3's decision 3: while a release is owed, the automatic checkpoint runs once per
    /// interval, not once per commit.** At `7cede54` a deferral left `commits_since_checkpoint` over the
    /// threshold, so every later commit with nothing else open flushed the whole pool and synced.
    #[test]
    fn while_a_release_is_owed_the_automatic_checkpoint_flushes_once_per_interval() {
        const K: u64 = 6;
        let interval = super::checkpoint_interval();
        assert!(interval > K, "premise: FERRODB_CHECKPOINT_INTERVAL={interval} is not above {K}, so every commit is due anyway and this measures nothing");
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.path().join("cadence.db")).unwrap();
        let syncs = Arc::new(AtomicU64::new(0));
        let dm = Arc::new(DiskManager::with_storage(Arc::new(SyncCountingFile { file, syncs: syncs.clone() })).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let wal = Arc::new(WalManager::new(dir.path().join("cadence.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        // A committed delete whose release fails at the commit and at every retry.
        let t1 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![7; 40])).unwrap();
        txn.commit(t1).unwrap();
        let t2 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.delete(rid).unwrap();
        FAIL_RELEASES.with(|f| f.set(u32::MAX));
        txn.commit(t2).unwrap();
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: the release is not owed");

        // The next commit is due.
        txn.commits_since_checkpoint.store(interval - 1, Ordering::SeqCst);
        let before = syncs.load(Ordering::SeqCst);
        let deferred = deferred_checkpoints();
        for i in 0..K {
            let t = txn.begin().unwrap();
            heap.set_transaction(txn.clone(), t);
            heap.insert(Tuple::new(vec![i as u8; 40])).unwrap();
            txn.commit(t).unwrap();
        }
        let flushes = syncs.load(Ordering::SeqCst) - before;
        assert_eq!(txn.pending_releases.lock().unwrap().len(), 1, "premise: the release stopped being owed, so nothing was deferred");
        assert!(deferred_checkpoints() > deferred, "the due commit's checkpoint kept the log without counting a deferral");
        assert_eq!(
            flushes,
            1 + (K - 1) / interval,
            "{K} commits with a release owed flushed and synced the pool {flushes} times: a deferred checkpoint must \
             reset the trigger, so the retry runs once per {interval} commits"
        );
        FAIL_RELEASES.with(|f| f.set(0));
    }
}

// **The seam's test half, BELOW the tests module on purpose.** `tests/d53_private_root_allowlist.rs`
// treats everything before a file's first `#[cfg(test)]` as statement paths. At `7cede54` this seam
// sat near the top of the file and hid everything after it from that scanner: the file's first
// `#[cfg(test)]` moved from line 1661 (at `368d0e1`) to line 224. Found while fixing review 3.
#[cfg(test)]
thread_local! {
    /// **Test-only: the next N releases on this thread fail as an I/O error would**, a retryable
    /// failure, so the pending path (F2, N1) can be driven without a failing disk. Thread-local, so a
    /// test that arms it cannot fail a release in a test running beside it.
    pub(crate) static FAIL_RELEASES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// The seam's test half: fail this release if `FAIL_RELEASES` still has failures to hand out.
#[cfg(test)]
fn injected_release_failure() -> bool {
    FAIL_RELEASES.with(|f| {
        let left = f.get();
        f.set(left.saturating_sub(1));
        left > 0
    })
}
