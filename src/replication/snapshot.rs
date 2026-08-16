//! E12 — the initial snapshot, and the handoff to the stream.
//!
//! A consumer that connects to a database with a million rows already in it and starts following
//! the log learns about row 1,000,001 and never learns about the first million. The stream is
//! complete from the moment it starts and says nothing about what came before, so a CDC source is
//! not finished until it can also answer "what is there now".
//!
//! # The handoff LSN is the only hard part, and the two ways to be wrong are not equal
//!
//! The snapshot has to join the stream at some LSN, and the scan is not instantaneous — writes land
//! while it runs. There are two directions to be wrong and they have very different costs:
//!
//! - **Too late** (hand off at the LSN *after* the scan): every change that landed during the scan
//!   and was not picked up by it is skipped. The consumer never sees those rows and cannot detect
//!   that it did not — a feed missing records looks exactly like a feed that had none. Permanent,
//!   silent data loss.
//! - **Too early** (hand off at the LSN *before* the scan): changes that landed during the scan are
//!   delivered twice, once in the snapshot and once by the stream. The consumer sees a duplicate,
//!   which it can absorb with an idempotence key — `commit_lsn`, or the row's own primary key.
//!
//! So the LSN is captured **before** the scan begins. That is at-least-once by construction, and it
//! is the standard CDC contract for exactly this reason: duplication is recoverable and loss is
//! not. Choosing the other direction would make the numbers look cleaner and the feed wrong.
//!
//! # Consistency comes from the database, not from this module
//!
//! The rows themselves are read by the caller, through the engine's own MVCC snapshot isolation —
//! a `SELECT` sees one consistent version of the table regardless of what commits during it. This
//! module does not reimplement a scan, because a hand-rolled page walk would have to re-derive
//! visibility rules that already exist and are already tested. It brackets the read with LSNs and
//! reports what it observed.
//!
//! [`Snapshot::concurrent_writes`] says whether the log moved during the scan, which is what
//! distinguishes "this handoff is exact" from "this handoff will re-deliver some changes". Both are
//! correct; only one is tidy, and a caller that wants to know can ask.
//!
//! # [`snapshot_table_exact`]: the same handoff with the overlap removed
//!
//! Everything above is true of [`snapshot_table`], which brackets a read it knows nothing about and
//! therefore cannot do better than at-least-once. It is kept because it needs nothing but a WAL.
//!
//! [`snapshot_table_exact`] is given the [`TxnManager`] as well, and that one extra thing is enough
//! to make the cutover **exact in both directions**. It opens the read inside a transaction, so it
//! knows precisely which transactions the rows it wrote out already contain, and it hands that set
//! back alongside the resume LSN. A stream built with
//! [`FeedStreamer::resuming_after_snapshot`](super::stream::FeedStreamer::resuming_after_snapshot)
//! drops events from those transactions and delivers everything else, so every change appears
//! exactly once across snapshot and stream.
//!
//! Two things had to change for that, and both are corrections rather than additions:
//!
//! - **The resume point moved earlier, not later.** Taking it "just before the scan" is not early
//!   enough. A transaction already in flight at that moment is excluded from the snapshot — MVCC
//!   does not show a reader another transaction's uncommitted work — and its records sit *below*
//!   that LSN. A stream starting there meets its `Commit` having never read its changes, emits
//!   nothing for it, and the rows reach nobody, silently. So the resume point is pulled back to the
//!   oldest in-flight transaction's `Begin`. That is a **gap** the at-least-once path also has, and
//!   it is the more serious of the two failures.
//! - **The overlap that pulling back creates is closed by transaction id, not by LSN.** Between the
//!   new resume point and the read there are commits the snapshot *did* see, interleaved in the log
//!   with ones it did not. No byte offset separates those two sets, which is the whole reason
//!   skipping by LSN alone cannot be exact.
//!
//! # What this does not promise
//!
//! - **Not end-to-end exactly-once delivery.** A consumer that acts on an event and dies before
//!   recording its cursor sees that event again on restart. That is a property of the consumer's
//!   checkpointing, not of the cutover, and no source can supply it.
//! - **Not a multi-table snapshot, as written.** [`snapshot_table_exact`] opens one reader per
//!   call, so snapshotting two tables through it gives two readers with two different boundaries —
//!   fine if each table has its own stream, wrong if one stream carries both, because no single
//!   boundary describes them. The consistent version is available and is simply a different shape:
//!
//!   1. call [`TxnManager::begin_snapshot_read`] directly,
//!   2. read every table under the one reader it returns,
//!   3. close it with [`TxnManager::end_read_only`],
//!   4. build one [`SnapshotBoundary`] naming **every table read in step 2** — the transaction set
//!      answers *when*, not *what*, so a boundary that omits a table it delivered re-sends that
//!      table, and one that names a table it did not deliver silently drops that table's entire
//!      pre-cutover history,
//!   5. **hold `handoff.pin` until the subscription exists**, then stream from `resume_lsn`.
//!
//!   Step 5 is the one this recipe used to omit, and omitting it is not a slow leak — it breaks the
//!   cutover outright. A checkpoint between the read and the subscription truncates the log past
//!   the resume point, and the stream is refused with *"cannot pin lsn 2597: the log has already
//!   been truncated to base 4260"*. It went unnoticed because the default checkpoint interval is
//!   256 commits, so nothing truncates during a small test; at `FERRODB_CHECKPOINT_INTERVAL=1` the
//!   recipe failed on the first try. The pin now comes back attached to the handoff, so following
//!   the recipe holds it automatically — dropping the handoff early is the remaining way to get
//!   this wrong, which is why step 5 says what to hold rather than what to call.

use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Arc;

use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::wal::log::{WalManager, WalPin};
use crate::wal::txn::{Snapshot as TxnSnapshot, TxnManager};

use super::jsonl::write_feed;
use super::logical::{ChangeEvent, ChangeOp};

/// What a snapshot captured, and where the stream must resume.
#[derive(Debug)]
pub struct Snapshot {
    pub table: String,
    /// Rows written out.
    pub rows: usize,
    /// **Where the consumer resumes streaming.** Captured before the scan, so a change that raced
    /// the scan is re-delivered rather than skipped.
    pub lsn: u64,
    /// The durable frontier after the scan finished.
    pub lsn_after: u64,
    /// Whether the log moved while the scan ran. When false, the handoff is exact and nothing will
    /// be re-delivered; when true, changes between `lsn` and `lsn_after` may arrive twice.
    pub concurrent_writes: bool,
    /// A claim on the log at `lsn`, for the same reason [`ExactSnapshot::pin`] holds one: the window
    /// between this returning and the consumer subscribing is exactly where a checkpoint lands.
    ///
    /// This path used to return `lsn` bare and leave the claim to the caller. Every caller then
    /// forgot, invisibly, because the default checkpoint interval is 256 commits and nothing
    /// truncates during a small test; at `FERRODB_CHECKPOINT_INTERVAL=1` the handoff was refused
    /// with *"cannot pin lsn 3231: the log has already been truncated to base 3911"*. Keep it alive
    /// until the [`Subscription`](super::stream::Subscription) that replaces it exists.
    ///
    /// Holding it costs `Clone` and `PartialEq` on this struct, which nothing used.
    pub pin: WalPin,
}

impl Snapshot {
    /// Number of bytes of log that may be re-delivered after the handoff.
    pub fn overlap_bytes(&self) -> u64 {
        self.lsn_after.saturating_sub(self.lsn)
    }
}

/// Take a snapshot of one table and write it as JSON Lines, returning the handoff point.
///
/// `read_rows` is the caller's — it should perform a single consistent read (a `SELECT` through the
/// executor) and return the column names alongside the rows. Doing it by closure keeps this module
/// out of the SQL layer and, more importantly, keeps the LSN bracketing *around* whatever read the
/// caller actually performs rather than around one this module imagines.
pub fn snapshot_table<W, F>(
    table: &str,
    wal: &Arc<WalManager>,
    w: &mut W,
    read_rows: F,
) -> Result<Snapshot, FerroError>
where
    W: Write,
    F: FnOnce() -> Result<(Vec<String>, Vec<Vec<Value>>), FerroError>,
{
    use std::sync::atomic::Ordering;

    // Durable first: an LSN that has not been flushed is not a position a consumer can resume from,
    // because a crash would move the frontier backwards underneath it.
    wal.flush()?;
    let lsn = wal.flushed_lsn.load(Ordering::SeqCst);

    // Claimed immediately, before the scan runs. The scan is the slow part and the checkpointer runs
    // during it, so a claim taken afterwards would already be too late. Takes an `Arc` rather than a
    // bare reference for this: a pin owns a handle on the log it claims, because releasing on drop
    // is the only thing that keeps a forgotten snapshot from freezing the WAL for ever.
    let pin = wal.pin(lsn)?;

    let (columns, rows) = read_rows()?;

    wal.flush()?;
    let lsn_after = wal.flushed_lsn.load(Ordering::SeqCst);

    let columns = Arc::new(columns);
    let events: Vec<ChangeEvent> = rows
        .into_iter()
        .map(|values| ChangeEvent {
            txn_id: 0,
            lsn,
            commit_lsn: lsn,
            commit_end_lsn: lsn,
            table: table.to_string(),
            columns: Arc::clone(&columns),
            // Snapshot rows are `READ`, not `INSERT`. A consumer must be able to tell "this row
            // exists" from "this row was just created" — replaying a snapshot as inserts would make
            // every existing row look like new activity, and anything counting events would be
            // wrong by the size of the table.
            op: ChangeOp::Read { row: values },
        })
        .collect();

    let n = write_feed(&events, w)?;

    Ok(Snapshot {
        table: table.to_string(),
        rows: n,
        lsn,
        lsn_after,
        concurrent_writes: lsn_after != lsn,
        pin,
    })
}

/// **Everything a stream needs to join a snapshot exactly, kept as one value.**
///
/// The three parts travel together because each is unsafe without the others, and unsafe silently:
///
/// - `resume_lsn` without `txns` re-delivers every commit between the resume point and the read.
/// - `txns` without `tables` is the one a review caught, and it is the dangerous one. **The
///   transaction set answers *when*, not *what*.** A snapshot of `orders` says nothing whatever
///   about `shipments` — but `shipments` rows were committed by transactions that set contains, so
///   a filter keyed on the transaction alone drops them. They were in no snapshot, the stream was
///   their only path, and the drop is reported as legitimate suppression. That is permanent, silent
///   loss of one table's history caused by snapshotting a different table.
/// - `tables` without `resume_lsn` lets a caller start the stream above the resume point, which
///   re-opens the gap the early resume point exists to close.
///
/// So it is built once, by whatever took the snapshot, from what that snapshot actually did.
#[derive(Debug, Clone)]
pub struct SnapshotBoundary {
    tables: BTreeSet<String>,
    txns: TxnSnapshot,
    resume_lsn: u64,
}

impl SnapshotBoundary {
    /// `tables` must be exactly the tables the snapshot wrote rows for — no more, no fewer.
    ///
    /// Naming one it did not deliver suppresses that table's pre-cutover history; omitting one it
    /// did deliver re-delivers it. Neither is detectable downstream.
    pub fn new(tables: BTreeSet<String>, txns: TxnSnapshot, resume_lsn: u64) -> Self {
        SnapshotBoundary { tables, txns, resume_lsn }
    }

    /// Where the stream must start.
    pub fn resume_lsn(&self) -> u64 {
        self.resume_lsn
    }

    /// The tables the snapshot delivered rows for.
    pub fn tables(&self) -> &BTreeSet<String> {
        &self.tables
    }

    /// The transactions whose work the snapshot's rows already contain.
    pub fn txns(&self) -> &TxnSnapshot {
        &self.txns
    }

    /// **Whether the stream must drop this event because the snapshot already delivered it.**
    ///
    /// Both halves, and they fail in opposite directions: the table must be one this snapshot
    /// covered, and the transaction must be one it contained.
    pub fn suppresses(&self, table: &str, txn_id: u64) -> bool {
        self.tables.contains(table) && self.txns.already_delivered(txn_id)
    }
}

/// A snapshot whose handoff to the stream is exact: no change missed, none delivered twice.
///
/// Not merged into [`Snapshot`] on purpose. The two carry different promises, and a caller holding
/// one should not be able to read it as the other by looking at a field name.
pub struct ExactSnapshot {
    pub table: String,
    /// Rows written out.
    pub rows: usize,
    /// **What the stream needs to join this snapshot.** Hand it whole to
    /// [`FeedStreamer::resuming_after_snapshot`](super::stream::FeedStreamer::resuming_after_snapshot);
    /// see [`SnapshotBoundary`] for why its parts are not separately safe.
    pub boundary: SnapshotBoundary,
    /// A claim on the log at the resume point, held so a checkpoint cannot discard the records the
    /// stream is about to ask for.
    ///
    /// Taken here rather than left to the caller because the window between "snapshot returns" and
    /// "consumer subscribes" is exactly where a checkpoint would land — the same failure a base
    /// backup hit before it started pinning, where the copy was refused with "below the log's base"
    /// before applying a single record. Dropping this releases the claim, so keep it until the
    /// [`Subscription`](super::stream::Subscription) that replaces it exists.
    pub pin: WalPin,
    /// The reading transaction's id. Reported so a caller can tell one cutover from another in a
    /// log; nothing downstream needs it.
    pub reader_txn_id: u64,
}

impl ExactSnapshot {
    /// Where the stream resumes. Shorthand for `self.boundary.resume_lsn()`.
    pub fn resume_lsn(&self) -> u64 {
        self.boundary.resume_lsn()
    }
}

impl std::fmt::Debug for ExactSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExactSnapshot")
            .field("table", &self.table)
            .field("rows", &self.rows)
            .field("boundary", &self.boundary)
            .field("reader_txn_id", &self.reader_txn_id)
            .finish()
    }
}

/// Take a consistent snapshot of one table and report a handoff the stream can join **exactly**.
///
/// `read_rows` is handed the id of a transaction that has already been opened, and must perform its
/// read under it — that is what ties the rows to the transaction set this returns. It must not
/// commit or roll that transaction back, and must not write through it; a reader that writes is
/// rolled back and the call fails, because its own writes would be invisible to its own snapshot.
///
/// The read is *not* bracketed by LSNs here. Bracketing is what the at-least-once path does because
/// it cannot see inside the read; this one knows the snapshot exactly, so the log's position while
/// the scan runs is no longer part of the answer.
pub fn snapshot_table_exact<W, F>(
    table: &str,
    txn: &TxnManager,
    w: &mut W,
    read_rows: F,
) -> Result<ExactSnapshot, FerroError>
where
    W: Write,
    F: FnOnce(u64) -> Result<(Vec<String>, Vec<Vec<Value>>), FerroError>,
{
    // The handoff arrives already pinned. It used to be pinned here instead, with a `?` between
    // `begin_snapshot_read` and `end_read_only` - and that `?` leaked the reader on every failure,
    // which blocks every subsequent checkpoint for the life of the process. **There is now no
    // fallible call at all between opening the reader and closing it**, which is a stronger
    // guarantee than remembering to clean up on each error path, because there is no error path.
    let handoff = txn.begin_snapshot_read()?;

    let read = read_rows(handoff.txn_id);

    // Closed whatever the read did. A snapshot reader left open blocks every checkpoint for the
    // life of the process, so an error path that skipped this would turn one failed snapshot into a
    // WAL that never shrinks again.
    let closed = txn.end_read_only(handoff.txn_id);

    // **Both failures are reported, and the close's is the one that used to vanish.** Writing this
    // as `read?; closed?;` returns on the scan's error and drops `closed` unread - so a scan that
    // failed *because* the closure wrote through the reader would report only the scan, while the
    // far more informative "was opened to take a snapshot and then wrote to the database; it has
    // been rolled back" was discarded. That is the case where the two are most likely to fail
    // together and where the second explains the first.
    let (columns, rows) = match (read, closed) {
        (Err(read_err), Err(close_err)) => {
            return Err(FerroError::Txn(format!(
                "the snapshot scan failed ({read_err}) and closing its reader also failed \
                 ({close_err}); the reader is no longer active either way"
            )));
        }
        (Err(read_err), Ok(())) => return Err(read_err),
        (Ok(_), Err(close_err)) => return Err(close_err),
        (Ok(read_ok), Ok(())) => read_ok,
    };

    let columns = Arc::new(columns);
    let events: Vec<ChangeEvent> = rows
        .into_iter()
        .map(|values| ChangeEvent {
            txn_id: handoff.txn_id,
            lsn: handoff.resume_lsn,
            commit_lsn: handoff.resume_lsn,
            commit_end_lsn: handoff.resume_lsn,
            table: table.to_string(),
            columns: Arc::clone(&columns),
            // `READ`, for the same reason as above: a row that already existed is not news of a
            // change, and a consumer counting inserts must not count the size of the table.
            op: ChangeOp::Read { row: values },
        })
        .collect();

    let n = write_feed(&events, w)?;

    Ok(ExactSnapshot {
        table: table.to_string(),
        rows: n,
        // The table set is exactly the one table this call read — built from what the snapshot did
        // rather than from what a caller meant. Naming a table it did not deliver would suppress
        // that table's whole pre-cutover history in the stream, silently.
        boundary: SnapshotBoundary::new(
            BTreeSet::from([table.to_string()]),
            handoff.snapshot,
            handoff.resume_lsn,
        ),
        pin: handoff.pin,
        reader_txn_id: handoff.txn_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wal(tag: &str) -> (tempfile::TempDir, Arc<WalManager>) {
        let d = tempfile::tempdir().unwrap();
        let w = Arc::new(WalManager::new(d.path().join(format!("{tag}.wal"))).unwrap());
        (d, w)
    }

    fn rows() -> (Vec<String>, Vec<Vec<Value>>) {
        (
            vec!["id".into(), "qty".into()],
            vec![
                vec![Value::Integer(1), Value::Integer(10)],
                vec![Value::Integer(2), Value::Integer(20)],
            ],
        )
    }

    #[test]
    fn a_snapshot_writes_read_events_and_reports_its_handoff() {
        let (_d, w) = wal("basic");
        let mut buf = Vec::new();
        let snap = snapshot_table("inventory", &w, &mut buf, || Ok(rows())).unwrap();

        assert_eq!(snap.rows, 2);
        assert_eq!(snap.table, "inventory");
        let text = String::from_utf8(buf).unwrap();
        assert_eq!(text.lines().count(), 2, "{text}");
        assert!(text.contains("\"op\":\"READ\""), "snapshot rows must be READ, not INSERT: {text}");
        assert!(text.contains("\"after\":{\"id\":1,\"qty\":10}"), "{text}");
        assert!(text.contains("\"before\":null"), "a READ has no before image: {text}");
    }

    /// **The direction that matters.** The handoff must be the LSN from *before* the scan, so a
    /// change that raced the scan is re-delivered rather than skipped.
    #[test]
    fn the_handoff_lsn_is_taken_before_the_scan_not_after() {
        use crate::wal::log::RecKind;
        let (_d, w) = wal("racing");
        let mut buf = Vec::new();

        // A write lands *during* the scan, exactly as it would on a live database.
        let snap = snapshot_table("t", &w, &mut buf, || {
            w.append(1, 0, &RecKind::Begin).unwrap();
            w.append(1, 0, &RecKind::Commit).unwrap();
            w.flush().unwrap();
            Ok(rows())
        })
        .unwrap();

        assert!(
            snap.concurrent_writes,
            "no write landed during the scan, so this test never exercised the race"
        );
        assert!(
            snap.lsn < snap.lsn_after,
            "the handoff {} is not before the post-scan frontier {}",
            snap.lsn,
            snap.lsn_after
        );
        assert!(
            snap.overlap_bytes() > 0,
            "the overlap is zero despite a concurrent write; the handoff was taken too late and \
             those changes would be skipped"
        );
    }

    /// A quiet database gives an exact handoff — nothing will be re-delivered. Without this, the
    /// test above would pass just as well against a handoff that is always needlessly early.
    #[test]
    fn a_quiet_database_hands_off_exactly() {
        let (_d, w) = wal("quiet");
        let mut buf = Vec::new();
        let snap = snapshot_table("t", &w, &mut buf, || Ok(rows())).unwrap();

        assert!(!snap.concurrent_writes, "nothing wrote, yet concurrent writes were reported");
        assert_eq!(snap.lsn, snap.lsn_after);
        assert_eq!(snap.overlap_bytes(), 0, "a quiet snapshot should re-deliver nothing");
    }

    /// An empty table must produce a snapshot with zero rows and a usable handoff, not an error.
    /// A consumer starting against an empty table still needs to know where to begin streaming.
    #[test]
    fn an_empty_table_still_yields_a_handoff_point() {
        let (_d, w) = wal("empty");
        let mut buf = Vec::new();
        let snap =
            snapshot_table("t", &w, &mut buf, || Ok((vec!["id".into()], Vec::new()))).unwrap();

        assert_eq!(snap.rows, 0);
        assert!(buf.is_empty(), "an empty table wrote lines: {:?}", String::from_utf8_lossy(&buf));
        assert!(snap.lsn > 0, "the handoff point is unusable");
    }

    fn engine(tag: &str) -> (tempfile::TempDir, std::sync::Arc<crate::wal::txn::TxnManager>) {
        use crate::buffer::buffer_pool::BufferPoolManager;
        use crate::storage::disk_manager::DiskManager;
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join(format!("{tag}.db")))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
        let txn = Arc::new(crate::wal::txn::TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        (dir, txn)
    }

    /// The exact path writes the same rows and reports a resume point that is durable and pinned.
    #[test]
    fn an_exact_snapshot_writes_its_rows_and_pins_its_resume_point() {
        use std::sync::atomic::Ordering;
        let (_d, txn) = engine("exact_basic");
        let mut buf = Vec::new();
        let snap =
            snapshot_table_exact("inventory", &txn, &mut buf, |_reader| Ok(rows())).unwrap();

        assert_eq!(snap.rows, 2);
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("\"op\":\"READ\""), "{text}");
        assert!(
            txn.wal.flushed_lsn.load(Ordering::SeqCst) >= snap.resume_lsn(),
            "the resume point is not durable"
        );
        assert_eq!(snap.pin.lsn(), snap.resume_lsn(), "the pin is not on the resume point");
        assert_eq!(
            txn.wal.min_pinned_lsn(),
            Some(snap.resume_lsn()),
            "the log was not claimed at the resume point, so a checkpoint could discard it"
        );
        // The reader is closed: it must not go on blocking checkpoints.
        assert!(txn.att.lock().unwrap().is_empty(), "the snapshot reader was left open");
        txn.checkpoint().expect("a closed reader should not block a checkpoint");
    }

    /// A failed scan must still close the reader. A transaction left open blocks every checkpoint
    /// for the life of the process, which turns one failed snapshot into a WAL that never shrinks.
    #[test]
    fn a_failed_exact_scan_closes_its_reader_and_yields_no_handoff() {
        let (_d, txn) = engine("exact_fail");
        let mut buf = Vec::new();
        let r = snapshot_table_exact("t", &txn, &mut buf, |_reader| {
            Err(FerroError::Io("table vanished".into()))
        });
        assert!(r.is_err(), "a failed scan produced a snapshot");
        assert!(buf.is_empty(), "a failed scan wrote rows");
        assert!(
            txn.att.lock().unwrap().is_empty(),
            "the reader was left open after a failed scan"
        );
        txn.checkpoint().expect("a leaked reader is blocking the checkpoint");
    }

    /// **When the scan and the close both fail, neither error is swallowed.**
    ///
    /// This is the case the old `read?; closed?;` ordering lost, and it is the case where the
    /// second error explains the first: a closure that writes through the snapshot reader makes the
    /// scan fail *and* makes `end_read_only` refuse and roll the transaction back. Reporting only
    /// the scan tells the caller its query broke; reporting the close tells it why.
    #[test]
    fn a_scan_that_fails_after_writing_reports_the_rollback_too() {
        use crate::storage::heap_file_manager::HeapFileManager;
        use crate::storage::tuple::Tuple;

        let (_d, txn) = engine("exact_fail_both");
        let bp = txn.bp.clone();
        let mut buf = Vec::new();

        let r = snapshot_table_exact("t", &txn, &mut buf, |reader| {
            // Writing through the reader is what makes the close refuse.
            let mut heap = HeapFileManager::new(bp.clone()).unwrap();
            heap.set_transaction(txn.clone(), reader);
            heap.insert(Tuple::new(vec![1, 2, 3])).unwrap();
            Err(FerroError::Io("table vanished".into()))
        });

        let msg = format!("{}", r.expect_err("a failed scan produced a snapshot"));
        assert!(msg.contains("table vanished"), "the scan's error was dropped: {msg}");
        assert!(
            msg.contains("rolled back"),
            "the close's error was dropped, so a reader that wrote is reported as a broken query \
             rather than as the refused snapshot it is: {msg}"
        );

        // And the reader is gone either way, which is what makes reporting safe rather than a
        // choice between diagnosing and cleaning up.
        assert!(txn.att.lock().unwrap().is_empty(), "the reader was left open");
        txn.checkpoint().expect("a leaked reader is blocking the checkpoint");
    }

    /// **The same, for a failure that happens before the scan is even reached.**
    ///
    /// The test above covers a scan that fails; this one covers the setup failing underneath it.
    /// They were different code paths: this call used to pin the resume point *itself*, on a line
    /// between `begin_snapshot_read` and `end_read_only`, and a `?` there returned with the reader
    /// still in `att` and its id known to nobody. `checkpoint` refuses while any transaction is
    /// active, so the database would never checkpoint again — every later CREATE TABLE, CREATE
    /// INDEX and CLI shutdown failing with "checkpoint with active txns" from then on.
    ///
    /// There is no such line any more: the handoff arrives pinned, so nothing fallible sits between
    /// opening the reader and closing it. This test pins that structural claim in place by driving
    /// the failure that used to trigger it.
    #[test]
    fn a_snapshot_whose_resume_point_cannot_be_pinned_leaves_no_reader_behind() {
        use std::sync::atomic::Ordering;
        let (_d, txn) = engine("exact_pin_fail");

        // An open transaction whose `Begin` becomes the resume point, then the log truncated out
        // from under it. `checkpoint` would refuse to do this — guarding `att` is its job — so the
        // WAL is told directly. Fault injection, deliberately: the path is rare and used to be
        // unrecoverable.
        let open = txn.begin().unwrap();
        txn.wal.truncate(txn.next_txn_id.load(Ordering::SeqCst)).unwrap();

        let mut buf = Vec::new();
        let err = snapshot_table_exact("t", &txn, &mut buf, |_reader| Ok(rows()))
            .expect_err("a snapshot was handed back over a resume point the log no longer holds");
        assert!(format!("{err}").contains("truncated"), "wrong reason: {err}");
        assert!(buf.is_empty(), "a snapshot that never read anything wrote rows");

        // Only the transaction this test opened is still active; the reader did not survive.
        let live: Vec<u64> = txn.att.lock().unwrap().keys().copied().collect();
        assert_eq!(live, vec![open], "the failed snapshot left its reader open");

        // Committed rather than aborted only because an abort walks an undo chain this test just
        // truncated away.
        txn.commit(open).unwrap();
        txn.checkpoint().expect("a leaked reader is blocking every checkpoint");
    }

    /// A failing read must not produce a handoff. Returning one would tell the consumer to start
    /// streaming from a point whose prior state it never received.
    #[test]
    fn a_failed_scan_yields_no_handoff() {
        let (_d, w) = wal("fail");
        let mut buf = Vec::new();
        let r = snapshot_table("t", &w, &mut buf, || {
            Err(FerroError::Io("table vanished".into()))
        });
        assert!(r.is_err(), "a failed scan produced a snapshot");
        assert!(buf.is_empty(), "a failed scan wrote rows");
    }
}
