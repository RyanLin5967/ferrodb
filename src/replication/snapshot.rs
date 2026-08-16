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

use std::io::Write;
use std::sync::Arc;

use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::wal::log::WalManager;

use super::jsonl::write_feed;
use super::logical::{ChangeEvent, ChangeOp};

/// What a snapshot captured, and where the stream must resume.
#[derive(Debug, Clone, PartialEq)]
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
    wal: &WalManager,
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wal(tag: &str) -> (tempfile::TempDir, WalManager) {
        let d = tempfile::tempdir().unwrap();
        let w = WalManager::new(d.path().join(format!("{tag}.wal"))).unwrap();
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
