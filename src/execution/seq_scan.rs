use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::wal::txn::ReadView;
use crate::{error::FerroError, execution::executor::Executor, storage::heap_scanner::HeapScanner, wal::visibility::resolve_visibility};
use crate::storage::heap_file_manager::{HeapFileManager, RecordId};
use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
/// **Tuples this executor pulled off the heap — D176.** Not rows returned: every tuple the
/// scanner yielded, including the ones `resolve_visibility` then discarded.
///
/// # Why it exists separately from `agent_sql::runtime::SCAN_TABLE_ROWS`
///
/// That counter records rows a scan RETURNED. When a predicate is pushed into the planner, a
/// point lookup returns one row — and reports 1 whether the planner picked an index or fell back
/// to dragging the whole table through this executor. So a merge could look O(delta) in that
/// counter while being O(table) in fact. This is the counter that tells the two apart, and the
/// pair is only meaningful read together.
///
/// # ⚠ ONE atomic add per SCAN, not per tuple
///
/// The running total lives in a plain `u64` field and is flushed once, in `Drop`. A per-tuple
/// `fetch_add` on a shared cache line is the one shape that could create the slope it measures —
/// the same cardinal rule `OURS_SCAN_EXAMINED` states at `agent_sql/runtime.rs:115`. `SEQ_SCANS`
/// counts the flushes, so a scan that yielded nothing is still visible as a scan that happened.
///
/// A `SeqScan` that is leaked rather than dropped never flushes; nothing in the read path leaks
/// one, and a missing flush shows as a `SEQ_SCANS` count below the number of scans issued rather
/// than as a silently low tuple count.
pub static SEQ_SCANS: AtomicU64 = AtomicU64::new(0);
pub static SEQ_SCAN_TUPLES: AtomicU64 = AtomicU64::new(0);

/// `(scans, tuples_pulled)` since process start. Read twice and subtract to scope it to a phase.
pub fn seq_scan_counters() -> (u64, u64) {
    (SEQ_SCANS.load(Ordering::Relaxed), SEQ_SCAN_TUPLES.load(Ordering::Relaxed))
}

pub struct SeqScan {
    pub scanner: HeapScanner,
    pub schema: Schema,
    pub view: Arc<ReadView>,
    pub tt_heap: HeapFileManager,
    /// D176 — tuples pulled, accumulated locally and flushed once in `Drop`. Not `pub`: a
    /// caller that could set it could forge the measurement.
    pulled: u64,
}

impl SeqScan {
    /// The only way to build one, so `pulled` cannot start at anything but zero.
    pub fn new(
        scanner: HeapScanner,
        schema: Schema,
        view: Arc<ReadView>,
        tt_heap: HeapFileManager,
    ) -> Self {
        Self { scanner, schema, view, tt_heap, pulled: 0 }
    }
}

impl Drop for SeqScan {
    fn drop(&mut self) {
        // D176 — the single flush. See `SEQ_SCAN_TUPLES`.
        SEQ_SCANS.fetch_add(1, Ordering::Relaxed);
        SEQ_SCAN_TUPLES.fetch_add(self.pulled, Ordering::Relaxed);
    }
}

impl Executor for SeqScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (rid, tuple) = match self.scanner.next()? {
                Ok((r, t)) => (r, t),
                Err(e) => return Some(Err(e))
            };
            // Counted where the heap yielded it, BEFORE visibility filtering — the engine paid
            // for this tuple whether or not the caller ever sees it. Plain field, no atomic.
            self.pulled += 1;
            let vt = match resolve_visibility(&self.view, &self.tt_heap, tuple) {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => return Some(Err(e))
            };
            return Some(vt.deserialize(&self.schema).map(|vals| (rid, vals)));
        }
    }
}