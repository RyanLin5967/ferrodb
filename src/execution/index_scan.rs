use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::wal::visibility::resolve_visibility;
use crate::{catalog::schema::Schema, error::FerroError, execution::executor::Executor, storage::range_scan::RangeScanner, wal::txn::ReadView};
use crate::storage::heap_file_manager::HeapFileManager;
use crate::catalog::column::Value;
use crate::storage::heap_file_manager::RecordId;

/// **How many index scans ran — D176, and this counter exists purely for ATTRIBUTION.**
///
/// A merge window that pulls `delta * n` tuples through `SeqScan` has two candidate causes inside
/// one `MERGE;`, and the seq-scan counters alone cannot separate them:
///
///  1. `evaluate_merge`'s per-key point lookups (`runtime.rs:4421`), which push `pk = k` into the
///     planner and should pick an index;
///  2. the publish step, which turns each `PendingWrite` into an `UPDATE` statement via
///     `into_stmt` / `apply_dml_in` and runs it.
///
/// Both issue exactly `delta` statements, so both predict `delta` sequential scans. Counting the
/// INDEX side breaks the tie: if `delta` index scans ran alongside `delta` sequential ones, then
/// the point lookups DID use the index and the sequential scans belong to the publish path. Naming
/// the wrong one would send a fix to the wrong function.
///
/// ⚠ One relaxed add per SCAN in `Drop`, never per row — the rule at `agent_sql/runtime.rs:115`.
/// Only the scan COUNT is kept: rows are already covered by the counters this pairs with.
pub static INDEX_SCANS: AtomicU64 = AtomicU64::new(0);

/// Index scans since process start. Read twice and subtract to scope it to a phase.
pub fn index_scan_counter() -> u64 {
    INDEX_SCANS.load(Ordering::Relaxed)
}

pub struct IndexScan {
    pub heap: HeapFileManager,
    pub scanner: RangeScanner<Value, RecordId>,
    pub schema: Schema,
    pub view: Arc<ReadView>,
    pub tt_heap: HeapFileManager,
    /// **D187 — drop an entry whose key is NULL, because this scan is bounded on at least one end.**
    ///
    /// A NULL primary key is insertable: nothing on the insert path refuses one. `type_rank` puts
    /// `Value::Null` below every other variant, so that key sorts at the very front of the tree —
    /// and an unbounded-below scan therefore starts ON it. `RangeScanner`'s upper-bound test is
    /// `&key > u`, and `Null > Integer(5)` is **false**, so the entry is never "past" the bound and
    /// the scan yields it. SQL says the opposite: `NULL < 5` is UNKNOWN and an UNKNOWN row is not
    /// in the answer.
    ///
    /// The test cannot live in `RangeScanner`, which is generic over `K: Ord` and cannot ask
    /// whether a key is NULL — `K` is `(Value, Value)` for the secondary tree. So it lives here,
    /// and `SecondaryIndexScan` carries the identical field for the identical reason.
    ///
    /// Why a flag rather than an unconditional skip: the rule is that a NULL's membership is
    /// decided by a COMPARISON AGAINST A BOUND, and that comparison is UNKNOWN. With no bound on
    /// either end there is no comparison, nothing to be UNKNOWN about, and the NULL rows belong in
    /// the answer. `predicate_to_bounds` cannot currently emit `(Unbounded, Unbounded)` — all five
    /// of its arms bound at least one side — so today this is always `true` for a planner-built
    /// scan; it is a flag so that adding a full index scan for ordering cannot silently start
    /// dropping rows. `tests/d187_null_index_scan.rs` exercises both values.
    pub skip_nulls: bool,
}

impl Drop for IndexScan {
    fn drop(&mut self) {
        INDEX_SCANS.fetch_add(1, Ordering::Relaxed);
    }
}

impl Executor for IndexScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (key, rid) = match self.scanner.next()? {
                Ok((k ,v)) => (k,v),
                Err(e) => return Some(Err(e))
            };
            // D187 — see `skip_nulls`. `continue`, never `return None`: NULL keys sort at the FRONT
            // of the tree, so stopping here would truncate the scan before it reached a single real
            // row rather than skipping one entry.
            if self.skip_nulls && matches!(key, Value::Null) {
                continue;
            }
            let tuple = match self.heap.read(rid) {
                Ok(t) => t,
                Err(e) => return Some(Err(e))
            };
            let vt = match resolve_visibility(&self.view, &self.tt_heap, tuple) {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => return Some(Err(e))
            };
            return Some(vt.deserialize(&self.schema).map(|vals| (rid, vals)));
        }
    }
}