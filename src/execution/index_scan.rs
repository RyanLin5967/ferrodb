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
/// The per-entry total lives in [`INDEX_SCAN_ENTRIES`], accumulated in a plain field and flushed
/// beside this one.
///
/// **D179 — `SecondaryIndexScan` flushes into this too, and did not before.** Until D179 only the
/// primary-tree `IndexScan` had a `Drop`, so a statement served entirely by a secondary index
/// reported ZERO index scans: the shape a statement that reached no index at all reports. Nothing
/// depended on the gap while `sec > v` could not use the index; D179 makes secondary range scans
/// reachable from the optimizer, so leaving it would have made the counter read "no index" for
/// exactly the plans D179 adds.
pub static INDEX_SCANS: AtomicU64 = AtomicU64::new(0);

/// **Index entries the scan walked — D181's rows-examined instrument.**
///
/// Every `(key, value)` the tree's `RangeScanner` yielded, counted where the scanner yielded it and
/// BEFORE visibility filtering, the primary lookup, or the `sec == v` skip — the engine paid for
/// the entry whether or not the caller ever sees the row. Terminating entries count: the one whose
/// key is past `sec_upper` was read to learn that.
///
/// This is the number `INDEX_SCANS` alone cannot give. Two plans that each run exactly one index
/// scan report `1` apiece however much of the tree they walk, so a choice BETWEEN two indexes is
/// invisible in the scan count and plain in this one. `SEQ_SCAN_TUPLES` is its sequential-side
/// twin; the pair is what makes "rows examined" answerable for any access path.
///
/// ⚠ Same rule as `SEQ_SCAN_TUPLES`: one relaxed add per scan in `Drop`, never per entry. A
/// per-entry `fetch_add` on a shared line is the one shape that could manufacture the slope it is
/// there to measure.
pub static INDEX_SCAN_ENTRIES: AtomicU64 = AtomicU64::new(0);

/// Index scans since process start. Read twice and subtract to scope it to a phase.
pub fn index_scan_counter() -> u64 {
    INDEX_SCANS.load(Ordering::Relaxed)
}

/// `(scans, entries_examined)` since process start — the pair, read together. Read twice and
/// subtract to scope it to a phase, exactly as `seq_scan_counters` is used.
pub fn index_scan_counters() -> (u64, u64) {
    (INDEX_SCANS.load(Ordering::Relaxed), INDEX_SCAN_ENTRIES.load(Ordering::Relaxed))
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
    /// D181 — entries pulled, accumulated locally and flushed once in `Drop`. Not `pub`: a caller
    /// that could set it could forge the measurement. See `SeqScan::pulled`.
    examined: u64,
}

impl IndexScan {
    /// The only way to build one, so `examined` cannot start at anything but zero.
    ///
    /// `skip_nulls` is a PARAMETER, not a field set afterwards: this doc says `new` is the
    /// only way to build one, and assigning the flag post-construction would weaken exactly
    /// that invariant for the thing deciding whether NULL rows are yielded.
    pub fn new(
        heap: HeapFileManager,
        scanner: RangeScanner<Value, RecordId>,
        schema: Schema,
        view: Arc<ReadView>,
        tt_heap: HeapFileManager,
        skip_nulls: bool,
    ) -> Self {
        Self { heap, scanner, schema, view, tt_heap, skip_nulls, examined: 0 }
    }
}

impl Drop for IndexScan {
    fn drop(&mut self) {
        INDEX_SCANS.fetch_add(1, Ordering::Relaxed);
        INDEX_SCAN_ENTRIES.fetch_add(self.examined, Ordering::Relaxed);
    }
}

impl Executor for IndexScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (key, rid) = match self.scanner.next()? {
                Ok((k ,v)) => (k,v),
                Err(e) => return Some(Err(e))
            };
            // Counted where the tree yielded it, before anything can discard it. Plain field.
            //
            // ⛔ THIS INCREMENT MUST STAY ABOVE THE NULL SKIP BELOW — long form of this note in
            // `sec_index_scan.rs::next`. Below the skip it under-counts by the NULLs dropped and
            // reads as a performance improvement rather than a measurement change, with no test
            // failing. D181 and D187 both insert here and neither is wrong alone.
            self.examined += 1;
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