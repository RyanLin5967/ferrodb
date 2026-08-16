//! Capture: how reads, writes and attribution get *retained* for one transaction, and the
//! retained log they accumulate into.
//!
//! Design authority: DESIGN.md section 2.
//!
//! Retention is the whole feature. Everything downstream — causal rollback (criterion 10), the
//! `write-set \ read-set` blind-write metric, the provenance answer (criterion 9) — exists only
//! because the read-set was kept instead of discarded at commit.
//!
//! The form of a retained read is chosen by [`AccessShape`] and by nothing else. There is no
//! method here that turns exact versions into an interval, whatever the count, because for `k`
//! scattered reads over `N` rows the enclosing interval covers `N(k-1)/(k+1)` rows — at `k = 3`
//! that is already half the table.

use std::sync::{Arc, Mutex};

use crate::branch::types::BranchId;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::execution::executor::Executor;
use crate::provenance::readset::{
    blind_writes, AccessShape, PredicateSummary, ReadSet, ReadSetBuilder, VersionRef,
};
use crate::provenance::revert::{DependencyGraph, DependencyGraphBuilder, RevertMode, RevertPlan};
use crate::provenance::{ProvId, ProvenanceStore};
use crate::storage::heap_file_manager::{HeapFileManager, RecordId};
use crate::storage::tuple::Tuple;
use crate::tel::ids::{ColId, RowId, TableId, TxnId};

/// A predicate read together with *when* it happened.
///
/// The timestamp is not in [`PredicateSummary`] on purpose: the summary describes a region of the
/// key space and is reusable, while "what had been written by the time I looked" is a property of
/// the access. Causal edges out of a predicate read need both — coverage alone would make a read
/// depend on writes that happened after it.
#[derive(Debug, Clone, PartialEq)]
pub struct TimedPredicate {
    pub summary: PredicateSummary,
    /// The snapshot high water mark the scan read at. A write with `begin_ts < observed_at` was
    /// visible to it — strictly less, the same rule `ReadView::is_commited_for_me` applies.
    pub observed_at: u64,
}

/// One version this transaction produced, with the value it wrote.
///
/// The value is what lets a predicate read-set produce a causal edge: "did the thing I wrote fall
/// inside the region you scanned?" is answerable only against values.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteRecord {
    pub version: VersionRef,
    pub col: Option<ColId>,
    pub value: Option<Value>,
}

impl WriteRecord {
    pub fn new(version: VersionRef, col: Option<ColId>, value: Option<Value>) -> Self {
        WriteRecord { version, col, value }
    }
}

/// Accumulates one transaction's retained reads and writes.
#[derive(Debug, Clone)]
pub struct TxnCapture {
    txn: TxnId,
    prov: ProvId,
    branch: BranchId,
    reads: ReadSetBuilder,
    predicates: Vec<TimedPredicate>,
    writes: Vec<WriteRecord>,
}

impl TxnCapture {
    pub fn new(txn: TxnId, prov: ProvId, branch: BranchId) -> Self {
        TxnCapture {
            txn,
            prov,
            branch,
            reads: ReadSetBuilder::new(),
            predicates: Vec::new(),
            writes: Vec::new(),
        }
    }

    pub fn txn(&self) -> TxnId {
        self.txn
    }

    pub fn prov(&self) -> ProvId {
        self.prov
    }

    /// Record an access. `shape` alone decides what is retained.
    ///
    /// `observed_at` is the snapshot the access read at; it is kept only for predicate forms,
    /// where exact version identity is unavailable.
    pub fn on_read(
        &mut self,
        shape: AccessShape,
        versions: Vec<VersionRef>,
        summary: Option<PredicateSummary>,
        observed_at: u64,
    ) {
        if let (crate::provenance::readset::ReadSetForm::Predicate, Some(p)) =
            (shape.form(), summary.clone())
        {
            self.predicates.push(TimedPredicate { summary: p, observed_at });
        }
        self.reads.observe(shape, versions, summary);
    }

    /// Convenience for a single point read.
    pub fn on_point_read(&mut self, v: VersionRef) {
        self.on_read(AccessShape::Point, vec![v], None, 0);
    }

    pub fn on_write(&mut self, w: WriteRecord) {
        self.writes.push(w);
    }

    /// Record a write and stamp its version with this transaction's run, in one step, so the two
    /// halves of attribution cannot drift apart.
    pub fn on_write_stamped(
        &mut self,
        store: &dyn ProvenanceStore,
        w: WriteRecord,
    ) -> Result<(), FerroError> {
        store.stamp(w.version.rid, self.prov)?;
        self.writes.push(w);
        Ok(())
    }

    pub fn writes(&self) -> &[WriteRecord] {
        &self.writes
    }

    pub fn finish(self) -> TxnProvenance {
        TxnProvenance {
            txn: self.txn,
            prov: self.prov,
            branch: self.branch,
            read_sets: self.reads.finish(),
            predicate_reads: self.predicates,
            writes: self.writes,
        }
    }
}

/// What one committed transaction retained.
#[derive(Debug, Clone, PartialEq)]
pub struct TxnProvenance {
    pub txn: TxnId,
    pub prov: ProvId,
    pub branch: BranchId,
    /// Canonical retention, in the form the access shape demanded.
    pub read_sets: Vec<ReadSet>,
    /// The predicate-form reads again, carrying the snapshot they read at. Same summaries as the
    /// `ReadSet::Predicate` entries above; kept alongside because a `ReadSet` deliberately has no
    /// place to hang a timestamp.
    pub predicate_reads: Vec<TimedPredicate>,
    pub writes: Vec<WriteRecord>,
}

impl TxnProvenance {
    /// Rows this transaction changed without ever looking at them (DESIGN.md section 4).
    pub fn blind_writes(&self) -> Vec<(TableId, RowId, Option<ColId>)> {
        let ws: Vec<(TableId, RowId, Option<ColId>)> = self
            .writes
            .iter()
            .map(|w| (w.version.tbl, w.version.row, w.col))
            .collect();
        blind_writes(&ws, &self.read_sets)
    }
}

/// The retained provenance of every committed transaction: the substrate causal rollback runs on.
#[derive(Debug, Clone, Default)]
pub struct ProvenanceLog {
    txns: Vec<TxnProvenance>,
}

impl ProvenanceLog {
    pub fn new() -> Self {
        ProvenanceLog::default()
    }

    pub fn record(&mut self, t: TxnProvenance) {
        self.txns.push(t);
    }

    pub fn get(&self, txn: TxnId) -> Option<&TxnProvenance> {
        self.txns.iter().find(|t| t.txn == txn)
    }

    pub fn txns(&self) -> &[TxnProvenance] {
        &self.txns
    }

    /// Read-after-write edges across everything retained, both exact and predicate-derived.
    pub fn dependency_graph(&self) -> DependencyGraph {
        let mut b = DependencyGraphBuilder::new();
        for t in &self.txns {
            for w in &t.writes {
                b.record_write_value(t.txn, w.version, w.col, w.value.clone());
            }
            b.record_read_sets(t.txn, &t.read_sets);
            for p in &t.predicate_reads {
                b.record_predicate_read(t.txn, p.summary.clone(), p.observed_at);
            }
        }
        b.build()
    }

    /// Exit criterion 10. Halt is the default: the plan names the downstream work and reverts
    /// nothing.
    pub fn plan_revert(&self, target: TxnId, mode: RevertMode) -> RevertPlan {
        self.dependency_graph().plan_revert(target, mode)
    }

    /// The exact versions a plan says to undo, deepest dependent first and the target last.
    ///
    /// This is what the write path consumes: a plan that named only transactions would leave the
    /// undo to guess which slots they touched. Empty for a halted plan, because a halt reverts
    /// nothing — that is the safety property, expressed as data rather than a comment.
    pub fn versions_to_undo(&self, plan: &RevertPlan) -> Vec<(TxnId, VersionRef)> {
        if plan.is_blocked() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for txn in plan.cascade.iter().chain(std::iter::once(&plan.target)) {
            if let Some(t) = self.get(*txn) {
                for w in &t.writes {
                    out.push((*txn, w.version));
                }
            }
        }
        out
    }

    /// The tree a halted revert shows the caller, annotated with who each transaction was.
    pub fn revert_report(
        &self,
        target: TxnId,
        mode: RevertMode,
        store: &dyn ProvenanceStore,
    ) -> String {
        let g = self.dependency_graph();
        let plan = g.plan_revert(target, mode);
        let mut out = String::new();
        if plan.is_blocked() {
            out.push_str(&format!(
                "REVERT {} HALTED: {} downstream transaction(s) consumed its writes.\n",
                target,
                plan.blocked_by.len()
            ));
        } else if !plan.cascade.is_empty() {
            out.push_str(&format!(
                "REVERT {} CASCADE: undoing {} downstream transaction(s) first.\n",
                target,
                plan.cascade.len()
            ));
        } else {
            out.push_str(&format!("REVERT {}: nothing downstream.\n", target));
        }
        out.push_str(&g.render_tree(target));
        for t in plan.blocked_by.iter().chain(plan.cascade.iter()) {
            if let Some(tp) = self.get(*t) {
                if let Ok(run) = store.lookup(tp.prov) {
                    out.push_str(&format!("  {} -> {}\n", t, run.describe()));
                }
            }
        }
        out
    }
}

/// Resolves the version identity of a physical slot: the `begin_ts` from the 24-byte version
/// header. Implemented for the real heap so capture works against actual storage, not a mock.
pub trait VersionSource: Send + Sync {
    fn begin_ts(&self, rid: RecordId) -> Result<u64, FerroError>;
}

impl VersionSource for HeapFileManager {
    fn begin_ts(&self, rid: RecordId) -> Result<u64, FerroError> {
        let t: Tuple = self.read(rid)?;
        Ok(t.version_header()?.begin_ts)
    }
}

/// Resolves the *immutable surrogate* row identity of a scanned row.
///
/// This is deliberately a policy the caller supplies rather than something derived from
/// `RecordId`: `RecordId` names a physical slot and moves whenever a version is written, so using
/// it as identity would silently break the one property [`RowId`] exists to provide.
pub trait RowIdSource: Send + Sync {
    fn row_id(&self, rid: RecordId, values: &[Value]) -> Result<RowId, FerroError>;
}

/// The surrogate lives in an integer column of the row.
pub struct SurrogateColumn(pub usize);

impl RowIdSource for SurrogateColumn {
    fn row_id(&self, _rid: RecordId, values: &[Value]) -> Result<RowId, FerroError> {
        match values.get(self.0) {
            Some(Value::Integer(i)) => Ok(RowId(*i as u64)),
            Some(other) => Err(FerroError::Provenance(format!(
                "surrogate column {} is not an integer: {:?}",
                self.0, other
            ))),
            None => Err(FerroError::Provenance(format!(
                "surrogate column {} out of range",
                self.0
            ))),
        }
    }
}

/// A Volcano operator that retains what the operator underneath it read.
///
/// Wraps any [`Executor`], so it composes with the existing scan operators without changing them.
/// It records by access shape:
///
/// - `Point` / `IndexLookup` — one [`VersionRef`] per row, with the real `begin_ts` read from the
///   version header, giving exact causal edges;
/// - `Range` / `FullScan` — the predicate template supplied at construction, emitted once at
///   end-of-stream with the observed row count filled in.
///
/// It never converts between the two.
pub struct CapturingScan {
    inner: Box<dyn Executor>,
    capture: Arc<Mutex<TxnCapture>>,
    tbl: TableId,
    shape: AccessShape,
    rows: Arc<dyn RowIdSource>,
    versions: Arc<dyn VersionSource>,
    /// Bounds of the range being scanned; `rows_observed` is overwritten at end-of-stream.
    predicate: Option<PredicateSummary>,
    observed_at: u64,
    seen: u64,
    emitted: bool,
}

impl CapturingScan {
    pub fn new(
        inner: Box<dyn Executor>,
        capture: Arc<Mutex<TxnCapture>>,
        tbl: TableId,
        shape: AccessShape,
        rows: Arc<dyn RowIdSource>,
        versions: Arc<dyn VersionSource>,
        predicate: Option<PredicateSummary>,
        observed_at: u64,
    ) -> Self {
        CapturingScan {
            inner,
            capture,
            tbl,
            shape,
            rows,
            versions,
            predicate,
            observed_at,
            seen: 0,
            emitted: false,
        }
    }

    fn note_row(&mut self, rid: RecordId, values: &[Value]) -> Result<(), FerroError> {
        self.seen += 1;
        if self.shape.form() != crate::provenance::readset::ReadSetForm::ExactVersions {
            return Ok(());
        }
        let row = self.rows.row_id(rid, values)?;
        let begin_ts = self.versions.begin_ts(rid)?;
        let v = VersionRef { tbl: self.tbl, row, rid, begin_ts };
        let mut c = self
            .capture
            .lock()
            .map_err(|_| FerroError::Provenance("capture lock poisoned".into()))?;
        c.on_read(self.shape, vec![v], None, self.observed_at);
        Ok(())
    }

    fn emit_predicate(&mut self) -> Result<(), FerroError> {
        if self.emitted {
            return Ok(());
        }
        self.emitted = true;
        if self.shape.form() != crate::provenance::readset::ReadSetForm::Predicate {
            return Ok(());
        }
        let mut p = match self.predicate.clone() {
            Some(p) => p,
            // A range access with no retained predicate would be a silent hole in phantom
            // coverage, so it is refused rather than skipped.
            None => {
                return Err(FerroError::Provenance(format!(
                    "{:?} access retained no predicate summary",
                    self.shape
                )))
            }
        };
        p.rows_observed = self.seen;
        let mut c = self
            .capture
            .lock()
            .map_err(|_| FerroError::Provenance("capture lock poisoned".into()))?;
        c.on_read(self.shape, Vec::new(), Some(p), self.observed_at);
        Ok(())
    }
}

impl Executor for CapturingScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        match self.inner.next() {
            Some(Ok((rid, values))) => {
                if let Err(e) = self.note_row(rid, &values) {
                    return Some(Err(e));
                }
                Some(Ok((rid, values)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => {
                if let Err(e) = self.emit_predicate() {
                    return Some(Err(e));
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provenance::readset::{Bound, ReadSetForm};
    use crate::provenance::store::MemProvenanceStore;
    use crate::provenance::RunEntity;

    fn vref(row: u64, ts: u64) -> VersionRef {
        VersionRef {
            tbl: TableId(1),
            row: RowId(row),
            rid: RecordId { page_id: 1, slot_num: row as u16 },
            begin_ts: ts,
        }
    }

    fn store_with(agent: &str, run: &str) -> (MemProvenanceStore, ProvId) {
        let s = MemProvenanceStore::new();
        let id = s
            .intern(&RunEntity::new(
                ProvId::NONE,
                agent,
                run,
                "claude-opus",
                "2026-05",
                [1u8; 32],
                1,
                BranchId::new(1, 0),
            ))
            .unwrap();
        (s, id)
    }

    #[test]
    fn a_capture_retains_reads_in_the_form_the_shape_demands() {
        let mut c = TxnCapture::new(TxnId(1), ProvId(1), BranchId::TRUNK);
        c.on_point_read(vref(1, 10));
        c.on_read(
            AccessShape::Range,
            Vec::new(),
            Some(PredicateSummary {
                tbl: TableId(1),
                col: Some(ColId(0)),
                lo: Bound::Included(Value::Integer(1)),
                hi: Bound::Included(Value::Integer(9)),
                residual: None,
                rows_observed: 4,
            }),
            77,
        );
        let t = c.finish();
        assert_eq!(t.read_sets.len(), 2);
        assert_eq!(t.read_sets[0].form(), ReadSetForm::ExactVersions);
        assert_eq!(t.read_sets[1].form(), ReadSetForm::Predicate);
        assert_eq!(t.predicate_reads.len(), 1);
        assert_eq!(t.predicate_reads[0].observed_at, 77);
    }

    #[test]
    fn a_point_read_never_retains_a_predicate_even_if_one_is_offered() {
        let mut c = TxnCapture::new(TxnId(1), ProvId(1), BranchId::TRUNK);
        c.on_read(
            AccessShape::Point,
            vec![vref(1, 10)],
            Some(PredicateSummary::full_scan(TableId(1), 999)),
            5,
        );
        let t = c.finish();
        assert_eq!(t.read_sets.len(), 1);
        assert_eq!(t.read_sets[0].form(), ReadSetForm::ExactVersions);
        assert!(t.predicate_reads.is_empty());
    }

    #[test]
    fn stamping_on_write_ties_the_version_to_the_run() {
        let (s, id) = store_with("restock-agent", "run-42");
        let mut c = TxnCapture::new(TxnId(1), id, BranchId::TRUNK);
        c.on_write_stamped(&s, WriteRecord::new(vref(3, 20), Some(ColId(1)), Some(Value::Integer(7))))
            .unwrap();
        let who = s.who_wrote(RecordId { page_id: 1, slot_num: 3 }).unwrap();
        assert_eq!(who.agent_id, "restock-agent");
        assert_eq!(who.run_id, "run-42");
    }

    #[test]
    fn blind_writes_survive_the_round_trip_through_a_capture() {
        let mut c = TxnCapture::new(TxnId(1), ProvId(1), BranchId::TRUNK);
        c.on_point_read(vref(1, 10));
        c.on_write(WriteRecord::new(vref(1, 11), Some(ColId(0)), Some(Value::Integer(1))));
        c.on_write(WriteRecord::new(vref(2, 11), Some(ColId(0)), Some(Value::Integer(2))));
        let t = c.finish();
        assert_eq!(t.blind_writes(), vec![(TableId(1), RowId(2), Some(ColId(0)))]);
    }

    #[test]
    fn the_log_finds_a_downstream_dependent_and_halts() {
        // txn1 writes row1; txn2 point-reads row1 and writes row2.
        let mut a = TxnCapture::new(TxnId(1), ProvId(1), BranchId::TRUNK);
        a.on_write(WriteRecord::new(vref(1, 10), Some(ColId(1)), Some(Value::Integer(5))));
        let mut b = TxnCapture::new(TxnId(2), ProvId(2), BranchId::TRUNK);
        b.on_point_read(vref(1, 10));
        b.on_write(WriteRecord::new(vref(2, 20), Some(ColId(1)), Some(Value::Integer(50))));

        let mut log = ProvenanceLog::new();
        log.record(a.finish());
        log.record(b.finish());

        let plan = log.plan_revert(TxnId(1), RevertMode::Halt);
        assert!(plan.is_blocked());
        assert_eq!(plan.blocked_by, vec![TxnId(2)]);
        assert!(plan.cascade.is_empty());
    }

    #[test]
    fn a_halted_plan_names_no_version_to_undo_and_a_cascade_names_them_deepest_first() {
        let mut a = TxnCapture::new(TxnId(1), ProvId(1), BranchId::TRUNK);
        a.on_write(WriteRecord::new(vref(1, 10), Some(ColId(1)), Some(Value::Integer(5))));
        let mut b = TxnCapture::new(TxnId(2), ProvId(2), BranchId::TRUNK);
        b.on_point_read(vref(1, 10));
        b.on_write(WriteRecord::new(vref(2, 20), Some(ColId(1)), Some(Value::Integer(50))));

        let mut log = ProvenanceLog::new();
        log.record(a.finish());
        log.record(b.finish());

        let halted = log.plan_revert(TxnId(1), RevertMode::Halt);
        assert!(log.versions_to_undo(&halted).is_empty());

        let cascade = log.plan_revert(TxnId(1), RevertMode::Cascade);
        assert_eq!(
            log.versions_to_undo(&cascade),
            vec![(TxnId(2), vref(2, 20)), (TxnId(1), vref(1, 10))]
        );
    }

    #[test]
    fn the_report_names_the_agent_behind_each_blocking_transaction() {
        let (s, restock) = store_with("restock-agent", "run-42");
        let auditor = s
            .intern(&RunEntity::new(
                ProvId::NONE,
                "auditor-agent",
                "run-99",
                "claude-opus",
                "2026-05",
                [2u8; 32],
                2,
                BranchId::new(2, 0),
            ))
            .unwrap();

        let mut a = TxnCapture::new(TxnId(1), restock, BranchId::TRUNK);
        a.on_write(WriteRecord::new(vref(1, 10), Some(ColId(1)), Some(Value::Integer(5))));
        let mut b = TxnCapture::new(TxnId(2), auditor, BranchId::TRUNK);
        b.on_point_read(vref(1, 10));
        b.on_write(WriteRecord::new(vref(2, 20), Some(ColId(1)), Some(Value::Integer(50))));

        let mut log = ProvenanceLog::new();
        log.record(a.finish());
        log.record(b.finish());

        let report = log.revert_report(TxnId(1), RevertMode::Halt, &s);
        assert!(report.contains("HALTED"), "{}", report);
        assert!(report.contains("txn2"), "{}", report);
        assert!(report.contains("auditor-agent"), "{}", report);
    }
}
