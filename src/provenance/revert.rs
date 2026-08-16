//! Causal rollback over retained read-sets.
//!
//! Design authority: DESIGN.md section 2 ("Causal rollback") and exit criterion 10.
//!
//! Reverting write A must find the write B that *read* A, and B's dependents. **Halt by default**
//! and show the tree; cascade only on explicit request. Silently cascading a revert through an
//! agent's downstream work is not recoverable by the agent.

use crate::catalog::column::Value;
use crate::provenance::readset::{PredicateSummary, ReadSet, VersionRef};
use crate::tel::ids::{ColId, TxnId};

/// What to do when the target of a revert has dependents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum RevertMode {
    /// Stop, report the dependency tree, change nothing. The default, deliberately.
    #[default]
    Halt,
    /// Revert the target and everything transitively downstream of it.
    Cascade,
}

/// `from` wrote a version that `to` read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DependencyEdge {
    pub from: TxnId,
    pub to: TxnId,
    pub via: VersionRef,
}

/// Read-after-write edges between transactions.
#[derive(Debug, Clone, Default)]
pub struct DependencyGraph {
    pub edges: Vec<DependencyEdge>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        DependencyGraph::default()
    }

    pub fn add_edge(&mut self, edge: DependencyEdge) {
        self.edges.push(edge);
    }

    /// Transactions that directly read something this transaction wrote.
    pub fn dependents_of(&self, txn: TxnId) -> Vec<TxnId> {
        let mut out: Vec<TxnId> =
            self.edges.iter().filter(|e| e.from == txn).map(|e| e.to).collect();
        out.sort();
        out.dedup();
        out
    }

    /// Everything transitively downstream, excluding `txn` itself. Cycle-safe.
    pub fn transitive_dependents(&self, txn: TxnId) -> Vec<TxnId> {
        let mut seen: Vec<TxnId> = vec![txn];
        let mut queue: Vec<TxnId> = vec![txn];
        while let Some(cur) = queue.pop() {
            for d in self.dependents_of(cur) {
                if !seen.contains(&d) {
                    seen.push(d);
                    queue.push(d);
                }
            }
        }
        let mut out: Vec<TxnId> = seen.into_iter().filter(|t| *t != txn).collect();
        out.sort();
        out.dedup();
        out
    }

    /// Build the plan for reverting `target`.
    ///
    /// Under `Halt` the dependents land in `blocked_by` and `cascade` stays empty: the caller is
    /// shown the tree and nothing is reverted. Under `Cascade` they land in `cascade`, ordered
    /// so that dependents are reverted before the transactions they depend on.
    pub fn plan_revert(&self, target: TxnId, mode: RevertMode) -> RevertPlan {
        let downstream = self.transitive_dependents(target);
        match mode {
            RevertMode::Halt => RevertPlan {
                target,
                mode,
                blocked_by: downstream,
                cascade: Vec::new(),
            },
            RevertMode::Cascade => {
                // Revert deepest-first: a dependent must be undone before its dependency.
                let mut ordered = downstream;
                ordered.sort_by(|a, b| b.cmp(a));
                RevertPlan { target, mode, blocked_by: Vec::new(), cascade: ordered }
            }
        }
    }

    /// The tree a halted revert shows the caller. Cycle-safe: a transaction already printed on
    /// the current path is marked rather than followed, so a read-write cycle cannot loop.
    pub fn render_tree(&self, root: TxnId) -> String {
        let mut out = String::new();
        out.push_str(&format!("{} (revert target)\n", root));
        let mut path = vec![root];
        self.render_children(root, "", &mut path, &mut out);
        out
    }

    fn render_children(
        &self,
        node: TxnId,
        prefix: &str,
        path: &mut Vec<TxnId>,
        out: &mut String,
    ) {
        let kids = self.dependents_of(node);
        for (i, k) in kids.iter().enumerate() {
            let last = i + 1 == kids.len();
            let branch = if last { "`-- " } else { "|-- " };
            let via: Vec<String> = self
                .edges
                .iter()
                .filter(|e| e.from == node && e.to == *k)
                .map(|e| format!("{}@{}", e.via.row, e.via.begin_ts))
                .collect();
            if path.contains(k) {
                out.push_str(&format!("{}{}{} (cycle)\n", prefix, branch, k));
                continue;
            }
            out.push_str(&format!("{}{}{} read {}\n", prefix, branch, k, via.join(",")));
            let child_prefix = format!("{}{}", prefix, if last { "    " } else { "|   " });
            path.push(*k);
            self.render_children(*k, &child_prefix, path, out);
            path.pop();
        }
    }
}

/// Assembles a [`DependencyGraph`] from the writes and reads each transaction retained.
#[derive(Debug, Clone, Default)]
pub struct DependencyGraphBuilder {
    writes: Vec<(TxnId, VersionRef)>,
    reads: Vec<(TxnId, VersionRef)>,
    valued_writes: Vec<ValuedWrite>,
    predicate_reads: Vec<PredicateRead>,
}

/// A write with the value it produced. Needed only for predicate-derived edges, where the
/// question is "did what you wrote fall inside the region I scanned".
#[derive(Debug, Clone, PartialEq)]
struct ValuedWrite {
    txn: TxnId,
    version: VersionRef,
    col: Option<ColId>,
    value: Value,
}

/// A retained range/scan read, with the snapshot it read at.
#[derive(Debug, Clone, PartialEq)]
struct PredicateRead {
    txn: TxnId,
    summary: PredicateSummary,
    observed_at: u64,
}

impl DependencyGraphBuilder {
    pub fn new() -> Self {
        DependencyGraphBuilder::default()
    }

    /// Record that `txn` produced version `v`.
    pub fn record_write(&mut self, txn: TxnId, v: VersionRef) {
        self.writes.push((txn, v));
    }

    /// Record that `txn` read version `v`.
    pub fn record_read(&mut self, txn: TxnId, v: VersionRef) {
        self.reads.push((txn, v));
    }

    /// Record every exact version in `read_sets` as read by `txn`.
    ///
    /// Predicate-form read-sets contribute no edges here: they name a region, not versions. A
    /// revert that must account for predicate reads has to re-evaluate `PredicateSummary::covers`
    /// against the reverted values, which is a different pass and deliberately not smuggled in
    /// as a fake exact edge.
    pub fn record_read_sets(&mut self, txn: TxnId, read_sets: &[ReadSet]) {
        for rs in read_sets {
            if let ReadSet::ExactVersions(vs) = rs {
                for v in vs {
                    self.record_read(txn, *v);
                }
            }
        }
    }

    /// Record a write together with the value it wrote, so predicate reads can be checked
    /// against it. Also records the plain write, so exact-version edges still form.
    pub fn record_write_value(
        &mut self,
        txn: TxnId,
        v: VersionRef,
        col: Option<ColId>,
        value: Option<Value>,
    ) {
        self.record_write(txn, v);
        if let Some(value) = value {
            self.valued_writes.push(ValuedWrite { txn, version: v, col, value });
        }
    }

    /// Record that `txn` scanned a region at snapshot `observed_at`.
    ///
    /// This is the pass [`DependencyGraphBuilder::record_read_sets`] deliberately refuses to do
    /// inline: a predicate read names a region, not versions, so its edges have to be derived by
    /// re-evaluating [`PredicateSummary::covers`] against the values that were actually written.
    /// The timestamp is load-bearing — coverage alone would make a scan depend on writes that
    /// landed after it looked.
    pub fn record_predicate_read(
        &mut self,
        txn: TxnId,
        summary: PredicateSummary,
        observed_at: u64,
    ) {
        self.predicate_reads.push(PredicateRead { txn, summary, observed_at });
    }

    pub fn build(&self) -> DependencyGraph {
        let mut g = DependencyGraph::new();
        for (writer, wv) in &self.writes {
            for (reader, rv) in &self.reads {
                if reader != writer && rv == wv {
                    g.add_edge(DependencyEdge { from: *writer, to: *reader, via: *wv });
                }
            }
        }
        for w in &self.valued_writes {
            for r in &self.predicate_reads {
                if r.txn == w.txn {
                    continue;
                }
                // The scan can only have seen a version its snapshot admitted. Strictly less
                // than, matching the engine's own rule in `ReadView::is_commited_for_me`
                // (`ts < snapshot.high_water`): a version stamped exactly at the high water mark
                // is not yet visible.
                if w.version.begin_ts >= r.observed_at {
                    continue;
                }
                if r.summary.covers(w.version.tbl, w.col, &w.value) {
                    let edge =
                        DependencyEdge { from: w.txn, to: r.txn, via: w.version };
                    if !g.edges.contains(&edge) {
                        g.add_edge(edge);
                    }
                }
            }
        }
        g
    }
}

/// The answer a `REVERT` returns.
#[derive(Debug, Clone, PartialEq)]
pub struct RevertPlan {
    pub target: TxnId,
    pub mode: RevertMode,
    /// Non-empty under `Halt` when downstream work exists. The revert did **not** happen.
    pub blocked_by: Vec<TxnId>,
    /// Under `Cascade`, the transactions to undo before `target`, in order.
    pub cascade: Vec<TxnId>,
}

impl RevertPlan {
    /// True when the revert was refused because downstream work depends on the target.
    pub fn is_blocked(&self) -> bool {
        !self.blocked_by.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::heap_file_manager::RecordId;
    use crate::tel::ids::{RowId, TableId};

    fn vref(row: u64, ts: u64) -> VersionRef {
        VersionRef {
            tbl: TableId(1),
            row: RowId(row),
            rid: RecordId { page_id: 1, slot_num: row as u16 },
            begin_ts: ts,
        }
    }

    /// txn1 writes v; txn2 reads v and writes w; txn3 reads w.
    fn chain() -> DependencyGraph {
        let (v, w) = (vref(1, 10), vref(2, 20));
        let mut b = DependencyGraphBuilder::new();
        b.record_write(TxnId(1), v);
        b.record_read(TxnId(2), v);
        b.record_write(TxnId(2), w);
        b.record_read(TxnId(3), w);
        b.build()
    }

    #[test]
    fn a_downstream_dependent_is_found_through_the_read_set() {
        let g = chain();
        assert_eq!(g.dependents_of(TxnId(1)), vec![TxnId(2)]);
        assert_eq!(g.transitive_dependents(TxnId(1)), vec![TxnId(2), TxnId(3)]);
    }

    #[test]
    fn halt_is_the_default_and_reverts_nothing() {
        assert_eq!(RevertMode::default(), RevertMode::Halt);
        let plan = chain().plan_revert(TxnId(1), RevertMode::Halt);
        assert!(plan.is_blocked());
        assert_eq!(plan.blocked_by, vec![TxnId(2), TxnId(3)]);
        assert!(plan.cascade.is_empty());
    }

    #[test]
    fn cascade_orders_dependents_before_their_dependency() {
        let plan = chain().plan_revert(TxnId(1), RevertMode::Cascade);
        assert!(!plan.is_blocked());
        assert_eq!(plan.cascade, vec![TxnId(3), TxnId(2)]);
    }

    #[test]
    fn a_transaction_does_not_depend_on_itself() {
        let v = vref(1, 10);
        let mut b = DependencyGraphBuilder::new();
        b.record_write(TxnId(1), v);
        b.record_read(TxnId(1), v);
        assert!(b.build().dependents_of(TxnId(1)).is_empty());
    }

    #[test]
    fn a_predicate_read_depends_on_a_write_that_landed_inside_its_range() {
        use crate::provenance::readset::{Bound, PredicateSummary};
        let mut b = DependencyGraphBuilder::new();
        // txn1 sets col0 of row1 to 15 at ts 10.
        b.record_write_value(
            TxnId(1),
            vref(1, 10),
            Some(ColId(0)),
            Some(Value::Integer(15)),
        );
        // txn2 scans col0 in [10, 20) at snapshot 30, so it saw that write.
        b.record_predicate_read(
            TxnId(2),
            PredicateSummary {
                tbl: TableId(1),
                col: Some(ColId(0)),
                lo: Bound::Included(Value::Integer(10)),
                hi: Bound::Excluded(Value::Integer(20)),
                residual: None,
                rows_observed: 1,
            },
            30,
        );
        let g = b.build();
        assert_eq!(g.dependents_of(TxnId(1)), vec![TxnId(2)]);
    }

    #[test]
    fn a_write_outside_the_scanned_range_creates_no_edge() {
        use crate::provenance::readset::{Bound, PredicateSummary};
        let mut b = DependencyGraphBuilder::new();
        b.record_write_value(
            TxnId(1),
            vref(1, 10),
            Some(ColId(0)),
            Some(Value::Integer(99)),
        );
        b.record_predicate_read(
            TxnId(2),
            PredicateSummary {
                tbl: TableId(1),
                col: Some(ColId(0)),
                lo: Bound::Included(Value::Integer(10)),
                hi: Bound::Excluded(Value::Integer(20)),
                residual: None,
                rows_observed: 0,
            },
            30,
        );
        assert!(b.build().edges.is_empty());
    }

    #[test]
    fn a_scan_does_not_depend_on_a_write_it_could_not_have_seen() {
        use crate::provenance::readset::PredicateSummary;
        let mut b = DependencyGraphBuilder::new();
        // Write lands at ts 40; the scan read at snapshot 30.
        b.record_write_value(TxnId(1), vref(1, 40), None, Some(Value::Integer(15)));
        b.record_predicate_read(TxnId(2), PredicateSummary::full_scan(TableId(1), 3), 30);
        assert!(b.build().edges.is_empty());
    }

    #[test]
    fn the_halt_tree_shows_the_whole_downstream_chain() {
        let tree = chain().render_tree(TxnId(1));
        assert!(tree.contains("txn1 (revert target)"), "{}", tree);
        assert!(tree.contains("txn2 read row1@10"), "{}", tree);
        assert!(tree.contains("txn3 read row2@20"), "{}", tree);
    }

    #[test]
    fn the_tree_terminates_on_a_read_write_cycle() {
        let (v, w) = (vref(1, 10), vref(2, 20));
        let mut b = DependencyGraphBuilder::new();
        b.record_write(TxnId(1), v);
        b.record_read(TxnId(2), v);
        b.record_write(TxnId(2), w);
        b.record_read(TxnId(1), w);
        let g = b.build();
        let tree = g.render_tree(TxnId(1));
        assert!(tree.contains("cycle"), "{}", tree);
        assert_eq!(g.transitive_dependents(TxnId(1)), vec![TxnId(2)]);
    }

    #[test]
    fn predicate_reads_contribute_no_fake_exact_edges() {
        use crate::provenance::readset::PredicateSummary;
        let mut b = DependencyGraphBuilder::new();
        b.record_write(TxnId(1), vref(1, 10));
        b.record_read_sets(
            TxnId(2),
            &[ReadSet::Predicate(PredicateSummary::full_scan(TableId(1), 5))],
        );
        assert!(b.build().edges.is_empty());
    }
}
