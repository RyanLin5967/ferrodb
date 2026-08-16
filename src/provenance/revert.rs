//! Causal rollback over retained read-sets.
//!
//! Design authority: DESIGN.md section 2 ("Causal rollback") and exit criterion 10.
//!
//! Reverting write A must find the write B that *read* A, and B's dependents. **Halt by default**
//! and show the tree; cascade only on explicit request. Silently cascading a revert through an
//! agent's downstream work is not recoverable by the agent.

use crate::provenance::readset::{ReadSet, VersionRef};
use crate::tel::ids::TxnId;

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
}

/// Assembles a [`DependencyGraph`] from the writes and reads each transaction retained.
#[derive(Debug, Clone, Default)]
pub struct DependencyGraphBuilder {
    writes: Vec<(TxnId, VersionRef)>,
    reads: Vec<(TxnId, VersionRef)>,
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

    pub fn build(&self) -> DependencyGraph {
        let mut g = DependencyGraph::new();
        for (writer, wv) in &self.writes {
            for (reader, rv) in &self.reads {
                if reader != writer && rv == wv {
                    g.add_edge(DependencyEdge { from: *writer, to: *reader, via: *wv });
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
