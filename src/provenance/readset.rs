//! Retained read-sets.
//!
//! Design authority: DESIGN.md section 2.
//!
//! **Two forms, chosen by ACCESS SHAPE, never by size:**
//! - point / index lookup -> exact version ids (small, and gives exact causal edges)
//! - range / scan -> retained predicate summary
//!
//! Two traps this module exists to make unrepresentable:
//!
//! 1. **No Bloom filters over row identity.** 9.6 bits/row at 1% false positive versus ~1 bit/row
//!    for a bitmap — worse than the exact set it replaces, and it loses phantom coverage.
//! 2. **Never coarsen scattered point reads into one enclosing interval.** For `k` scattered
//!    reads over `N` rows the enclosing interval covers `N(k-1)/(k+1)` rows — at `k = 3` that is
//!    half the table. A cliff, not a tradeoff. Coarsening only pays when the access was already
//!    clustered, and "was it clustered" is the access shape, which the caller knows and the size
//!    does not.

use crate::catalog::column::Value;
use crate::storage::heap_file_manager::RecordId;
use crate::tel::ids::{ColId, RowId, TableId};

/// The shape of the access that produced a read, as the executor knows it.
///
/// This is the *only* input allowed to select a read-set form. Size is explicitly not an input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccessShape {
    /// Single-row fetch by rid.
    Point,
    /// Index probe for one key or a handful of keys.
    IndexLookup,
    /// Ordered range over an index.
    Range,
    /// Full table scan.
    FullScan,
}

/// Which representation an access shape demands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadSetForm {
    ExactVersions,
    Predicate,
}

impl AccessShape {
    pub fn form(&self) -> ReadSetForm {
        match self {
            AccessShape::Point | AccessShape::IndexLookup => ReadSetForm::ExactVersions,
            AccessShape::Range | AccessShape::FullScan => ReadSetForm::Predicate,
        }
    }
}

/// One exact version that was read. Gives exact causal edges: revert can find precisely which
/// later write depended on this version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionRef {
    pub tbl: TableId,
    /// Immutable surrogate identity, stable across version writes.
    pub row: RowId,
    /// The physical slot the version was read from.
    pub rid: RecordId,
    /// The version's `begin_ts` from the 24-byte tuple version header. Together with `row` this
    /// names one specific version, not just one row.
    pub begin_ts: u64,
}

/// An interval endpoint.
#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    Unbounded,
    Included(Value),
    Excluded(Value),
}

/// A retained predicate: what a range or scan *looked at*, including the rows that were not
/// there. This is what gives phantom coverage; an exact set of the rows that happened to exist
/// cannot detect an insert into the range.
#[derive(Debug, Clone, PartialEq)]
pub struct PredicateSummary {
    pub tbl: TableId,
    /// The column the range was over. `None` for an unqualified full scan.
    pub col: Option<ColId>,
    pub lo: Bound,
    pub hi: Bound,
    /// Any residual filter applied on top of the range, kept verbatim so a re-check is possible.
    pub residual: Option<String>,
    /// How many rows the scan actually returned. Diagnostic only — it must never feed back into
    /// the choice of form.
    pub rows_observed: u64,
}

impl PredicateSummary {
    pub fn full_scan(tbl: TableId, rows_observed: u64) -> Self {
        PredicateSummary {
            tbl,
            col: None,
            lo: Bound::Unbounded,
            hi: Bound::Unbounded,
            residual: None,
            rows_observed,
        }
    }

    /// Whether a written value falls inside this predicate — the phantom check.
    pub fn covers(&self, tbl: TableId, col: Option<ColId>, v: &Value) -> bool {
        if self.tbl != tbl {
            return false;
        }
        if self.col.is_some() && self.col != col {
            return false;
        }
        let lo_ok = match &self.lo {
            Bound::Unbounded => true,
            Bound::Included(b) => v >= b,
            Bound::Excluded(b) => v > b,
        };
        let hi_ok = match &self.hi {
            Bound::Unbounded => true,
            Bound::Included(b) => v <= b,
            Bound::Excluded(b) => v < b,
        };
        lo_ok && hi_ok
    }
}

/// What a transaction read, retained for merge validation, causal rollback and the verification
/// gate.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadSet {
    ExactVersions(Vec<VersionRef>),
    Predicate(PredicateSummary),
}

impl ReadSet {
    pub fn form(&self) -> ReadSetForm {
        match self {
            ReadSet::ExactVersions(_) => ReadSetForm::ExactVersions,
            ReadSet::Predicate(_) => ReadSetForm::Predicate,
        }
    }

    /// Did this read-set observe `v`?
    pub fn contains_version(&self, v: &VersionRef) -> bool {
        match self {
            ReadSet::ExactVersions(vs) => vs.contains(v),
            // A predicate summary cannot answer this by version identity; the caller must use
            // `PredicateSummary::covers` with the actual value.
            ReadSet::Predicate(_) => false,
        }
    }

    /// Rows named exactly. Empty for a predicate summary — deliberately, so that a caller that
    /// needs row identity is forced to notice it does not have it.
    pub fn exact_rows(&self) -> Vec<(TableId, RowId)> {
        match self {
            ReadSet::ExactVersions(vs) => vs.iter().map(|v| (v.tbl, v.row)).collect(),
            ReadSet::Predicate(_) => Vec::new(),
        }
    }
}

/// Accumulates a transaction's reads.
///
/// The builder has no method that converts exact versions into a predicate, and that omission is
/// the point: there is no code path by which scattered point reads get coarsened into an
/// enclosing interval, whatever the count.
#[derive(Debug, Clone, Default)]
pub struct ReadSetBuilder {
    exact: Vec<VersionRef>,
    predicates: Vec<PredicateSummary>,
}

impl ReadSetBuilder {
    pub fn new() -> Self {
        ReadSetBuilder::default()
    }

    /// Record a point or index-lookup read. De-duplicates.
    pub fn observe_version(&mut self, v: VersionRef) {
        if let Err(i) = self.exact.binary_search(&v) {
            self.exact.insert(i, v);
        }
    }

    /// Record a range or full-scan read.
    pub fn observe_predicate(&mut self, p: PredicateSummary) {
        self.predicates.push(p);
    }

    /// Route by access shape. `versions` are the rows the access actually returned; they are
    /// retained only when the shape calls for exact versions.
    pub fn observe(&mut self, shape: AccessShape, versions: Vec<VersionRef>, summary: Option<PredicateSummary>) {
        match shape.form() {
            ReadSetForm::ExactVersions => {
                for v in versions {
                    self.observe_version(v);
                }
            }
            ReadSetForm::Predicate => {
                if let Some(p) = summary {
                    self.observe_predicate(p);
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.predicates.is_empty()
    }

    pub fn finish(self) -> Vec<ReadSet> {
        let mut out = Vec::with_capacity(self.predicates.len() + 1);
        if !self.exact.is_empty() {
            out.push(ReadSet::ExactVersions(self.exact));
        }
        for p in self.predicates {
            out.push(ReadSet::Predicate(p));
        }
        out
    }
}

/// **`write-set \ read-set`**: rows the agent changed without ever looking at them.
///
/// The cheap novel metric from DESIGN.md section 4 — nearly free, no threshold to tune, high
/// precision, and available only because read-sets are retained at all. Returns the written cells
/// that no read-set observed.
pub fn blind_writes(
    write_set: &[(TableId, RowId, Option<ColId>)],
    read_sets: &[ReadSet],
) -> Vec<(TableId, RowId, Option<ColId>)> {
    let mut read_rows: Vec<(TableId, RowId)> = Vec::new();
    let mut predicate_tables: Vec<TableId> = Vec::new();
    for rs in read_sets {
        match rs {
            ReadSet::ExactVersions(vs) => read_rows.extend(vs.iter().map(|v| (v.tbl, v.row))),
            // A scan looked at every row of its table, so nothing written there is blind.
            ReadSet::Predicate(p) => predicate_tables.push(p.tbl),
        }
    }
    write_set
        .iter()
        .filter(|(tbl, row, _)| {
            !predicate_tables.contains(tbl) && !read_rows.contains(&(*tbl, *row))
        })
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vref(row: u64, ts: u64) -> VersionRef {
        VersionRef {
            tbl: TableId(1),
            row: RowId(row),
            rid: RecordId { page_id: 1, slot_num: row as u16 },
            begin_ts: ts,
        }
    }

    #[test]
    fn form_is_decided_by_shape_alone() {
        assert_eq!(AccessShape::Point.form(), ReadSetForm::ExactVersions);
        assert_eq!(AccessShape::IndexLookup.form(), ReadSetForm::ExactVersions);
        assert_eq!(AccessShape::Range.form(), ReadSetForm::Predicate);
        assert_eq!(AccessShape::FullScan.form(), ReadSetForm::Predicate);
    }

    #[test]
    fn scattered_point_reads_are_never_coarsened_however_many() {
        let mut b = ReadSetBuilder::new();
        // 500 scattered point reads: the enclosing interval would cover essentially the table.
        for i in 0..500u64 {
            b.observe(AccessShape::Point, vec![vref(i * 977, i)], None);
        }
        let sets = b.finish();
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].form(), ReadSetForm::ExactVersions);
        assert_eq!(sets[0].exact_rows().len(), 500);
    }

    #[test]
    fn predicate_summary_gives_phantom_coverage() {
        let p = PredicateSummary {
            tbl: TableId(1),
            col: Some(ColId(0)),
            lo: Bound::Included(Value::Integer(10)),
            hi: Bound::Excluded(Value::Integer(20)),
            residual: None,
            rows_observed: 3,
        };
        assert!(p.covers(TableId(1), Some(ColId(0)), &Value::Integer(10)));
        assert!(p.covers(TableId(1), Some(ColId(0)), &Value::Integer(19)));
        assert!(!p.covers(TableId(1), Some(ColId(0)), &Value::Integer(20)));
        assert!(!p.covers(TableId(2), Some(ColId(0)), &Value::Integer(11)));
    }

    #[test]
    fn blind_writes_finds_the_row_nobody_looked_at() {
        let reads = vec![ReadSet::ExactVersions(vec![vref(1, 5)])];
        let writes = vec![
            (TableId(1), RowId(1), Some(ColId(0))),
            (TableId(1), RowId(2), Some(ColId(0))),
        ];
        let blind = blind_writes(&writes, &reads);
        assert_eq!(blind, vec![(TableId(1), RowId(2), Some(ColId(0)))]);
    }

    #[test]
    fn a_scan_makes_no_write_in_that_table_blind() {
        let reads = vec![ReadSet::Predicate(PredicateSummary::full_scan(TableId(1), 99))];
        let writes = vec![(TableId(1), RowId(7), None), (TableId(2), RowId(7), None)];
        assert_eq!(blind_writes(&writes, &reads), vec![(TableId(2), RowId(7), None)]);
    }
}
