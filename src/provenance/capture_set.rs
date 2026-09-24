//! Every retained capture, plus an index of what each one READ — so `REVERT` can walk out from
//! the merge it is reverting instead of joining every write against every read ever retained.
//!
//! Design authority: DESIGN.md section 2 and exit criterion 10; the wall this answers is #18 in
//! `artie-research` `frontier/paper_q2_walls.md`, with the design and its pre-registration in
//! `frontier/lane_wall18_revert.md` §4 and Amendment 1.
//!
//! # What it replaces, and what it must not change
//!
//! `revert_merge` used to fold every retained capture into a [`ProvenanceLog`] and ask
//! [`crate::provenance::revert::DependencyGraphBuilder::build`] for the whole graph — two nested
//! loops, exact writes × exact reads and valued writes × predicate reads — only to keep the
//! target's transitive dependents and throw the rest away. Captures of published tasks are never
//! dropped, so that was Θ(N²) comparisons per revert after N merged tasks, under the lock every
//! statement takes.
//!
//! The answer must not move. A plan is a function of ONE set — everything transitively downstream
//! of the target — so any planner that finds the same set over the same edge relation returns an
//! identical [`RevertPlan`] (both end in [`RevertPlan::from_downstream`]). The relation, read off
//! `build`: T → D iff D ≠ T and either
//!
//! 1. D retained, exactly, a version T wrote; or
//! 2. D retained a predicate read `(S, observed_at)`, T retained a valued write `(v, col, value)`,
//!    `v.begin_ts < observed_at`, and `S.covers(v.tbl, col, value)`.
//!
//! # How it finds the same set
//!
//! Writes need no index: the walk asks "what did T write" only of T's own capture. READS are
//! indexed as they are retained, so "who read this" is a lookup:
//!
//! - an exact read goes in `readers`, keyed by the version;
//! - a predicate read goes in exactly one of three buckets, by the shape of its summary:
//!   - **point** — one column, `lo == hi`, both inclusive (`UPDATE ... WHERE id = 7`) — keyed by
//!     `(table, column)` then by value, with `Value`'s own `Ord`, the order `covers` compares with;
//!   - **unbounded** — both bounds open (a full scan, the agent's natural read) — keyed by
//!     `(table, column or none)` and ordered by `observed_at`, so the reads that could have seen a
//!     write are exactly a range;
//!   - **range** — everything else, kept per table and examined in full.
//!
//! The buckets only NARROW which reads are looked at. Every candidate is re-checked against the
//! whole rule — self, timestamp, `covers` — so a bucket that admits too much costs time and one
//! that admits too little is the only way to be wrong. Classification is exhaustive (anything not
//! a point and not unbounded is a range), and each bucket's lookup is a superset of the reads its
//! rule can accept.
//!
//! # Where it is maintained
//!
//! The set owns the captures privately and exposes no `&mut TxnCapture`, so a read cannot be
//! retained without being indexed: `insert` and `remove` index and unindex whole captures, and
//! [`CaptureEntry`] is the only way to add to one. [`CaptureSet::index_is_consistent`] rebuilds the
//! index from the captures and compares, for tests and debug checks.
//!
//! # Two derivations, stated
//!
//! This is a SECOND derivation of the edge relation beside `build`, which still serves
//! [`ProvenanceLog`]'s callers and is kept as this walk's oracle
//! ([`CaptureSet::plan_revert_by_full_graph`]). The runtime used to carry a note saying "one
//! derivation, and it lives in `ProvenanceLog::dependency_graph`"; that stopped being true here, on
//! purpose. What keeps the two from drifting: both live in this module's parent, the unit test
//! `the_walk_agrees_with_the_full_graph_for_every_target_and_mode`, and a `debug_assert_eq!` in
//! `AgentRuntime::revert_merge` that runs the oracle beside every REVERT in a debug build.
//!
//! # Residual cost, stated so it is not found later
//!
//! Exact reads, points and unbounded scans are looked up in time proportional to what they return.
//! The range bucket is not: it is examined in full for every valued write the walk visits in that
//! table, so a table carrying many bounded range reads makes each visited write pay for all of
//! them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound as Edge;

use crate::branch::types::BranchId;
use crate::catalog::column::Value;
use crate::provenance::capture::{ProvenanceLog, TimedPredicate, TxnCapture, WriteRecord};
use crate::provenance::readset::{AccessShape, Bound, PredicateSummary, VersionRef};
use crate::provenance::revert::{RevertMode, RevertPlan};
use crate::provenance::ProvId;
use crate::tel::ids::{ColId, TableId, TxnId};

/// What one demand-driven plan consulted. Both are counts of work done, never of work avoided.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalkCost {
    /// Captures the walk visited: the target, then each dependent it reached.
    pub captures: u64,
    /// Index entries examined: one per (write, candidate read) pair looked at, whether or not the
    /// pair became an edge. The walk's analogue of `GRAPH_BUILD_PAIRS`.
    pub candidates: u64,
}

/// The index of retained READS. Derived entirely from the captures beside it; see
/// [`CaptureSet::index_is_consistent`].
#[derive(Debug, Clone, Default, PartialEq)]
struct ReadIndex {
    /// Exact version → the transactions that retained it exactly.
    readers: HashMap<VersionRef, BTreeSet<TxnId>>,
    /// `(table, column)` → point value → `(reader, position in its predicate_reads)`.
    points: HashMap<(TableId, ColId), BTreeMap<Value, BTreeSet<(TxnId, usize)>>>,
    /// `(table, column or none)` → `(observed_at, reader, position)`, ordered by `observed_at`.
    unbounded: HashMap<(TableId, Option<ColId>), BTreeSet<(u64, TxnId, usize)>>,
    /// table → `(reader, position)` for every other predicate shape.
    ranges: HashMap<TableId, BTreeSet<(TxnId, usize)>>,
}

/// Which bucket a predicate read belongs in. Exhaustive by construction: `bucket_of` returns
/// `Range` for anything it does not recognise, so no shape can be left out of the index.
enum Bucket<'a> {
    Point(TableId, ColId, &'a Value),
    Unbounded(TableId, Option<ColId>),
    Range(TableId),
}

fn bucket_of(s: &PredicateSummary) -> Bucket<'_> {
    match (&s.lo, &s.hi, s.col) {
        (Bound::Unbounded, Bound::Unbounded, col) => Bucket::Unbounded(s.tbl, col),
        // `lo == hi` under `Value`'s `Ord`, so `covers` accepts exactly the values equal to `a`,
        // which is exactly what a `BTreeMap` lookup keyed by `a` returns.
        (Bound::Included(a), Bound::Included(b), Some(col)) if a == b => {
            Bucket::Point(s.tbl, col, a)
        }
        _ => Bucket::Range(s.tbl),
    }
}

impl ReadIndex {
    fn of(captures: &BTreeMap<u64, TxnCapture>) -> ReadIndex {
        let mut ix = ReadIndex::default();
        for c in captures.values() {
            ix.add_capture(c);
        }
        ix
    }

    fn add_capture(&mut self, c: &TxnCapture) {
        let txn = c.txn();
        for v in c.exact_reads() {
            self.add_exact(txn, *v);
        }
        for (at, p) in c.predicate_reads().iter().enumerate() {
            self.add_predicate(txn, at, p);
        }
    }

    fn remove_capture(&mut self, c: &TxnCapture) {
        let txn = c.txn();
        for v in c.exact_reads() {
            self.remove_exact(txn, v);
        }
        for (at, p) in c.predicate_reads().iter().enumerate() {
            self.remove_predicate(txn, at, p);
        }
    }

    fn add_exact(&mut self, txn: TxnId, v: VersionRef) {
        self.readers.entry(v).or_default().insert(txn);
    }

    fn remove_exact(&mut self, txn: TxnId, v: &VersionRef) {
        if let Some(rs) = self.readers.get_mut(v) {
            rs.remove(&txn);
            if rs.is_empty() {
                self.readers.remove(v);
            }
        }
    }

    fn add_predicate(&mut self, txn: TxnId, at: usize, p: &TimedPredicate) {
        match bucket_of(&p.summary) {
            Bucket::Point(t, c, v) => {
                let by_value = self.points.entry((t, c)).or_default();
                by_value.entry(v.clone()).or_default().insert((txn, at));
            }
            Bucket::Unbounded(t, c) => {
                self.unbounded.entry((t, c)).or_default().insert((p.observed_at, txn, at));
            }
            Bucket::Range(t) => {
                self.ranges.entry(t).or_default().insert((txn, at));
            }
        }
    }

    /// Empty containers are dropped as they empty, so a forgotten transaction leaves no trace and
    /// an index rebuilt from the captures compares equal to this one.
    fn remove_predicate(&mut self, txn: TxnId, at: usize, p: &TimedPredicate) {
        match bucket_of(&p.summary) {
            Bucket::Point(t, c, v) => {
                if let Some(by_value) = self.points.get_mut(&(t, c)) {
                    if let Some(ids) = by_value.get_mut(v) {
                        ids.remove(&(txn, at));
                        if ids.is_empty() {
                            by_value.remove(v);
                        }
                    }
                    if by_value.is_empty() {
                        self.points.remove(&(t, c));
                    }
                }
            }
            Bucket::Unbounded(t, c) => {
                if let Some(line) = self.unbounded.get_mut(&(t, c)) {
                    line.remove(&(p.observed_at, txn, at));
                    if line.is_empty() {
                        self.unbounded.remove(&(t, c));
                    }
                }
            }
            Bucket::Range(t) => {
                if let Some(ids) = self.ranges.get_mut(&t) {
                    ids.remove(&(txn, at));
                    if ids.is_empty() {
                        self.ranges.remove(&t);
                    }
                }
            }
        }
    }

    /// Every predicate read that COULD cover a valued write of `value` to `col` of `tbl` stamped
    /// `begin_ts` — a superset of the ones that do. The caller re-checks each against the full
    /// rule.
    fn predicate_candidates(
        &self,
        tbl: TableId,
        col: Option<ColId>,
        value: &Value,
        begin_ts: u64,
    ) -> Vec<(TxnId, usize)> {
        let mut out = Vec::new();
        // A read admits a version stamped strictly below its snapshot (`begin_ts < observed_at`),
        // so the reads that could have seen this one are every key above `(begin_ts, max, max)`.
        let after = Edge::Excluded((begin_ts, TxnId(u64::MAX), usize::MAX));
        // A summary naming a column matches only a write recorded against that column, so the
        // column-keyed buckets are consulted only for a write that has one.
        if let Some(c) = col {
            if let Some(ids) = self.points.get(&(tbl, c)).and_then(|by_value| by_value.get(value)) {
                out.extend(ids.iter().copied());
            }
            if let Some(line) = self.unbounded.get(&(tbl, Some(c))) {
                out.extend(line.range((after, Edge::Unbounded)).map(|&(_, t, at)| (t, at)));
            }
        }
        // A summary with no column matches a write to any column, or to none.
        if let Some(line) = self.unbounded.get(&(tbl, None)) {
            out.extend(line.range((after, Edge::Unbounded)).map(|&(_, t, at)| (t, at)));
        }
        if let Some(ids) = self.ranges.get(&tbl) {
            out.extend(ids.iter().copied());
        }
        out
    }
}

/// Every retained capture, keyed by its own transaction id, with the index of what each read.
///
/// See the module docs for the invariant and for why nothing outside can mutate a capture.
#[derive(Debug, Clone, Default)]
pub struct CaptureSet {
    captures: BTreeMap<u64, TxnCapture>,
    index: ReadIndex,
}

impl CaptureSet {
    pub fn new() -> Self {
        CaptureSet::default()
    }

    /// Insert `capture` under `key`, indexing everything it has already retained, and hand back
    /// whatever held that key (unindexed first).
    ///
    /// **Refuses a key that is not the capture's own transaction.** The runtime always spells the
    /// two the same way (`captures.insert(txn.0, TxnCapture::new(txn, ..))`), and the index names
    /// readers by the capture's `txn()` while lookups go by key: a disagreement would make the walk
    /// look up a reader under a key that holds someone else, silently.
    pub fn insert(&mut self, key: u64, capture: TxnCapture) -> Option<TxnCapture> {
        assert_eq!(
            key,
            capture.txn().0,
            "a capture must be keyed by its own transaction id: key {} holds {:?}",
            key,
            capture.txn()
        );
        let old = self.remove(&key);
        self.index.add_capture(&capture);
        self.captures.insert(key, capture);
        old
    }

    /// Remove a capture and every index entry it contributed.
    pub fn remove(&mut self, key: &u64) -> Option<TxnCapture> {
        let old = self.captures.remove(key)?;
        self.index.remove_capture(&old);
        Some(old)
    }

    pub fn get(&self, key: &u64) -> Option<&TxnCapture> {
        self.captures.get(key)
    }

    pub fn contains_key(&self, key: &u64) -> bool {
        self.captures.contains_key(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &u64> {
        self.captures.keys()
    }

    pub fn len(&self) -> usize {
        self.captures.len()
    }

    pub fn is_empty(&self) -> bool {
        self.captures.is_empty()
    }

    /// The capture for `txn`, opened if absent — `or_insert_with`, never `if let Some`, for the
    /// reason the runtime's retention sites give: a read that finds no capture and retains nothing
    /// is indistinguishable from one that had nothing to retain. Every retention goes through the
    /// returned entry, which is what keeps the index true.
    pub fn entry(&mut self, txn: TxnId, prov: ProvId, branch: BranchId) -> CaptureEntry<'_> {
        self.captures.entry(txn.0).or_insert_with(|| TxnCapture::new(txn, prov, branch));
        CaptureEntry { set: self, txn }
    }

    /// Plan a revert of `target` by walking out from it: the target's writes, the reads the index
    /// says could have seen them, each re-checked in full, then the same from every dependent
    /// found.
    ///
    /// The same plan as [`CaptureSet::plan_revert_by_full_graph`], by the argument in the module
    /// docs, at a cost set by the answer rather than by history.
    pub fn plan_revert(&self, target: TxnId, mode: RevertMode) -> (RevertPlan, WalkCost) {
        let mut cost = WalkCost::default();
        let mut seen: BTreeSet<TxnId> = BTreeSet::new();
        seen.insert(target);
        let mut frontier: Vec<TxnId> = vec![target];
        while let Some(t) = frontier.pop() {
            // A transaction with no capture wrote nothing that was retained, so nothing depends on
            // it — the full graph has no edge out of it either.
            let Some(cap) = self.captures.get(&t.0) else { continue };
            cost.captures += 1;
            for d in self.dependents_of(cap, &mut cost) {
                if seen.insert(d) {
                    frontier.push(d);
                }
            }
        }
        // `seen` started with the target, so a cycle back to it cannot add it; it goes now. What is
        // left is sorted and de-duplicated, which is exactly `transitive_dependents`' contract.
        seen.remove(&target);
        (RevertPlan::from_downstream(target, mode, seen.into_iter().collect()), cost)
    }

    /// The direct dependents of one capture's writes, possibly repeated.
    fn dependents_of(&self, cap: &TxnCapture, cost: &mut WalkCost) -> Vec<TxnId> {
        let writer = cap.txn();
        let mut out = Vec::new();
        for w in cap.writes() {
            // (1) exact: anyone who retained this very version.
            if let Some(readers) = self.index.readers.get(&w.version) {
                for &r in readers {
                    cost.candidates += 1;
                    if r != writer {
                        out.push(r);
                    }
                }
            }
            // (2) predicate: only a write that carries a value can fall inside a region.
            let Some(value) = w.value.as_ref() else { continue };
            for (reader, at) in
                self.index.predicate_candidates(w.version.tbl, w.col, value, w.version.begin_ts)
            {
                cost.candidates += 1;
                if reader == writer {
                    continue;
                }
                let p = &self
                    .captures
                    .get(&reader.0)
                    .expect("the read index names a transaction this set does not hold")
                    .predicate_reads()[at];
                // The whole rule, as `build` states it. The bucket only chose what to look at.
                let admitted = w.version.begin_ts < p.observed_at;
                if admitted && p.summary.covers(w.version.tbl, w.col, value) {
                    out.push(reader);
                }
            }
        }
        out
    }

    /// The plan the full graph gives: every capture folded into a [`ProvenanceLog`] and joined
    /// pairwise. This is the implementation [`CaptureSet::plan_revert`] replaced, kept as its
    /// oracle. It does not add to `GRAPH_BUILD_PAIRS`, because it runs beside production work
    /// rather than as it.
    pub fn plan_revert_by_full_graph(&self, target: TxnId, mode: RevertMode) -> RevertPlan {
        let mut log = ProvenanceLog::new();
        for c in self.captures.values() {
            log.record(c.clone().finish());
        }
        log.dependency_graph_unobserved().plan_revert(target, mode)
    }

    /// Whether the index is exactly what rebuilding it from the captures would give. O(everything
    /// retained): for tests and debug checks.
    pub fn index_is_consistent(&self) -> bool {
        ReadIndex::of(&self.captures) == self.index
    }

    /// Every transaction any index entry names. For tests.
    #[cfg(test)]
    fn indexed_txns(&self) -> BTreeSet<TxnId> {
        let ix = &self.index;
        let mut out: BTreeSet<TxnId> = BTreeSet::new();
        out.extend(ix.readers.values().flatten().copied());
        out.extend(ix.points.values().flat_map(|m| m.values()).flatten().map(|&(t, _)| t));
        out.extend(ix.unbounded.values().flatten().map(|&(_, t, _)| t));
        out.extend(ix.ranges.values().flatten().map(|&(t, _)| t));
        out
    }
}

/// One capture, opened for retention. Every method retains through the capture's own API and then
/// indexes what the capture NOW holds, so the index follows the capture's routing decision rather
/// than restating it.
pub struct CaptureEntry<'a> {
    set: &'a mut CaptureSet,
    txn: TxnId,
}

impl CaptureEntry<'_> {
    /// [`TxnCapture::on_read`], indexed.
    pub fn on_read(
        &mut self,
        shape: AccessShape,
        versions: Vec<VersionRef>,
        summary: Option<PredicateSummary>,
        observed_at: u64,
    ) {
        let txn = self.txn;
        let CaptureSet { captures, index } = &mut *self.set;
        let cap = captures.get_mut(&txn.0).expect("`CaptureSet::entry` opened this capture");
        let offered = versions.clone();
        let before = cap.predicate_reads().len();
        cap.on_read(shape, versions, summary, observed_at);
        // Whatever the shape rule decided, a version is indexed iff the capture now holds it
        // exactly. Re-adding one held from an earlier read is a no-op on a set.
        for v in offered {
            if cap.has_exact_read(&v) {
                index.add_exact(txn, v);
            }
        }
        for (at, p) in cap.predicate_reads().iter().enumerate().skip(before) {
            index.add_predicate(txn, at, p);
        }
    }

    /// [`TxnCapture::on_write_targeting_read`], indexed.
    pub fn on_write_targeting_read(&mut self, summary: PredicateSummary, observed_at: u64) {
        let txn = self.txn;
        let CaptureSet { captures, index } = &mut *self.set;
        let cap = captures.get_mut(&txn.0).expect("`CaptureSet::entry` opened this capture");
        let before = cap.predicate_reads().len();
        cap.on_write_targeting_read(summary, observed_at);
        for (at, p) in cap.predicate_reads().iter().enumerate().skip(before) {
            index.add_predicate(txn, at, p);
        }
    }

    /// [`TxnCapture::on_write`]. Nothing to index: the walk reads a transaction's writes from its
    /// own capture, and only ever asks that question of a transaction it is visiting.
    pub fn on_write(&mut self, w: WriteRecord) {
        self.set
            .captures
            .get_mut(&self.txn.0)
            .expect("`CaptureSet::entry` opened this capture")
            .on_write(w);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::heap_file_manager::RecordId;
    use crate::tel::ids::RowId;

    const T1: TableId = TableId(1);
    const T2: TableId = TableId(2);

    fn vref(tbl: TableId, row: u64, ts: u64) -> VersionRef {
        VersionRef { tbl, row: RowId(row), rid: RecordId { page_id: 0, slot_num: 0 }, begin_ts: ts }
    }

    fn int(i: i32) -> Value {
        Value::Integer(i)
    }

    fn summary(tbl: TableId, col: Option<u32>, lo: Bound, hi: Bound) -> PredicateSummary {
        PredicateSummary { tbl, col: col.map(ColId), lo, hi, residual: None, rows_observed: 0 }
    }

    fn point(tbl: TableId, col: u32, v: i32) -> PredicateSummary {
        summary(tbl, Some(col), Bound::Included(int(v)), Bound::Included(int(v)))
    }

    /// `txn` publishes `version`, recording the value `v` against column `col` — the shape
    /// `record_applied` gives one cell of a published image.
    fn write(set: &mut CaptureSet, txn: u64, version: VersionRef, col: u32, v: i32) {
        set.entry(TxnId(txn), ProvId::NONE, BranchId::TRUNK)
            .on_write(WriteRecord::new(version, Some(ColId(col)), Some(int(v))));
    }

    fn exact_read(set: &mut CaptureSet, txn: u64, v: VersionRef, observed_at: u64) {
        set.entry(TxnId(txn), ProvId::NONE, BranchId::TRUNK).on_read(
            AccessShape::Point,
            vec![v],
            None,
            observed_at,
        );
    }

    /// `txn` ran `UPDATE ... WHERE id = key` on table 1: a row-targeting point on column 0.
    fn names(set: &mut CaptureSet, txn: u64, key: i32, observed_at: u64) {
        set.entry(TxnId(txn), ProvId::NONE, BranchId::TRUNK)
            .on_write_targeting_read(point(T1, 0, key), observed_at);
    }

    /// `[lo, hi)` over column 1 of table 1.
    fn range1(lo: i32, hi: i32) -> PredicateSummary {
        summary(T1, Some(1), Bound::Included(int(lo)), Bound::Excluded(int(hi)))
    }

    fn scan(set: &mut CaptureSet, txn: u64, s: PredicateSummary, observed_at: u64) {
        set.entry(TxnId(txn), ProvId::NONE, BranchId::TRUNK).on_read(
            AccessShape::FullScan,
            Vec::new(),
            Some(s),
            observed_at,
        );
    }

    fn halt(set: &CaptureSet, txn: u64) -> Vec<TxnId> {
        set.plan_revert(TxnId(txn), RevertMode::Halt).0.blocked_by
    }

    fn ids(v: &[u64]) -> Vec<TxnId> {
        v.iter().map(|&t| TxnId(t)).collect()
    }

    /// A writes v1; B reads v1 exactly and writes v2; C reads v2 exactly. C depends on A only
    /// through B, so a walk that stops after one hop names `[B]` for A.
    #[test]
    fn an_exact_read_makes_its_reader_a_dependent_two_hops_out() {
        let mut set = CaptureSet::new();
        let (v1, v2) = (vref(T1, 1, 1), vref(T1, 2, 2));
        write(&mut set, 1, v1, 1, 10);
        exact_read(&mut set, 2, v1, 2);
        write(&mut set, 2, v2, 1, 20);
        exact_read(&mut set, 3, v2, 3);

        let (halted, cost) = set.plan_revert(TxnId(1), RevertMode::Halt);
        assert_eq!(halted.blocked_by, ids(&[2, 3]), "B read A's version, C read B's");
        assert!(halted.cascade.is_empty());
        assert_eq!(cost.captures, 3, "the walk visits A, B and C and nothing else: {cost:?}");
        let cascaded = set.plan_revert(TxnId(1), RevertMode::Cascade).0;
        assert_eq!(cascaded.cascade, ids(&[3, 2]), "deepest first");
        assert!(cascaded.blocked_by.is_empty());
        assert_eq!(halt(&set, 2), ids(&[3]));
        assert!(halt(&set, 3).is_empty(), "C wrote nothing");
        assert!(set.index_is_consistent());
    }

    /// A publishes row 7 at `begin_ts` 5 (its key column 0 holds 7). B names row 7 at snapshot 6:
    /// a dependent. C names row 7 at snapshot 5, which does not admit a version stamped 5. D names
    /// row 8.
    #[test]
    fn a_point_read_covers_only_its_own_key_and_only_after_the_write() {
        let mut set = CaptureSet::new();
        write(&mut set, 1, vref(T1, 7, 5), 0, 7);
        names(&mut set, 2, 7, 6);
        names(&mut set, 3, 7, 5);
        names(&mut set, 4, 8, 9);

        assert_eq!(halt(&set, 1), ids(&[2]));
        assert!(set.index_is_consistent());
    }

    /// A writes 3 to column 1 of a row in table 1 at `begin_ts` 5. Dependents: B, a column-less
    /// full scan of table 1 at snapshot 6; E, an unbounded read of column 1 at 7. Not dependents:
    /// C, the same full scan at snapshot 4 (before the write); D, a full scan of table 2; F, an
    /// unbounded read of column 0.
    #[test]
    fn an_unbounded_scan_covers_every_earlier_write_in_its_table_and_column() {
        let mut set = CaptureSet::new();
        write(&mut set, 1, vref(T1, 1, 5), 1, 3);
        scan(&mut set, 2, PredicateSummary::full_scan(T1, 0), 6);
        scan(&mut set, 3, PredicateSummary::full_scan(T1, 0), 4);
        scan(&mut set, 4, PredicateSummary::full_scan(T2, 0), 9);
        scan(&mut set, 5, summary(T1, Some(1), Bound::Unbounded, Bound::Unbounded), 7);
        scan(&mut set, 6, summary(T1, Some(0), Bound::Unbounded, Bound::Unbounded), 7);

        assert_eq!(halt(&set, 1), ids(&[2, 5]));
        assert!(set.index_is_consistent());
    }

    /// A writes 30 to column 1 at `begin_ts` 5. B read `[20, 50)` of column 1 at 6: covered. C read
    /// `[40, 50)`: not.
    #[test]
    fn a_bounded_range_is_checked_against_the_written_value() {
        let mut set = CaptureSet::new();
        write(&mut set, 1, vref(T1, 1, 5), 1, 30);
        scan(&mut set, 2, range1(20, 50), 6);
        scan(&mut set, 3, range1(40, 50), 6);

        assert_eq!(halt(&set, 1), ids(&[2]));
        assert!(set.index_is_consistent());
    }

    /// B reaches A through every kind of index entry at once — an exact read, a point, an unbounded
    /// scan and a range. Once B is removed, nothing of it may remain anywhere: a leftover entry
    /// would name a transaction the set no longer holds.
    #[test]
    fn forgetting_a_capture_leaves_nothing_of_it_in_any_index() {
        let mut set = CaptureSet::new();
        let v = vref(T1, 7, 5);
        write(&mut set, 1, v, 0, 7);
        write(&mut set, 1, v, 1, 30);
        exact_read(&mut set, 2, v, 6);
        names(&mut set, 2, 7, 6);
        scan(&mut set, 2, PredicateSummary::full_scan(T1, 0), 6);
        scan(&mut set, 2, range1(20, 50), 6);
        assert_eq!(halt(&set, 1), ids(&[2]));
        assert_eq!(set.indexed_txns(), [TxnId(2)].into_iter().collect::<BTreeSet<_>>());

        let removed = set.remove(&2).expect("B was retained");
        assert_eq!(removed.txn(), TxnId(2));
        assert!(halt(&set, 1).is_empty(), "B is gone, so nothing depends on A");
        assert!(set.indexed_txns().is_empty(), "left behind: {:?}", set.indexed_txns());
        assert!(set.index_is_consistent());

        // And a whole capture inserted at once is indexed like one built up read by read.
        set.insert(2, removed);
        assert_eq!(halt(&set, 1), ids(&[2]));
        assert!(set.index_is_consistent());
    }

    /// Every target and both modes, on one fixture mixing all four index kinds, two tables and a
    /// cycle (A and B each read a version the other wrote). The oracle is the full-graph join the
    /// walk replaced — a different implementation, not the subject.
    #[test]
    fn the_walk_agrees_with_the_full_graph_for_every_target_and_mode() {
        let mut set = CaptureSet::new();
        let (a1, b1, c1, d1) = (vref(T1, 1, 1), vref(T1, 2, 2), vref(T1, 3, 3), vref(T2, 1, 4));
        write(&mut set, 1, a1, 0, 1);
        write(&mut set, 1, a1, 1, 30);
        exact_read(&mut set, 2, a1, 2);
        write(&mut set, 2, b1, 0, 2);
        write(&mut set, 2, b1, 1, 60);
        exact_read(&mut set, 1, b1, 3);
        names(&mut set, 3, 2, 3);
        write(&mut set, 3, c1, 0, 3);
        write(&mut set, 3, c1, 1, 45);
        scan(&mut set, 4, summary(T1, Some(1), Bound::Included(int(40)), Bound::Unbounded), 4);
        write(&mut set, 4, d1, 0, 1);
        scan(&mut set, 5, PredicateSummary::full_scan(T2, 0), 5);
        scan(&mut set, 6, PredicateSummary::full_scan(T1, 0), 1);
        scan(&mut set, 7, summary(T1, Some(1), Bound::Unbounded, Bound::Unbounded), 9);

        for t in 1..=8u64 {
            for mode in [RevertMode::Halt, RevertMode::Cascade] {
                assert_eq!(
                    set.plan_revert(TxnId(t), mode).0,
                    set.plan_revert_by_full_graph(TxnId(t), mode),
                    "target {t}, {mode:?}"
                );
            }
        }
        // Not vacuous. Hand-derived: A→B (exact a1), A→G (unbounded col 1 over 30); B→A (exact b1,
        // the cycle), B→C (point id = 2), B→G, B→D (range [40, ∞) over 60); C→G, C→D (over 45);
        // D→E (full scan of table 2 over d1). F scanned table 1 at snapshot 1 and admits nothing.
        // So A reaches B, G at one hop, C, D at two and E at three.
        assert_eq!(halt(&set, 1), ids(&[2, 3, 4, 5, 7]));
        assert!(halt(&set, 6).is_empty(), "F wrote nothing");
        assert!(set.index_is_consistent());
    }

    #[test]
    #[should_panic(expected = "a capture must be keyed by its own transaction id")]
    fn inserting_under_a_key_that_is_not_the_captures_txn_refuses() {
        let mut set = CaptureSet::new();
        set.insert(5, TxnCapture::new(TxnId(6), ProvId::NONE, BranchId::TRUNK));
    }
}
