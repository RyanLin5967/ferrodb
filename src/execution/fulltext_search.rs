//! B8 — ranked retrieval: a bounded-heap top-k over BM25, read off the posting tree.
//!
//! The access path is `crate::storage::index_fulltext`'s prefix scan; this module is the part that
//! decides which of the rows it finds are the *best* ones, and how many of them a caller gets.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::execution::executor::Executor;
use crate::storage::heap_file_manager::{HeapFileManager, RecordId};
use crate::storage::index::BPlusTreeManager;
use crate::storage::index_fulltext::{distinct_tokens, indexed_text, open_posting_tree, postings_for_token, tokenize};
use crate::wal::txn::ReadView;
use crate::wal::visibility::resolve_visibility;

/// How many rows `SEARCH` returns when the statement does not say.
///
/// **The operator supplies its own bound, and this is it.** There is no `ORDER BY` and no `LIMIT` in
/// this SQL surface — `parser::unsupported_keyword` refuses both by name — so a ranked read cannot
/// take its bound from the query the way it would in Postgres. Ranking without a bound is also not
/// much of a feature: the point of scoring is that the first rows are the ones worth reading, and a
/// caller who gets all 4000 matches back in score order has paid for the ranking and still has to
/// truncate. So the bound exists whether or not the statement mentions it, and `TOP k` overrides it.
///
/// A query with more matches than this returns exactly this many rows, by design.
pub const DEFAULT_TOP_K: usize = 10;

/// BM25's term-frequency saturation. 1.2 is the usual default: raising it makes repeated terms count
/// closer to linearly, lowering it makes one occurrence nearly as good as five.
pub const BM25_K1: f64 = 1.2;
/// BM25's length-normalisation strength. 0.75 is the usual default; 0 would ignore document length
/// entirely and 1 would divide it out completely.
pub const BM25_B: f64 = 0.75;

/// `ln(1 + (N - df + 0.5) / (df + 0.5))` — BM25's inverse document frequency, in the form that
/// cannot go negative (the `1 +` is what Lucene added to the 1994 formula for exactly that reason).
///
/// **What `n_docs` is here, and what it is not.** It is the size of the *candidate set* for this
/// query — the visible rows holding at least one query term — not the number of rows in the table.
/// That is a deliberate choice with a stated consequence:
///
/// - it is **exact and cannot drift**: it is counted during this statement, so there is no stored
///   statistic to be stale, and nothing to maintain on `INSERT`/`DELETE`. A collection-size `N`
///   would need either a durable counter (which MVCC makes snapshot-dependent — a deleted row is
///   still a document to an older reader) or an `ANALYZE`-style estimate that is wrong between runs.
/// - it changes what idf *means*: how well a term discriminates **among the rows that matched**,
///   rather than against the whole corpus. For ordering a top-k list that is the quantity that
///   matters — the rows that matched nothing are not being ranked.
/// - for a **single-term** query every candidate contains that term, so `df == N`, idf is the same
///   for all of them, and the ranking falls back entirely to term frequency and length
///   normalisation. That is still a ranking, and it is why the multi-term case is what a
///   discrimination test has to use.
pub fn bm25_idf(n_docs: usize, df: usize) -> f64 {
    let n = n_docs as f64;
    let df = df as f64;
    (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
}

/// One term's contribution to a document's score.
///
/// `tf` and `dl` are **re-derived from the row that was just read**, never stored. That is not an
/// economy, it is the only way they can be trusted: postings are de-duplicated to one per
/// `(token, primary key)` — the E66 rule, without which a lookup returns the row twice — so a stored
/// term frequency could not be updated in place, and an `UPDATE` that changed the text would leave
/// the old count behind. Re-tokenizing the version the reader can actually see makes both quantities
/// exact for that reader by construction.
pub fn bm25_term_score(idf: f64, tf: usize, dl: usize, avgdl: f64) -> f64 {
    if tf == 0 {
        return 0.0;
    }
    let tf = tf as f64;
    let length_ratio = if avgdl > 0.0 { dl as f64 / avgdl } else { 1.0 };
    idf * (tf * (BM25_K1 + 1.0)) / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * length_ratio))
}

/// A scored row on its way through the heap.
struct Scored {
    score: f64,
    /// Primary key, used only to break score ties so a result is reproducible.
    pk: Value,
    rid: RecordId,
    row: Vec<Value>,
}

/// Rank order: higher score first, and on a tie the smaller primary key first.
///
/// The tiebreak is not cosmetic. Without it, two rows with equal scores come back in whatever order
/// the heap happens to hold them, which makes a top-k boundary non-deterministic — with `TOP 2` and
/// three equal scores, *which* row is dropped would vary between runs and no test could pin it.
fn rank(a: &Scored, b: &Scored) -> std::cmp::Ordering {
    a.score.total_cmp(&b.score).then_with(|| b.pk.cmp(&a.pk))
}

/// A bounded min-heap that keeps the best `k` rows it is offered.
///
/// The worst kept row sits at the root, so admitting a candidate is one comparison and the structure
/// never holds more than `k` rows however many candidates arrive. Selection is O(n log k) rather than
/// the O(n log n) of sorting everything and throwing most of it away.
struct BoundedTopK {
    k: usize,
    heap: Vec<Scored>,
}

impl BoundedTopK {
    fn new(k: usize) -> Self {
        Self { k, heap: Vec::new() }
    }

    fn offer(&mut self, item: Scored) {
        // `k == 0` is refused at parse time and again in `open`; keeping nothing is still the right
        // answer for it, and it must not be an index panic on `heap[0]`.
        if self.k == 0 {
            return;
        }
        if self.heap.len() < self.k {
            self.heap.push(item);
            self.sift_up(self.heap.len() - 1);
        } else if rank(&item, &self.heap[0]) == std::cmp::Ordering::Greater {
            self.heap[0] = item;
            self.sift_down(0);
        }
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / 2;
            if rank(&self.heap[i], &self.heap[parent]) == std::cmp::Ordering::Less {
                self.heap.swap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        loop {
            let (left, right) = (2 * i + 1, 2 * i + 2);
            let mut lowest = i;
            if left < self.heap.len() && rank(&self.heap[left], &self.heap[lowest]) == std::cmp::Ordering::Less {
                lowest = left;
            }
            if right < self.heap.len() && rank(&self.heap[right], &self.heap[lowest]) == std::cmp::Ordering::Less {
                lowest = right;
            }
            if lowest == i {
                return;
            }
            self.heap.swap(i, lowest);
            i = lowest;
        }
    }

    /// Best first.
    fn into_ranked(mut self) -> Vec<Scored> {
        self.heap.sort_by(|a, b| rank(b, a));
        self.heap
    }
}

/// What survived the candidate pass: enough to score with, plus the row to return.
struct Candidate {
    pk: Value,
    rid: RecordId,
    row: Vec<Value>,
    /// Tokens in the version this reader sees — the document length BM25 normalises by.
    doc_len: usize,
    /// Occurrences of each query term, positionally aligned with the term list.
    term_freqs: Vec<usize>,
}

/// The `SEARCH` operator: ranked, bounded retrieval over one full-text indexed column.
///
/// **Top-k is a blocking operator**, so the retrieval happens when the operator is opened rather
/// than lazily in `next`: the bound cannot be applied until every candidate has been scored, and
/// which rows are in the answer is not known until the last one has been seen. `next` then hands
/// back the ranked rows in order. Errors surface from `open`, where a caller can still tell the
/// difference between "no such index" and "no matches".
pub struct FullTextSearch {
    ranked: std::vec::IntoIter<(RecordId, Vec<Value>)>,
}

impl FullTextSearch {
    /// Retrieve the best `top_k` rows of `table` for `query`, by the full-text index on `column`.
    ///
    /// Each returned row is the table's columns followed by **one extra column: the BM25 score** as
    /// a `Float`. A ranked read whose ranking is invisible is a ranked read nobody can check, and
    /// this surface has no way to name a computed column, so the score rides at the end where its
    /// position is fixed and documented rather than inferred.
    pub fn open(
        catalog: &Catalog,
        table: &str,
        column: &str,
        query: &str,
        top_k: Option<usize>,
        bp: Arc<BufferPoolManager>,
        view: Arc<ReadView>,
    ) -> Result<Self, FerroError> {
        let entry = catalog.require_table(table)?;
        let schema: Schema = entry.schema.clone();
        let col_index = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| {
                FerroError::Bind(format!(
                    "cannot search '{table}.{column}': no such column. '{table}' has: {}",
                    schema.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ")
                ))
            })?;
        // Only a full-text index will do. A B-tree index on the same column orders whole values and
        // knows nothing about the words inside them, so falling back to one would answer a search
        // with an equality scan and no ranking at all.
        let ft_root = entry
            .fulltext_indexes
            .iter()
            .find(|i| i.column_name == column)
            .ok_or_else(|| {
                FerroError::Bind(format!(
                    "no full-text index on '{table}.{column}'; create one with \
                     CREATE FULLTEXT INDEX <name> ON {table} ({column});"
                ))
            })?
            .root_page_id;

        let k = top_k.unwrap_or(DEFAULT_TOP_K);
        if k == 0 {
            return Err(FerroError::Bind(
                "TOP must be at least 1; a bound of 0 rows would return nothing whatever the data says".into(),
            ));
        }

        // A query that tokenizes to nothing is refused rather than answered with an empty result:
        // `SEARCH docs (body) FOR '###';` and `SEARCH docs (body) FOR 'nonesuch';` are different
        // situations, and reporting both as "no rows" hides a query that never had a chance.
        let terms = distinct_tokens(query);
        if terms.is_empty() {
            return Err(FerroError::Bind(format!(
                "the search text {query:?} contains no tokens: it has no letters or digits to \
                 match, so there is nothing to look up"
            )));
        }

        let tree = open_posting_tree(ft_root, bp.clone());
        // SHARED root cell (D53); see optimizer.rs for why a private one here is a hazard.
        let primary_index = match catalog.root_cell(table, None) {
            Some(cell) => BPlusTreeManager::<Value, RecordId>::open_shared(cell, bp.clone()),
            None => BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone()),
        };
        let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
        let tt_heap = HeapFileManager::open(entry.time_travel_root, bp.clone());

        // The candidate set: read one posting list per term and union the primary keys. A `BTreeMap`
        // rather than a `HashMap` because `Value` is `Ord` and not `Hash`, and because iterating in
        // key order makes the scan — and so a tied result — reproducible.
        let mut candidate_pks: BTreeMap<Value, ()> = BTreeMap::new();
        for term in &terms {
            for pk in postings_for_token(&tree, term)? {
                candidate_pks.insert(pk, ());
            }
        }

        // Resolve every candidate exactly once. This is the `SecondaryIndexScan` flow — posting to
        // primary key to `RecordId` to heap tuple to visible version — plus the recheck that a
        // posting is only a claim about *some* version of the row.
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut doc_freqs = vec![0usize; terms.len()];
        let mut total_len = 0usize;
        for pk in candidate_pks.keys() {
            let rid = match primary_index.search(pk)? {
                Some(rid) => rid,
                // The primary entry is gone, so nothing can resolve this posting.
                None => continue,
            };
            let tuple = heap.read(rid)?;
            let visible = match resolve_visibility(&view, &tt_heap, tuple)? {
                Some(v) => v,
                // No version of this row is visible to this reader: a deleted row, or one written by
                // a transaction this snapshot cannot see. This is what makes leaving a dead posting
                // in place correct rather than merely cheap.
                None => continue,
            };
            let row = visible.deserialize(&schema)?;
            let Some(text) = indexed_text(&row[col_index])? else {
                // NULL holds no tokens, so it can only be here through a stale posting.
                continue;
            };

            let doc_tokens = tokenize(text);
            let term_freqs: Vec<usize> = terms
                .iter()
                .map(|t| doc_tokens.iter().filter(|d| *d == t).count())
                .collect();
            // **The recheck, and the reason an `UPDATE` may leave postings behind.** The tokens of
            // this row's *current visible* version are what count; a posting from a version whose
            // text no longer holds any query term is stale and its row is not a match. This is the
            // analogue of `SecondaryIndexScan`'s `if vals[col_index] != sec { continue }`, and
            // without it `UPDATE body = '...'` would leave the row findable by every word it used
            // to contain.
            if term_freqs.iter().all(|c| *c == 0) {
                continue;
            }

            for (i, count) in term_freqs.iter().enumerate() {
                if *count > 0 {
                    doc_freqs[i] += 1;
                }
            }
            total_len += doc_tokens.len();
            candidates.push(Candidate { pk: pk.clone(), rid, row, doc_len: doc_tokens.len(), term_freqs });
        }

        let n_docs = candidates.len();
        let avgdl = if n_docs == 0 { 0.0 } else { total_len as f64 / n_docs as f64 };
        let idfs: Vec<f64> = doc_freqs.iter().map(|df| bm25_idf(n_docs, *df)).collect();

        let mut top = BoundedTopK::new(k);
        for candidate in candidates {
            let mut score = 0.0;
            for (i, tf) in candidate.term_freqs.iter().enumerate() {
                score += bm25_term_score(idfs[i], *tf, candidate.doc_len, avgdl);
            }
            let mut row = candidate.row;
            row.push(Value::Float(score));
            top.offer(Scored { score, pk: candidate.pk, rid: candidate.rid, row });
        }

        let ranked: Vec<(RecordId, Vec<Value>)> =
            top.into_ranked().into_iter().map(|s| (s.rid, s.row)).collect();
        Ok(Self { ranked: ranked.into_iter() })
    }
}

impl Executor for FullTextSearch {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        self.ranked.next().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scored(score: f64, pk: i32) -> Scored {
        Scored { score, pk: Value::Integer(pk), rid: RecordId::new(pk as u32, 0), row: Vec::new() }
    }

    fn ranked_pks(k: usize, offers: &[(f64, i32)]) -> Vec<i32> {
        let mut top = BoundedTopK::new(k);
        for (score, pk) in offers {
            top.offer(scored(*score, *pk));
        }
        top.into_ranked()
            .into_iter()
            .map(|s| match s.pk {
                Value::Integer(i) => i,
                other => panic!("unexpected pk {other:?}"),
            })
            .collect()
    }

    /// Breaking shape: more candidates than the bound, offered worst-first and best-last. A heap
    /// that only ever kept the first `k` it saw would return 1,2 here.
    #[test]
    fn the_heap_keeps_the_best_k_whatever_order_they_arrive_in() {
        assert_eq!(ranked_pks(2, &[(0.1, 1), (0.2, 2), (9.0, 3), (5.0, 4)]), vec![3, 4]);
        assert_eq!(ranked_pks(2, &[(9.0, 3), (5.0, 4), (0.2, 2), (0.1, 1)]), vec![3, 4]);
        // best-last, worst-first, and a middle value that must displace exactly one row
        assert_eq!(ranked_pks(3, &[(1.0, 1), (2.0, 2), (3.0, 3), (2.5, 4)]), vec![3, 4, 2]);
    }

    /// Breaking shape: eight candidates and `k = 3`, i.e. enough for the heap to sift more than one
    /// level. A sift that only compared against the root would let a middling row survive.
    #[test]
    fn the_heap_agrees_with_a_full_sort() {
        let offers = [(0.5, 1), (7.5, 2), (2.5, 3), (9.5, 4), (1.5, 5), (8.5, 6), (3.5, 7), (6.5, 8)];
        assert_eq!(ranked_pks(3, &offers), vec![4, 6, 2]);
        // and with k at least the candidate count it is a plain descending sort
        assert_eq!(ranked_pks(8, &offers), vec![4, 6, 2, 8, 7, 3, 5, 1]);
    }

    /// Breaking shape: equal scores at the top-k boundary. Without the primary-key tiebreak, which
    /// row is dropped depends on heap layout and no test of a bounded result can be stable.
    #[test]
    fn ties_break_on_the_primary_key_so_the_boundary_is_deterministic() {
        assert_eq!(ranked_pks(2, &[(1.0, 7), (1.0, 3), (1.0, 5)]), vec![3, 5]);
        assert_eq!(ranked_pks(2, &[(1.0, 5), (1.0, 3), (1.0, 7)]), vec![3, 5]);
    }

    /// The bound is a bound: `k` rows out of many, and fewer than `k` when that is all there is.
    #[test]
    fn the_heap_never_returns_more_than_k_and_never_pads() {
        assert_eq!(ranked_pks(1, &[(1.0, 1), (2.0, 2), (3.0, 3)]).len(), 1);
        assert_eq!(ranked_pks(5, &[(1.0, 1), (2.0, 2)]).len(), 2);
        assert!(ranked_pks(5, &[]).is_empty());
        // k == 0 is refused before it reaches here; it must still not panic on heap[0]
        assert!(ranked_pks(0, &[(1.0, 1)]).is_empty());
    }

    /// idf must fall as a term gets commoner, and stay non-negative at the extreme where every
    /// document has it. Breaking shape: the classic `ln((N - df + 0.5)/(df + 0.5))` without the
    /// `1 +`, which goes negative once a term is in more than half the documents — that turns a
    /// common term into a *penalty* and can rank a document below one that matched nothing.
    #[test]
    fn idf_falls_with_document_frequency_and_never_goes_negative() {
        let rare = bm25_idf(100, 1);
        let common = bm25_idf(100, 50);
        let universal = bm25_idf(100, 100);
        assert!(rare > common, "rare {rare} should outweigh common {common}");
        assert!(common > universal, "common {common} should outweigh universal {universal}");
        assert!(universal >= 0.0, "idf must not go negative: {universal}");
        assert!(bm25_idf(1, 1) >= 0.0);
    }

    /// The two things a term score is supposed to do: rise with term frequency (saturating), and
    /// fall as the document gets longer than average.
    #[test]
    fn term_score_saturates_in_tf_and_penalises_length() {
        let idf = 1.0;
        let one = bm25_term_score(idf, 1, 10, 10.0);
        let two = bm25_term_score(idf, 2, 10, 10.0);
        let three = bm25_term_score(idf, 3, 10, 10.0);
        let ten = bm25_term_score(idf, 10, 10, 10.0);
        assert!(two > one, "two occurrences must beat one");
        assert!(ten > three && three > two);
        // Saturation is a falling *marginal* gain per extra occurrence — compared step against
        // step, because comparing a one-step rise with an eight-step one says nothing either way.
        assert!(three - two < two - one, "tf must saturate: {one} {two} {three}");
        // ...and a finite ceiling: however often a term appears, one term cannot score above
        // idf * (k1 + 1). A linear tf term would have no such bound.
        assert!(ten < idf * (BM25_K1 + 1.0), "tf must be bounded by idf*(k1+1): {ten}");

        let short = bm25_term_score(idf, 1, 5, 10.0);
        let long = bm25_term_score(idf, 1, 40, 10.0);
        assert!(short > long, "a shorter document with the same tf must score higher");

        assert_eq!(bm25_term_score(idf, 0, 10, 10.0), 0.0, "an absent term contributes nothing");
        // avgdl == 0 happens only when there are no documents; it must not divide by zero
        assert!(bm25_term_score(idf, 1, 10, 0.0).is_finite());
    }
}
