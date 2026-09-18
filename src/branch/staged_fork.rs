//! A fork's keys, held in memory until the branch's **first write**. (`SCALE-DESIGN.md` D6, opt 2)
//!
//! WHY. `TableBranchCatalog::fork` performed six B+tree mutations and then an fsync, measured at
//! 271 forks/sec serial and ~4,400 concurrent (`bench/fork_concurrency_after.txt`). But agents fork
//! SPECULATIVELY, and most branches are reaped without ever writing a row. **A branch that never
//! wrote need never have been durable at all**, so a speculative fork can cost no disk: not the
//! fsync, and not the tree mutations either.
//!
//! What makes that admissible is a premise check already in the design record: a fork lost to a
//! crash is observably identical to a branch reaped a moment later. Generations make a stale handle
//! a hard `BranchError::Reaped`, and reaping here is non-cooperative and lease-driven, so the
//! contract already permits the loss. What it does NOT permit is a *partial* fork surviving — an
//! orphan id, a page parked in the file, an index entry pointing at a record that is not there.
//! `tests/integration_fork_lazy_durability.rs` is the falsifier for exactly that, and it was
//! watched failing (512 forks parked 131,072 bytes) before this module existed.
//!
//! ⛔ WHY THE STAGE IS AT THE **KEY** LAYER AND NOT THE RECORD LAYER. The obvious shape is a
//! `HashMap<BranchId, BranchRecord>` consulted by `get`. It is wrong, and the reason is worth
//! writing down: a record overlay has to be re-implemented in every one of the seventeen trait
//! methods — the state span, the deadline span, the child span, the envelope span and the FREE_ID
//! span each answer a different question about the same branch, and a method that forgets the
//! overlay does not fail loudly, it silently reports a live branch as absent. A key-level stage is
//! consulted in TWO places — `kv_search` and `kv_range` — and every query above is built out of
//! those two. The keys staged are byte-for-byte the keys `fork` used to write, because the same
//! `write_record_new` produces them; only the sink differs.
//!
//! It is also not a new pattern in this repo. `cow::WriteBuffer` is a per-branch in-memory buffer
//! of key writes, probed before B+tree descent, and its docstring already says why: "a branch that
//! dies before flushing allocates zero pages — the common case for an abandoned agent task". That
//! is this idea, applied to the branch's DATA. The catalog was the one place still paying eagerly.
//! Its `WriteBufferEntry` is reused here rather than a second spelling of Put/Delete.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use crate::cow::WriteBufferEntry;
use crate::error::FerroError;

/// One pending fork: the keys it staged, and the facts needed to land or drop them.
struct StagedBranch {
    /// Staged keys in the order `fork` wrote them, de-duplicated. Materialising replays them in
    /// this order, which is the order the eager path used.
    keys: Vec<Vec<u8>>,
    /// The parent's id slot. Materialising a child must materialise its pending ancestors first:
    /// a record whose `parent_id` names a branch with no record is a dangling pointer, and the
    /// child key under that parent would be an index entry over nothing.
    parent: u64,
    /// The generation this branch holds, so discarding it can retire the slot at the next one.
    generation: u32,
    /// The id came from `next_id` rather than from the FREE_ID span, so it owns **no** tree keys
    /// at all and can go back to the in-memory pool when discarded.
    minted: bool,
}

/// Every pending fork in one catalog.
///
/// Lock order is `TableBranchCatalog::logical` then this. Nothing here ever reads the tree, so it
/// cannot invert.
#[derive(Default)]
pub(crate) struct StagedForks {
    /// Every staged key, in key order, so a range query merges against the tree in one pass.
    /// The `u64` is the owning branch, so materialising one branch moves exactly its own keys.
    by_key: BTreeMap<Vec<u8>, (u64, WriteBufferEntry)>,
    by_branch: HashMap<u64, StagedBranch>,
    /// Slots whose pending occupant was discarded, and the generation the next occupant must take.
    ///
    /// **This is what keeps a discarded handle a hard error.** A reaped branch normally leaves its
    /// record behind with `generation + 1`, and a recycled slot reads its generation from there. A
    /// branch that was never durable leaves no record, so the bump has nowhere to live but here.
    /// Bounded by "slots discarded and not yet re-forked", which the fork/reap agent workload
    /// keeps at roughly the number of free slots — the same quantity the FREE_ID span holds for
    /// branches that did write.
    retired: HashMap<u64, u32>,
    /// Ids minted from `next_id` whose branch was discarded. Reusing one costs no disk whatsoever,
    /// because the slot has no tree keys to clean up.
    free_pool: Vec<u64>,
}

impl StagedForks {
    /// Probe before descending. `None` means "not staged, ask the tree" — it does **not** mean the
    /// key is absent. `Some(Delete)` does.
    pub(crate) fn probe(&self, key: &[u8]) -> Option<&WriteBufferEntry> {
        self.by_key.get(key).map(|(_, e)| e)
    }

    /// The staged keys in `[lo, hi)`, cloned.
    ///
    /// A snapshot rather than a borrow because the caller merges it with a LAZY tree scanner and
    /// must not hold this lock across a page read. Taking it BEFORE the tree iterator is created
    /// is deliberate: a branch that materialises in the window is then either in the snapshot and
    /// in the tree (deduplicated below, staged wins, same bytes) or in neither list twice. Taking
    /// it afterwards would allow a branch to appear in neither.
    pub(crate) fn snapshot_range(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
    ) -> Vec<(Vec<u8>, WriteBufferEntry)> {
        self.by_key
            .range::<[u8], _>((lo, hi))
            .map(|(k, (_, e))| (k.clone(), e.clone()))
            .collect()
    }

    /// Begin staging a fork. The branch must not already be pending.
    pub(crate) fn open(&mut self, branch: u64, parent: u64, generation: u32, minted: bool) {
        let prev =
            self.by_branch.insert(branch, StagedBranch { keys: Vec::new(), parent, generation, minted });
        debug_assert!(prev.is_none(), "branch {branch} was staged twice");
        self.retired.remove(&branch);
    }

    /// Stage one key write against a pending branch.
    pub(crate) fn put(&mut self, branch: u64, key: Vec<u8>, entry: WriteBufferEntry) {
        let Some(st) = self.by_branch.get_mut(&branch) else {
            debug_assert!(false, "staged a key against branch {branch}, which is not pending");
            return;
        };
        if self.by_key.insert(key.clone(), (branch, entry)).is_none() {
            st.keys.push(key);
        }
    }

    pub(crate) fn is_pending(&self, branch: u64) -> bool {
        self.by_branch.contains_key(&branch)
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.by_branch.len()
    }

    /// The pending ancestors of `branch`, outermost first, ending with `branch` itself. Empty if
    /// `branch` is not pending.
    ///
    /// Terminates on a cycle rather than looping: a parent chain is built only by `fork`, which
    /// always names an already-existing parent, so a cycle is impossible — and an impossible loop
    /// that hangs the database is worse than one that returns a short answer.
    pub(crate) fn ancestors_first(&self, branch: u64) -> Vec<u64> {
        let mut chain = Vec::new();
        let mut cur = branch;
        while let Some(st) = self.by_branch.get(&cur) {
            if chain.contains(&cur) {
                break;
            }
            chain.push(cur);
            cur = st.parent;
        }
        chain.reverse();
        chain
    }

    /// Remove `branch` from the stage and hand back its writes, in order, for the tree.
    /// The `bool` is `minted`: its keys are provably new, so they can be inserted rather than
    /// upserted (D10 — the wasted delete is 56% of an upsert).
    pub(crate) fn take(&mut self, branch: u64) -> Option<(Vec<(Vec<u8>, WriteBufferEntry)>, bool)> {
        let st = self.by_branch.remove(&branch)?;
        let ops = st
            .keys
            .iter()
            .filter_map(|k| self.by_key.remove(k).map(|(_, e)| (k.clone(), e)))
            .collect();
        Some((ops, st.minted))
    }

    /// Drop `branch` entirely: it was never durable, so there is nothing to undo.
    ///
    /// `generation` is the generation the slot must NOT hand out again — `mark_reaped` has already
    /// bumped it on the record being discarded.
    pub(crate) fn discard(&mut self, branch: u64, generation: u32) {
        let Some(st) = self.by_branch.remove(&branch) else { return };
        for k in &st.keys {
            self.by_key.remove(k);
        }
        let retire = generation.max(st.generation.saturating_add(1));
        self.retired.insert(branch, retire);
        if st.minted && !self.free_pool.contains(&branch) {
            self.free_pool.push(branch);
        }
    }

    /// Remove one staged key, whoever owns it. Used by `detach_child`, which names a key rather
    /// than a branch. Returns whether anything was staged under it.
    pub(crate) fn remove_key(&mut self, key: &[u8]) -> bool {
        let Some((owner, _)) = self.by_key.remove(key) else { return false };
        if let Some(st) = self.by_branch.get_mut(&owner) {
            st.keys.retain(|k| k.as_slice() != key);
        }
        true
    }

    /// The generation a slot must be re-minted at, if a pending occupant of it was discarded.
    pub(crate) fn retired_generation(&self, id: u64) -> Option<u32> {
        self.retired.get(&id).copied()
    }

    /// An id whose pending branch was discarded. It has no tree keys, so reusing it writes nothing.
    pub(crate) fn take_free_id(&mut self) -> Option<u64> {
        self.free_pool.pop()
    }
}

/// A lazy merge of the tree's range scanner with a staged snapshot of the same range.
///
/// Staged wins on an equal key — it is the newer value — and `Delete` suppresses the tree's entry
/// rather than yielding it. Streaming rather than collecting, because `BranchCatalog::scan` is
/// documented to hold one record at a time and not a second copy of the catalog.
pub(crate) struct Merged<I> {
    tree: I,
    staged: std::vec::IntoIter<(Vec<u8>, WriteBufferEntry)>,
    peek_tree: Option<(Vec<u8>, Vec<u8>)>,
    peek_staged: Option<(Vec<u8>, WriteBufferEntry)>,
    pending_err: Option<FerroError>,
    tree_done: bool,
}

impl<I> Merged<I>
where
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>), FerroError>>,
{
    pub(crate) fn new(tree: I, staged: Vec<(Vec<u8>, WriteBufferEntry)>) -> Self {
        Merged {
            tree,
            staged: staged.into_iter(),
            peek_tree: None,
            peek_staged: None,
            pending_err: None,
            tree_done: false,
        }
    }

    fn fill(&mut self) {
        if self.peek_tree.is_none() && !self.tree_done && self.pending_err.is_none() {
            match self.tree.next() {
                None => self.tree_done = true,
                Some(Ok(kv)) => self.peek_tree = Some(kv),
                Some(Err(e)) => self.pending_err = Some(e),
            }
        }
        if self.peek_staged.is_none() {
            self.peek_staged = self.staged.next();
        }
    }
}

impl<I> Iterator for Merged<I>
where
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>), FerroError>>,
{
    type Item = Result<(Vec<u8>, Vec<u8>), FerroError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(e) = self.pending_err.take() {
                return Some(Err(e));
            }
            self.fill();
            // A staged key at or below the tree's next key is the one to consider; the tree's is
            // shadowed when they are equal.
            let take_staged = match (&self.peek_tree, &self.peek_staged) {
                (_, None) => false,
                (None, Some(_)) => true,
                (Some((tk, _)), Some((sk, _))) => sk <= tk,
            };
            if take_staged {
                let (sk, entry) = self.peek_staged.take().expect("peeked");
                if self.peek_tree.as_ref().is_some_and(|(tk, _)| *tk == sk) {
                    self.peek_tree = None;
                }
                match entry {
                    WriteBufferEntry::Put(v) => return Some(Ok((sk, v))),
                    // Staged as deleted: the tree's entry, if any, has just been dropped above.
                    WriteBufferEntry::Delete => continue,
                }
            }
            match self.peek_tree.take() {
                Some(kv) => return Some(Ok(kv)),
                None if self.pending_err.is_some() => continue,
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn tree(items: &[(&str, &str)]) -> std::vec::IntoIter<Result<(Vec<u8>, Vec<u8>), FerroError>> {
        items
            .iter()
            .map(|(a, b)| Ok((k(a), k(b))))
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn drain<I>(m: Merged<I>) -> Vec<(String, String)>
    where
        I: Iterator<Item = Result<(Vec<u8>, Vec<u8>), FerroError>>,
    {
        m.map(|e| {
            let (a, b) = e.unwrap();
            (String::from_utf8(a).unwrap(), String::from_utf8(b).unwrap())
        })
        .collect()
    }

    #[test]
    fn staged_keys_interleave_with_the_tree_in_key_order() {
        let m = Merged::new(
            tree(&[("a", "1"), ("c", "3"), ("e", "5")]),
            vec![
                (k("b"), WriteBufferEntry::Put(k("2"))),
                (k("f"), WriteBufferEntry::Put(k("6"))),
            ],
        );
        assert_eq!(
            drain(m),
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("c".into(), "3".into()),
                ("e".into(), "5".into()),
                ("f".into(), "6".into()),
            ]
        );
    }

    #[test]
    fn a_staged_value_shadows_the_trees_value_for_the_same_key() {
        let m = Merged::new(
            tree(&[("a", "old"), ("b", "keep")]),
            vec![(k("a"), WriteBufferEntry::Put(k("new")))],
        );
        assert_eq!(drain(m), vec![("a".into(), "new".into()), ("b".into(), "keep".into())]);
    }

    /// The FREE_ID span depends on this: a slot claimed by a pending fork must not be handed to a
    /// second one, and the only thing saying so is a staged `Delete` over a key the tree still has.
    #[test]
    fn a_staged_delete_suppresses_the_trees_entry() {
        let m = Merged::new(
            tree(&[("a", "1"), ("b", "2"), ("c", "3")]),
            vec![(k("b"), WriteBufferEntry::Delete)],
        );
        assert_eq!(drain(m), vec![("a".into(), "1".into()), ("c".into(), "3".into())]);
    }

    #[test]
    fn a_staged_delete_of_a_key_the_tree_never_had_yields_nothing() {
        let m = Merged::new(tree(&[("a", "1")]), vec![(k("z"), WriteBufferEntry::Delete)]);
        assert_eq!(drain(m), vec![("a".into(), "1".into())]);
    }

    /// A page read that fails mid-scan must reach the caller. The merge holds one tree item ahead
    /// of the caller, so the error can arrive one position later than the scanner produced it —
    /// what matters, and what is asserted, is that it is never SWALLOWED, because `ids_in_span`
    /// and `scan` both `?` on it and a dropped error would silently shorten a catalog listing.
    #[test]
    fn a_tree_error_is_surfaced_and_not_swallowed() {
        let items: Vec<Result<(Vec<u8>, Vec<u8>), FerroError>> =
            vec![Ok((k("a"), k("1"))), Err(FerroError::KeyNotFound)];
        let m = Merged::new(items.into_iter(), vec![(k("b"), WriteBufferEntry::Put(k("2")))]);
        let got: Vec<_> = m.collect();
        assert_eq!(got.iter().filter(|r| r.is_err()).count(), 1, "the error must reach the caller");
        assert_eq!(got.iter().filter(|r| r.is_ok()).count(), 2, "and must not eat the good rows");
    }

    #[test]
    fn discarding_a_pending_branch_retires_its_slot_at_the_next_generation() {
        let mut s = StagedForks::default();
        s.open(7, 0, 3, true);
        s.put(7, k("rec7"), WriteBufferEntry::Put(k("x")));
        assert!(s.is_pending(7));
        s.discard(7, 4);
        assert!(!s.is_pending(7));
        assert_eq!(s.probe(b"rec7"), None, "a discarded branch leaves no staged key");
        assert_eq!(s.retired_generation(7), Some(4));
        assert_eq!(s.take_free_id(), Some(7), "a minted id goes back to the pool");
        assert_eq!(s.take_free_id(), None);
    }

    /// A recycled slot owns tree keys, so it must NOT go into the pool — the pool is the
    /// "costs no disk" path and only a minted id qualifies.
    #[test]
    fn discarding_a_recycled_slot_does_not_put_it_in_the_free_pool() {
        let mut s = StagedForks::default();
        s.open(9, 0, 1, false);
        s.discard(9, 2);
        assert_eq!(s.take_free_id(), None);
        assert_eq!(s.retired_generation(9), Some(2));
    }

    #[test]
    fn ancestors_come_back_outermost_first_and_stop_at_the_first_durable_one() {
        let mut s = StagedForks::default();
        s.open(1, 0, 0, true); // parent 0 (trunk) is durable
        s.open(2, 1, 0, true);
        s.open(3, 2, 0, true);
        assert_eq!(s.ancestors_first(3), vec![1, 2, 3]);
        assert_eq!(s.ancestors_first(1), vec![1]);
        assert_eq!(s.ancestors_first(99), Vec::<u64>::new());
    }

    #[test]
    fn taking_a_branch_returns_its_writes_in_the_order_they_were_staged() {
        let mut s = StagedForks::default();
        s.open(4, 0, 0, true);
        s.put(4, k("z"), WriteBufferEntry::Put(k("1")));
        s.put(4, k("a"), WriteBufferEntry::Put(k("2")));
        s.put(4, k("z"), WriteBufferEntry::Put(k("3"))); // overwrite keeps the first position
        let (ops, minted) = s.take(4).expect("pending");
        assert!(minted);
        assert_eq!(ops, vec![
            (k("z"), WriteBufferEntry::Put(k("3"))),
            (k("a"), WriteBufferEntry::Put(k("2"))),
        ]);
        assert!(!s.is_pending(4));
        assert_eq!(s.probe(b"z"), None);
    }
}
