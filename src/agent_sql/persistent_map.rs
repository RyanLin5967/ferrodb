//! An immutable ordered map with structural sharing: clone is O(1), a write copies one path.
//!
//! # Why this exists
//!
//! `AgentRuntime::begin_session_as` forks a child agent task off a parent that may itself be a
//! live task holding staged, uncommitted rows. The child's visible state **is** the parent's
//! state at fork time, so the fork used to deep-clone the parent's five workspace maps. That is
//! correct and it is O(W) in the parent's working set — which multiplies by fanout, so N children
//! of one parent cost O(N·W). Measured before the change (`bench/d27_fork_workspace_cost_*.txt`):
//! 1024 children of a parent holding 4000 staged rows copied 4.1M rows and cost 1651 MB of
//! resident memory, dead-linear in both N and W.
//!
//! Replacing the `BTreeMap` with this structure makes a fork an `Arc` pointer bump and leaves the
//! copying to the writes that actually happen: a child that writes one row copies the ~log₂(W)
//! nodes on that row's root-to-leaf path and shares every other subtree with its parent and its
//! siblings.
//!
//! # What it must NOT change
//!
//! Two properties of the deep clone are load-bearing and this structure keeps both, for a
//! stronger reason than the clone had:
//!
//! 1. **The parent's later writes stay invisible to the child.** Under the deep clone this held
//!    because the child owned a separate copy. Here it holds because the nodes are *immutable*:
//!    a parent write allocates new nodes on its own path and rebinds the parent's root, and no
//!    node the child can reach is ever mutated. The child's root still addresses the fork-point
//!    tree. Nothing can write through a shared node, because there is no `&mut` path to one —
//!    `Arc` is never `make_mut`'d here, and that is the invariant to preserve if this file is
//!    edited.
//!
//! 2. **A read never walks the parent chain.** There is no parent pointer to walk. A lookup is a
//!    descent of the child's own tree, O(log W), and its cost does not depend on how deep the
//!    child sits in the fork chain or on whether its ancestors are still alive.
//!
//!    Sharing structure is *not* the overlay pattern this rules out. The difference is that the
//!    shared thing is addressed directly — the child holds the root — rather than searched for by
//!    walking up a chain of ancestors, so the cost does not grow with depth and an ancestor's
//!    death does not take the child's rows with it.
//!
//!    (The rule itself is stated at the fork site in `AgentRuntime::begin_session_as`, which
//!    attributes it to `DESIGN.md`. **That file is cited 92 times across `src/` and does not
//!    exist** — not in this worktree, not in the main checkout, and not anywhere in git history.
//!    The fork-site comment is therefore the only statement of the rule actually available to
//!    read, and it is what these two properties were taken from.)
//!
//! # The structure
//!
//! A weight-balanced binary search tree (Adams 1993; Okasaki, *Purely Functional Data
//! Structures*, 1998), made persistent by path copying. Nodes are immutable and shared behind
//! `Arc`; inserting rebuilds only the nodes from the root to the insertion point and clones the
//! rest as pointers.
//!
//! # What lost, and why
//!
//! **Bagwell's HAMT (2001)** is the other standard answer, and it has a shallower tree. It loses
//! on iteration order: a HAMT iterates in hash order, and workspace iteration order is
//! *observable* — `DIFF` emits `ChangeSet` rows in workspace order, `merge` builds `row_outcomes`
//! in workspace order, and `blind_writes_of` returns a `Vec` in workspace order. Reordering
//! user-visible output as a side effect of a memory fix is not a trade worth making, and an
//! ordered map keeps the existing semantics exactly.
//!
//! **`Arc<BTreeMap>` with `Arc::make_mut` — clone-on-first-write.** Ten lines, no new data
//! structure, and the fork really does become O(1). It loses on the workload: the first write
//! through *any* handle deep-clones the whole map, so N children that each write one row still
//! cost O(N·W). It defers the copy rather than removing it, and a child is forked precisely
//! because it is going to write. It would have measured beautifully on a fork-only benchmark and
//! changed nothing real — which is the trap this whole row exists inside.
//!
//! **A parent link, with reads walking the chain (the overlay pattern).** O(1) fork, O(1) write,
//! no copying at all. It loses on correctness, not performance: it breaks both properties above
//! at once. The parent's later writes become visible to the child, and every read walks the
//! ancestor chain — which is the one pattern the fork site rules out outright, and which
//! `tests/d27_fork_shares_without_leaking.rs` now tests against directly.
//!
//! **An immutable sorted `Vec` behind an `Arc`, binary-searched.** O(1) fork, O(log n) read, and
//! the most compact representation of any option here. It loses on writes: inserting into a sorted
//! vector copies it, so every staged row costs an O(W) memmove and staging a statement of W rows
//! costs O(W²). It moves the wall from fork to write rather than removing it.
//!
//! The payload sits behind `Arc<(K, V)>` so path copying moves pointers rather than deep-copying
//! keys and values: rebuilding a path costs O(log n) *pointer* clones, not O(log n) row clones.
//! That matters — the values here are `RowState`, which owns a `Vec<Value>`.
//!
//! # Deliberately not implemented
//!
//! There is no `remove`. No workspace map ever removes a key: `rows` records a delete as
//! `RowState::Deleted` rather than by removing the entry, and `base_rows`, `tables` and
//! `base_shapes` are all first-touch-wins. Weight-balanced deletion is the fiddly half of the
//! structure, and shipping an untested `remove` that nothing calls would be a defect waiting for
//! its first caller rather than a feature.

use std::borrow::Borrow;
use std::sync::Arc;

/// Adams' balance parameters, as used by Haskell's `Data.Map` and MIT Scheme's weight-balanced
/// trees. `DELTA` is how lopsided a node may be before it rotates; `RATIO` decides single versus
/// double rotation.
const DELTA: usize = 3;
const RATIO: usize = 2;

type Link<K, V> = Option<Arc<Node<K, V>>>;

struct Node<K, V> {
    /// Behind an `Arc` so that rebuilding a path clones a pointer per node instead of a key and a
    /// value per node.
    entry: Arc<(K, V)>,
    /// Number of entries in this subtree, including this node. O(1) `len`, and the balance
    /// decisions below read it.
    size: usize,
    left: Link<K, V>,
    right: Link<K, V>,
}

// Hand-written rather than derived: `#[derive(Clone)]` would demand `K: Clone, V: Clone`, which
// this structure genuinely does not need — every field is an `Arc`.
impl<K, V> Clone for Node<K, V> {
    fn clone(&self) -> Self {
        Node {
            entry: Arc::clone(&self.entry),
            size: self.size,
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

fn size<K, V>(l: &Link<K, V>) -> usize {
    l.as_ref().map_or(0, |n| n.size)
}

/// A node from parts, with its size recomputed. Every constructor below goes through this, so a
/// size can never drift from the subtree it describes.
fn node<K, V>(entry: Arc<(K, V)>, left: Link<K, V>, right: Link<K, V>) -> Link<K, V> {
    let size = 1 + size(&left) + size(&right);
    Some(Arc::new(Node { entry, size, left, right }))
}

/// Rebuild a node, rotating if one side has outgrown the other by more than `DELTA`.
///
/// Called only where one side has changed by at most one entry, which is the precondition Adams'
/// rebalancing assumes: a single insert cannot make a balanced tree need more than one rotation
/// at each level on the way back up.
fn balance<K, V>(entry: Arc<(K, V)>, left: Link<K, V>, right: Link<K, V>) -> Link<K, V> {
    let (ls, rs) = (size(&left), size(&right));
    if ls + rs <= 1 {
        return node(entry, left, right);
    }
    if rs > DELTA * ls {
        // Right-heavy. `right` is non-empty: rs > DELTA*ls >= 0 and ls+rs > 1.
        let r = right.expect("right-heavy implies a right child");
        if size(&r.left) < RATIO * size(&r.right) {
            // single left
            node(Arc::clone(&r.entry), node(entry, left, r.left.clone()), r.right.clone())
        } else {
            // double left
            let rl = r.left.as_ref().expect("double rotation implies a right-left grandchild");
            node(
                Arc::clone(&rl.entry),
                node(entry, left, rl.left.clone()),
                node(Arc::clone(&r.entry), rl.right.clone(), r.right.clone()),
            )
        }
    } else if ls > DELTA * rs {
        let l = left.expect("left-heavy implies a left child");
        if size(&l.right) < RATIO * size(&l.left) {
            // single right
            node(Arc::clone(&l.entry), l.left.clone(), node(entry, l.right.clone(), right))
        } else {
            // double right
            let lr = l.right.as_ref().expect("double rotation implies a left-right grandchild");
            node(
                Arc::clone(&lr.entry),
                node(Arc::clone(&l.entry), l.left.clone(), lr.left.clone()),
                node(entry, lr.right.clone(), right),
            )
        }
    } else {
        node(entry, left, right)
    }
}

/// An immutable ordered map. Cloning is O(1) and shares structure with the original.
pub struct PersistentMap<K, V> {
    root: Link<K, V>,
}

/// **The whole point.** A clone is one `Option<Arc>` copy — an atomic refcount bump, no traversal,
/// no allocation, and no dependence on how many entries the map holds.
impl<K, V> Clone for PersistentMap<K, V> {
    fn clone(&self) -> Self {
        PersistentMap { root: self.root.clone() }
    }
}

impl<K, V> Default for PersistentMap<K, V> {
    fn default() -> Self {
        PersistentMap { root: None }
    }
}

impl<K, V> PersistentMap<K, V> {
    pub fn new() -> Self {
        PersistentMap { root: None }
    }

    pub fn len(&self) -> usize {
        size(&self.root)
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        let mut it = Iter { stack: Vec::new(), remaining: self.len() };
        it.push_left_spine(&self.root);
        it
    }

    /// Keys in ascending order, like `BTreeMap::keys`.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(k, _)| k)
    }
}

impl<K: Ord, V> PersistentMap<K, V> {
    /// Entries with `lo <= key <= hi`, in ascending order — `BTreeMap::range(lo..=hi)`.
    ///
    /// **D57.** The workspace map is keyed `(table_id, row_id)` and the read path used to iterate
    /// the WHOLE map per statement and skip the other tables' entries one by one — O(W) in every
    /// staged row on the branch, for a question about one table. A subtree whose keys all sit
    /// below `lo` is never entered, and the walk stops at the first key past `hi`, so this costs
    /// O(log W + k) for the k entries in range: the same bound `BTreeMap::range` gives.
    ///
    /// Inclusive on both ends because the one caller wants a prefix, `(t, 0)..=(t, u64::MAX)`,
    /// and an exclusive upper bound would have to invent a key past it.
    pub fn range<'a, Q>(&'a self, lo: &'a Q, hi: &'a Q) -> Range<'a, K, V, Q>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut it = Range { stack: Vec::new(), hi };
        it.push_left_spine_from(&self.root, lo);
        it
    }
}

/// See [`PersistentMap::range`].
pub struct Range<'a, K, V, Q: ?Sized> {
    stack: Vec<&'a Node<K, V>>,
    hi: &'a Q,
}

impl<'a, K: Ord + Borrow<Q>, V, Q: Ord + ?Sized> Range<'a, K, V, Q> {
    /// Like `Iter::push_left_spine`, but a node below `lo` is skipped along with its entire left
    /// subtree (everything there is smaller still), and only its right child is considered.
    fn push_left_spine_from(&mut self, mut link: &'a Link<K, V>, lo: &Q) {
        while let Some(n) = link {
            if n.entry.0.borrow() < lo {
                link = &n.right;
            } else {
                self.stack.push(n);
                link = &n.left;
            }
        }
    }

    fn push_left_spine(&mut self, mut link: &'a Link<K, V>) {
        while let Some(n) = link {
            self.stack.push(n);
            link = &n.left;
        }
    }
}

impl<'a, K: Ord + Borrow<Q>, V, Q: Ord + ?Sized> Iterator for Range<'a, K, V, Q> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let n = self.stack.pop()?;
        if n.entry.0.borrow() > self.hi {
            // In-order, so nothing after this is in range either.
            self.stack.clear();
            return None;
        }
        // Everything in `n.right` is >= n's key >= lo, so the plain spine push is right here.
        self.push_left_spine(&n.right);
        Some((&n.entry.0, &n.entry.1))
    }
}

impl<K: Ord, V> PersistentMap<K, V> {
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // Iterative, not recursive: a descent is the hot read path and it should not grow a stack
        // frame per level.
        let mut cur = &self.root;
        while let Some(n) = cur {
            match key.cmp(n.entry.0.borrow()) {
                std::cmp::Ordering::Less => cur = &n.left,
                std::cmp::Ordering::Greater => cur = &n.right,
                std::cmp::Ordering::Equal => return Some(&n.entry.1),
            }
        }
        None
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Insert, replacing any existing value. The receiver's root is rebound to a new tree that
    /// shares every subtree the new path did not touch.
    ///
    /// `&mut self` rebinds this handle's root only. Any other handle cloned from this one keeps
    /// the root it had, which is what makes a forked child immune to its parent's later writes.
    pub fn insert(&mut self, key: K, val: V) {
        self.root = ins(&self.root, key, val, true);
    }

    /// Insert only if the key is absent; an existing entry is left exactly as it stands.
    ///
    /// This is `BTreeMap::entry(k).or_insert(v)` with the returned reference dropped, which is how
    /// both first-touch-wins call sites used it. First-touch-wins is a correctness property here,
    /// not an optimisation: `base_rows` and `base_shapes` record the *fork point*, and a later
    /// write that moved one would silently redefine what the merge diffs against.
    pub fn insert_if_absent(&mut self, key: K, val: V) {
        self.root = ins(&self.root, key, val, false);
    }
}

/// Same allocation, by pointer. Used to notice that a recursive insert changed nothing, so the
/// path above it need not be rebuilt either.
fn same<K, V>(a: &Link<K, V>, b: &Link<K, V>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => Arc::ptr_eq(x, y),
        (None, None) => true,
        _ => false,
    }
}

/// Returns the new subtree. `replace` false leaves an existing key's value alone.
///
/// When the recursion comes back with the subtree it was given — an absent-only insert of a key
/// that is already there — this returns the *same* link rather than rebuilding the path above it.
/// That is not a micro-optimisation: `stage_all` calls `insert_if_absent` on `base_rows` once per
/// row per statement, so every re-touch of an already-staged row would otherwise copy a path and
/// unshare it from the parent, for no change at all.
fn ins<K: Ord, V>(link: &Link<K, V>, key: K, val: V, replace: bool) -> Link<K, V> {
    let Some(n) = link else {
        return node(Arc::new((key, val)), None, None);
    };
    match key.cmp(&n.entry.0) {
        std::cmp::Ordering::Less => {
            let left = ins(&n.left, key, val, replace);
            if same(&left, &n.left) {
                return link.clone();
            }
            balance(Arc::clone(&n.entry), left, n.right.clone())
        }
        std::cmp::Ordering::Greater => {
            let right = ins(&n.right, key, val, replace);
            if same(&right, &n.right) {
                return link.clone();
            }
            balance(Arc::clone(&n.entry), n.left.clone(), right)
        }
        std::cmp::Ordering::Equal => {
            if replace {
                // No rebalance: the shape is unchanged, only this node's payload.
                node(Arc::new((key, val)), n.left.clone(), n.right.clone())
            } else {
                // Absent-only and the key is present: hand back the *same* subtree, so a
                // no-op insert allocates nothing and copies no path at all.
                link.clone()
            }
        }
    }
}

/// In-order iteration, yielding `(&K, &V)` in ascending key order — the same order `BTreeMap`
/// iteration gave, which several callers' output ordering depends on.
pub struct Iter<'a, K, V> {
    stack: Vec<&'a Node<K, V>>,
    remaining: usize,
}

impl<'a, K, V> Iter<'a, K, V> {
    fn push_left_spine(&mut self, mut link: &'a Link<K, V>) {
        while let Some(n) = link {
            self.stack.push(n);
            link = &n.left;
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let n = self.stack.pop()?;
        self.push_left_spine(&n.right);
        self.remaining -= 1;
        Some((&n.entry.0, &n.entry.1))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a, K, V> IntoIterator for &'a PersistentMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;
    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for PersistentMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut m = PersistentMap::new();
        for (k, v) in iter {
            m.insert(k, v);
        }
        m
    }
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for PersistentMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Every structural invariant, checked over the whole tree: ordering, the recorded subtree
    /// sizes, and the weight balance. A size that drifted or a rotation that lost a node would
    /// make `len` lie and make lookups miss, and neither shows up in a small smoke test.
    fn check<K: Ord + std::fmt::Debug, V>(link: &Link<K, V>) -> usize {
        let Some(n) = link else { return 0 };
        let (ls, rs) = (check(&n.left), check(&n.right));
        assert_eq!(n.size, 1 + ls + rs, "recorded size disagrees with the subtree");
        if ls + rs > 1 {
            assert!(
                ls <= DELTA * rs && rs <= DELTA * ls,
                "weight balance violated: left={ls} right={rs}"
            );
        }
        if let Some(l) = &n.left {
            assert!(l.entry.0 < n.entry.0, "left child out of order");
        }
        if let Some(r) = &n.right {
            assert!(n.entry.0 < r.entry.0, "right child out of order");
        }
        1 + ls + rs
    }

    /// The tree must actually be a tree of depth ~log n, not a list. Without this the balance
    /// code could be entirely wrong and every other test would still pass — slowly.
    fn depth<K, V>(link: &Link<K, V>) -> usize {
        match link {
            None => 0,
            Some(n) => 1 + depth(&n.left).max(depth(&n.right)),
        }
    }

    #[test]
    fn it_behaves_like_a_btreemap_over_a_random_looking_workload() {
        let mut p: PersistentMap<u64, String> = PersistentMap::new();
        let mut b: BTreeMap<u64, String> = BTreeMap::new();
        // A multiplier that is coprime with the modulus, so keys arrive scattered and repeat.
        for i in 0u64..4_000 {
            let k = (i * 7919) % 1_500;
            let v = format!("v{i}");
            p.insert(k, v.clone());
            b.insert(k, v);
            if i % 250 == 0 {
                check(&p.root);
            }
        }
        check(&p.root);
        assert_eq!(p.len(), b.len());
        let pv: Vec<(u64, String)> = p.iter().map(|(k, v)| (*k, v.clone())).collect();
        let bv: Vec<(u64, String)> = b.iter().map(|(k, v)| (*k, v.clone())).collect();
        assert_eq!(pv, bv, "iteration must match BTreeMap key order and values");
        for k in 0..1_600u64 {
            assert_eq!(p.get(&k), b.get(&k), "lookup disagrees at {k}");
        }
    }

    #[test]
    fn ascending_inserts_stay_balanced_rather_than_degenerating_into_a_list() {
        // The adversarial case for an unbalanced BST. Without rotation this is a 4096-deep list
        // and every lookup is O(n).
        let mut p: PersistentMap<u64, u64> = PersistentMap::new();
        for i in 0..4_096u64 {
            p.insert(i, i);
        }
        check(&p.root);
        let d = depth(&p.root);
        // log2(4096) = 12. Weight-balanced trees admit a constant factor above that; a list would
        // be 4096.
        assert!(d <= 24, "ascending inserts produced a tree of depth {d}, expected ~12");
        assert_eq!(p.len(), 4_096);
    }

    #[test]
    fn descending_inserts_stay_balanced_too() {
        let mut p: PersistentMap<u64, u64> = PersistentMap::new();
        for i in (0..4_096u64).rev() {
            p.insert(i, i);
        }
        check(&p.root);
        assert!(depth(&p.root) <= 24);
        assert_eq!(p.len(), 4_096);
    }

    /// The property the fork depends on: a write through one handle is invisible through a clone
    /// taken before it. This is invariant 1 of the module docs, at the data-structure level.
    #[test]
    fn a_clone_does_not_see_writes_made_through_the_original_afterwards() {
        let mut parent: PersistentMap<u64, u64> = (0..500u64).map(|i| (i, i)).collect();
        let child = parent.clone();

        parent.insert(42, 9_999); // overwrite one the child already has
        parent.insert(10_000, 1); // and add one the child has never seen

        assert_eq!(child.get(&42), Some(&42), "child saw the parent's later overwrite");
        assert_eq!(child.get(&10_000), None, "child saw the parent's later insert");
        assert_eq!(child.len(), 500);
        assert_eq!(parent.get(&42), Some(&9_999));
        assert_eq!(parent.len(), 501);
        check(&child.root);
        check(&parent.root);
    }

    /// ...and symmetrically, so the sharing is not secretly one-directional.
    #[test]
    fn the_original_does_not_see_writes_made_through_a_clone() {
        let parent: PersistentMap<u64, u64> = (0..500u64).map(|i| (i, i)).collect();
        let mut child = parent.clone();
        child.insert(42, 9_999);
        child.insert(10_000, 1);
        assert_eq!(parent.get(&42), Some(&42));
        assert_eq!(parent.get(&10_000), None);
        assert_eq!(parent.len(), 500);
    }

    /// Siblings forked from one parent must not see each other at all — the fanout case.
    #[test]
    fn siblings_are_invisible_to_each_other() {
        let parent: PersistentMap<u64, u64> = (0..100u64).map(|i| (i, i)).collect();
        let mut kids: Vec<PersistentMap<u64, u64>> = (0..8).map(|_| parent.clone()).collect();
        for (i, k) in kids.iter_mut().enumerate() {
            k.insert(1_000 + i as u64, i as u64);
        }
        for (i, k) in kids.iter().enumerate() {
            assert_eq!(k.len(), 101, "sibling {i} has the wrong size");
            assert_eq!(k.get(&(1_000 + i as u64)), Some(&(i as u64)));
            for j in 0..8 {
                if i != j {
                    assert_eq!(k.get(&(1_000 + j as u64)), None, "sibling {i} saw sibling {j}");
                }
            }
        }
        assert_eq!(parent.len(), 100);
    }

    /// A chain of clones, each writing: the deepest one must see every ancestor's write made
    /// *before* it forked and none made after. This is the nested-agent-task shape.
    #[test]
    fn a_chain_of_clones_sees_its_ancestry_at_fork_time_and_nothing_later() {
        let mut chain: Vec<PersistentMap<u64, u64>> = Vec::new();
        let mut cur: PersistentMap<u64, u64> = PersistentMap::new();
        for d in 0..64u64 {
            cur.insert(d, d);
            chain.push(cur.clone());
        }
        // Now every ancestor writes again, after every fork.
        for (d, m) in chain.iter_mut().enumerate() {
            m.insert(1_000 + d as u64, 7);
        }
        // The snapshot taken at depth d holds exactly keys 0..=d.
        for (d, m) in chain.iter().enumerate() {
            let want = d + 1 + 1; // 0..=d, plus the one post-fork write through this handle
            assert_eq!(m.len(), want, "depth {d}");
            for k in 0..=d as u64 {
                assert_eq!(m.get(&k), Some(&k), "depth {d} lost ancestor key {k}");
            }
            assert_eq!(m.get(&(d as u64 + 1)), None, "depth {d} saw a later descendant's key");
        }
    }

    #[test]
    fn insert_if_absent_is_first_touch_wins() {
        let mut p: PersistentMap<u64, &str> = PersistentMap::new();
        p.insert_if_absent(1, "first");
        p.insert_if_absent(1, "second");
        assert_eq!(p.get(&1), Some(&"first"));
        assert_eq!(p.len(), 1);
        p.insert(1, "replaced");
        assert_eq!(p.get(&1), Some(&"replaced"));
        assert_eq!(p.len(), 1);
    }

    /// An absent-only insert of a key already present must not copy a path — it hands back the
    /// same root. `stage_all` calls it once per row per statement, so a rebuild here would make
    /// re-touching a row cost a path copy for nothing.
    #[test]
    fn a_no_op_insert_if_absent_shares_the_whole_tree() {
        let mut p: PersistentMap<u64, u64> = (0..100u64).map(|i| (i, i)).collect();
        let before = p.clone();
        p.insert_if_absent(50, 12_345);
        let (Some(a), Some(b)) = (&before.root, &p.root) else { panic!("both non-empty") };
        assert!(Arc::ptr_eq(a, b), "a no-op insert_if_absent rebuilt the tree");
    }

    /// Structural sharing, asserted as a pointer fact rather than inferred from a timing.
    /// After a clone and a single write, the untouched half of the tree must be the *same*
    /// allocation in both maps.
    #[test]
    fn a_write_after_a_clone_shares_every_subtree_it_did_not_touch() {
        let parent: PersistentMap<u64, u64> = (0..1_000u64).map(|i| (i, i)).collect();
        let mut child = parent.clone();
        // Write the smallest key, so the path goes hard left and the right subtree is untouched.
        child.insert(0, 42);
        let (p, c) = (parent.root.as_ref().unwrap(), child.root.as_ref().unwrap());
        assert!(!Arc::ptr_eq(p, c), "the root must be rebuilt by the write");
        let (pr, cr) = (p.right.as_ref().unwrap(), c.right.as_ref().unwrap());
        assert!(
            Arc::ptr_eq(pr, cr),
            "the untouched right subtree was copied instead of shared -- that is the O(W) fork \
             this structure exists to remove"
        );
    }

    /// The same fact counted rather than spot-checked: one write after a clone must allocate
    /// O(log n) new nodes, not O(n). Counted by walking both trees and comparing allocations.
    #[test]
    fn one_write_allocates_a_path_not_a_tree() {
        fn nodes<'a, K, V>(link: &'a Link<K, V>, out: &mut Vec<*const Node<K, V>>) {
            if let Some(n) = link {
                out.push(Arc::as_ptr(n));
                nodes(&n.left, out);
                nodes(&n.right, out);
            }
        }
        let parent: PersistentMap<u64, u64> = (0..1_024u64).map(|i| (i, i)).collect();
        let mut child = parent.clone();
        child.insert(512, 7);

        let (mut pn, mut cn) = (Vec::new(), Vec::new());
        nodes(&parent.root, &mut pn);
        nodes(&child.root, &mut cn);
        assert_eq!(pn.len(), 1_024);
        assert_eq!(cn.len(), 1_024);
        let shared = pn.iter().collect::<std::collections::HashSet<_>>();
        let fresh = cn.iter().filter(|p| !shared.contains(p)).count();
        // log2(1024) = 10; a rotation on the way up can add a few. A deep copy would be 1024.
        assert!(
            fresh <= 32,
            "one write allocated {fresh} new nodes out of 1024 -- expected a path of ~10"
        );
        assert!(fresh >= 1, "the write allocated nothing, so it cannot have happened");
    }

    #[test]
    fn empty_map_answers_without_panicking() {
        let p: PersistentMap<u64, u64> = PersistentMap::new();
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
        assert_eq!(p.get(&1), None);
        assert_eq!(p.iter().count(), 0);
        assert!(!p.contains_key(&0));
    }

    #[test]
    fn string_keys_borrow_for_lookup() {
        // `base_shapes` is keyed by `String` and looked up by `&str`.
        let mut p: PersistentMap<String, u64> = PersistentMap::new();
        p.insert("inventory".to_string(), 1);
        p.insert("roster".to_string(), 2);
        assert_eq!(p.get("inventory"), Some(&1));
        assert_eq!(p.get("missing"), None);
    }

    #[test]
    fn size_hint_is_exact_so_collect_preallocates() {
        let p: PersistentMap<u64, u64> = (0..37u64).map(|i| (i, i)).collect();
        let it = p.iter();
        assert_eq!(it.size_hint(), (37, Some(37)));
        assert_eq!(p.iter().count(), 37);
    }

    /// `range(lo, hi)` must agree with `BTreeMap::range(lo..=hi)` on every window of a keyed
    /// space, including empty windows, windows below and above every key, and a window that is a
    /// single key. Expected values come from `BTreeMap`, never from the subject.
    #[test]
    fn range_agrees_with_btreemap_on_every_window() {
        let keys: Vec<(u32, u64)> = (0..4u32)
            .flat_map(|t| (0..25u64).map(move |r| (t, r * 3)))
            .collect();
        let mut p = PersistentMap::new();
        let mut b = BTreeMap::new();
        for (i, k) in keys.iter().enumerate() {
            p.insert(*k, i);
            b.insert(*k, i);
        }
        let probes: Vec<(u32, u64)> = [(0, 0), (0, 1), (1, 0), (1, 36), (1, 37), (2, 72), (3, 72), (3, 73), (5, 0)]
            .into_iter()
            .collect();
        for lo in &probes {
            for hi in &probes {
                let got: Vec<_> = p.range(lo, hi).map(|(k, v)| (*k, *v)).collect();
                let want: Vec<_> = if lo <= hi { b.range(lo..=hi).map(|(k, v)| (*k, *v)).collect() } else { Vec::new() };
                assert_eq!(got, want, "range({lo:?}, {hi:?})");
            }
        }
        // The prefix the read path asks for: one table, every row, and nothing from its neighbours.
        for t in 0..4u32 {
            let got: Vec<_> = p.range(&(t, 0), &(t, u64::MAX)).map(|(k, _)| *k).collect();
            assert_eq!(got.len(), 25);
            assert!(got.iter().all(|k| k.0 == t));
        }
        assert_eq!(PersistentMap::<(u32, u64), ()>::new().range(&(0, 0), &(9, 9)).count(), 0);
    }
}
