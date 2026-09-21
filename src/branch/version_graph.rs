//! Ancestry queries over the branch forest in better than O(depth).
//!
//! # What this is for, and what it is NOT for
//!
//! **It is not for the read path, and it must never become part of it.** `mod.rs` invariant 2
//! says the read path never walks the parent chain, and `agent_sql::persistent_map` restates it:
//! a child's root *is* its parent's root at fork, so a lookup is a descent of the child's own
//! tree and its cost does not depend on depth. That invariant is the project's entire answer to
//! BranchBench, and an ancestry index is not an excuse to weaken it. Nothing here is called from
//! a `SELECT`.
//!
//! What it is for is the class of questions the *control* plane asks about lineage:
//!
//! 1. **"Is `a` an ancestor of `b`?"** — `reaper::detach_from_parent` walks strictly up the
//!    parent chain, one `has_live_children` per step, and its own comment says the STEPS are
//!    bounded only by the chain's length since D60 removed the cap.
//! 2. **"What is the lowest common ancestor of `a` and `b`?"** — which ferrodb cannot currently
//!    answer at all, and therefore cannot currently merge two *siblings*. See the next section;
//!    this is the real gap.
//!
//! # The merge restriction this lifts
//!
//! `ThreeWayMerger::merge` takes its LCA as `_lca` and never reads it (`tel/engine.rs`), and
//! `schema_merge.rs` states flatly that "there is no LCA computation anywhere in this system".
//! Both are true, and neither is a design flaw on its own — because every merge ferrodb performs
//! targets `parent_id.unwrap_or(BranchId::TRUNK)` (`agent_sql/runtime.rs`, in both `diff` and
//! `evaluate_merge`). When the target is always the direct parent, the fork point IS the parent,
//! LCA is known by construction in O(1), and computing it would be ceremony.
//!
//! The restriction is what that costs: **two sibling branches cannot be merged with each other.**
//! `SIMULATE` forks K candidates off one base; today they can only be admitted one at a time into
//! the shared parent, each re-scored against a base the previous admission moved. Merging
//! candidate 3 directly into candidate 7 — combining two explored lines without laundering both
//! through the trunk — needs their fork point, and their fork point is an LCA query. That is the
//! capability this module exists to make possible, and it is why the interesting operation here
//! is `lca` rather than `is_ancestor`.
//!
//! # Structure: binary lifting (jump pointers)
//!
//! Each node stores its depth and a table `jump[k]` = its 2^k-th ancestor. Building a child's
//! table reads only its parent's (`jump[k] = jump(jump[k-1], k-1)`), so a fork is O(log depth)
//! and touches nothing but the two nodes involved.
//!
//! The technique is Myers' applicative random-access stack (1983); it is what competitive
//! programming calls binary lifting, and it is the same idea as the skip-list-over-ancestors that
//! Git's commit-graph uses for its generation-number reachability checks.
//!
//! * `depth`        O(1)
//! * `level_ancestor` O(log depth) — one array index per set bit of the climb
//! * `is_ancestor`  O(log depth)
//! * `lca`          O(log depth)
//! * `insert_child` O(log depth), local to the child and its parent
//!
//! # What lost, and why
//!
//! **Nested sets / interval labelling (Dietz 1982; Celko's nested-set model; the `ancestor` axis
//! in XML stores; Django-MPTT).** `is_ancestor` becomes a single O(1) interval containment, which
//! is strictly better than what is built here. It is disqualified by **insertion**: the labels are
//! a global numbering of a tree traversal, so adding one child renumbers every node to its right —
//! O(n) per fork against a structure whose whole point is that forks are cheap and live. Branches
//! here are created continuously by running agents, not loaded once from a dump. A static
//! labelling that needs renumbering on every insert is exactly the disqualification the brief
//! names, and this is the option it disqualifies.
//!
//! **ORDPATH (Li & Moon 2001), shipped as SQL Server's `hierarchyid`.** The repair for the above:
//! variable-length labels with gaps, so an insert appends rather than renumbers. It keeps O(1)
//! ancestor tests by prefix comparison and is genuinely the strongest option on the table. It
//! loses on two counts, both about *this* system rather than about ORDPATH. Labels grow with
//! depth, so at the 10^6-branch, unbounded-depth regime the project aims at, the label itself
//! becomes the thing that scales badly — the cap D60 removed was on depth, and reintroducing a
//! depth-proportional per-node cost walks back toward it. And deleting an interior node strands a
//! label prefix that its descendants still encode, so reaping needs a separate story where binary
//! lifting needs none. Worth revisiting if `is_ancestor` ever outgrows `lca` in the workload,
//! which today it does not.
//!
//! **Euler tour + sparse-table RMQ (Bender & Farach-Colton 2000).** O(1) LCA, the best query
//! bound available. Preprocessing is O(n log n) over the *whole* tree and is not incremental: one
//! fork invalidates the tour. Same disqualification as nested sets, for the same reason.
//!
//! **2-hop labelling / bit-parallel reachability (Cohen et al. 2002; GRAIL; Ferrari).** The
//! general-DAG answer, and the branch graph is not a general DAG — every branch has exactly one
//! `parent_id`, written once at fork and never changed (`reaper.rs` relies on precisely this for
//! its termination argument). Paying for DAG reachability machinery to answer a question about a
//! forest is buying generality the data model forbids. It would become the right answer the day
//! a branch can have two parents, i.e. the day a merge creates a merge *commit* with both sides
//! as parents — which ferrodb does not do today.
//!
//! # Reaping
//!
//! Branches are deleted, and a label scheme that breaks when an interior branch is reaped is
//! wrong. Two things make this survivable here.
//!
//! First, **ancestry is genealogy, not liveness**. `reaper::detach_from_parent` walks a chain of
//! *reaped* ancestors on purpose — "a chain of reaped ancestors is precisely what the cascade
//! below walks, and precisely what MCTS pruning produces". So a reaped branch cannot simply
//! vanish from the lineage while a descendant of it is still live. Reaping marks a tombstone; the
//! answers stay correct.
//!
//! Second, tombstones do not accumulate without bound, because **a tombstone with no children is
//! collected, and collection cascades** — the same shape, and for the same reason, as
//! `detach_from_parent`'s cascade up a chain of reaped parents. The invariant that makes this
//! safe is worth stating separately:
//!
//! > **A node with any descendant is never physically removed.**
//!
//! It holds because removal requires `children == 0`, and any jump pointer aimed at a node
//! implies that node has a descendant and therefore a child. So no surviving node can hold a
//! slot index to a freed slot, which is what makes the free list safe to reuse. `remove_slot`
//! refuses rather than warns if it is ever asked to violate it.
//!
//! # Refusing instead of guessing
//!
//! Every query returns `Result` and refuses on an unknown branch rather than answering `false`.
//! `false` is the dangerous direction: "not an ancestor" is the answer that lets a caller treat
//! two branches as unrelated, and a merge that proceeds on a wrongly-unrelated pair is the bug
//! this module would be blamed for. A branch id that is absent means the caller and the index
//! disagree about what exists, and that is a fault, not a negative result.

use std::collections::HashMap;

use crate::branch::types::BranchId;

/// Why an ancestry query could not be answered.
///
/// Deliberately distinct from [`crate::branch::types::BranchError`]: these are faults of the
/// *index*, not of the branch. An `Unknown` here means the index was never told about a branch
/// the caller holds, which is a bookkeeping bug in whoever maintains it, not a reaped handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AncestryError {
    /// The index has no node for this branch. Never silently treated as "no relationship".
    Unknown(BranchId),
    /// `insert_child`/`insert_root` was given a branch the index already holds.
    Duplicate(BranchId),
    /// A depth was requested that is below the root or deeper than the branch itself.
    DepthOutOfRange { branch: BranchId, depth: u32, node_depth: u32 },
}

impl std::fmt::Display for AncestryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AncestryError::Unknown(b) => {
                write!(f, "branch {} is not in the version graph", b)
            }
            AncestryError::Duplicate(b) => {
                write!(f, "branch {} is already in the version graph", b)
            }
            AncestryError::DepthOutOfRange { branch, depth, node_depth } => write!(
                f,
                "depth {} is not an ancestor level of branch {} (which is at depth {})",
                depth, branch, node_depth
            ),
        }
    }
}

impl std::error::Error for AncestryError {}

/// One branch's ancestry record. Slot indices, not `BranchId`s, so a climb is array indexing
/// rather than a hash lookup per step — the difference between an O(log depth) query and an
/// O(log depth) query with a constant nobody wants to pay 10^6 times.
#[derive(Debug, Clone)]
struct Node {
    branch: BranchId,
    parent: Option<u32>,
    depth: u32,
    /// `jump[k]` is the 2^k-th ancestor's slot. `jump[0]` is the parent. Length is
    /// `floor(log2(depth)) + 1` for a non-root, and 0 for a root.
    jump: Vec<u32>,
    /// Direct children currently present in the index, live or tombstoned. Removal requires
    /// this to be zero; see the invariant in the module docs.
    children: u32,
    /// The branch has been reaped. Its lineage is still answerable; it is simply no longer live.
    tombstone: bool,
}

/// An ancestry index over the branch forest.
///
/// Not itself synchronised: it holds no locks and does no I/O, so a caller wraps it in whatever
/// it already uses to serialise branch metadata rather than acquiring a second, differently
/// ordered lock here.
#[derive(Debug, Default)]
pub struct VersionGraph {
    nodes: Vec<Option<Node>>,
    index: HashMap<BranchId, u32>,
    free: Vec<u32>,
    live: usize,
}

impl VersionGraph {
    pub fn new() -> Self {
        VersionGraph::default()
    }

    /// Branches currently in the index, tombstones included.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Branches in the index that have not been reaped.
    pub fn live_len(&self) -> usize {
        self.live
    }

    /// Tombstoned branches still retained because a descendant needs them.
    pub fn tombstone_len(&self) -> usize {
        self.index.len() - self.live
    }

    fn slot(&self, b: BranchId) -> Result<u32, AncestryError> {
        self.index.get(&b).copied().ok_or(AncestryError::Unknown(b))
    }

    fn node(&self, slot: u32) -> &Node {
        self.nodes[slot as usize]
            .as_ref()
            .expect("slot index points at a freed node: the no-descendant invariant was violated")
    }

    /// Record a branch with no parent. ferrodb has exactly one of these (the trunk), but a forest
    /// costs nothing extra and keeps the structure usable for a catalog that has not loaded the
    /// trunk yet.
    pub fn insert_root(&mut self, branch: BranchId) -> Result<(), AncestryError> {
        if self.index.contains_key(&branch) {
            return Err(AncestryError::Duplicate(branch));
        }
        let node = Node { branch, parent: None, depth: 0, jump: Vec::new(), children: 0, tombstone: false };
        self.push(node);
        Ok(())
    }

    /// Record a fork. O(log depth), and reads only `parent`'s jump table.
    ///
    /// This is the operation that disqualified every static labelling scheme in the module docs:
    /// it is local, it renumbers nothing, and it can therefore run on the fork path of a database
    /// that creates branches continuously.
    pub fn insert_child(&mut self, child: BranchId, parent: BranchId) -> Result<(), AncestryError> {
        if self.index.contains_key(&child) {
            return Err(AncestryError::Duplicate(child));
        }
        let pslot = self.slot(parent)?;

        // jump[0] = parent; jump[k] = the 2^(k-1)-th ancestor of the 2^(k-1)-th ancestor.
        // The table stops as soon as the parent's table runs out, which is exactly when the
        // next power of two would climb past the root.
        let mut jump = vec![pslot];
        let mut k = 1usize;
        loop {
            let prev = jump[k - 1];
            let Some(&next) = self.node(prev).jump.get(k - 1) else { break };
            jump.push(next);
            k += 1;
        }

        let depth = self.node(pslot).depth + 1;
        let node = Node { branch: child, parent: Some(pslot), depth, jump, children: 0, tombstone: false };
        self.push(node);
        self.nodes[pslot as usize].as_mut().expect("parent slot live").children += 1;
        Ok(())
    }

    fn push(&mut self, node: Node) {
        let branch = node.branch;
        let slot = match self.free.pop() {
            Some(s) => {
                self.nodes[s as usize] = Some(node);
                s
            }
            None => {
                self.nodes.push(Some(node));
                (self.nodes.len() - 1) as u32
            }
        };
        self.index.insert(branch, slot);
        self.live += 1;
    }

    /// Depth from the root. O(1) — and already O(1) in ferrodb proper, where `BranchRecord.depth`
    /// is stored at fork (`record.rs`) rather than counted.
    pub fn depth(&self, branch: BranchId) -> Result<u32, AncestryError> {
        Ok(self.node(self.slot(branch)?).depth)
    }

    /// The branch's parent, or `None` for a root.
    pub fn parent(&self, branch: BranchId) -> Result<Option<BranchId>, AncestryError> {
        let n = self.node(self.slot(branch)?);
        Ok(n.parent.map(|p| self.node(p).branch))
    }

    /// Whether this branch has been reaped. Its ancestry answers remain valid either way.
    pub fn is_tombstoned(&self, branch: BranchId) -> Result<bool, AncestryError> {
        Ok(self.node(self.slot(branch)?).tombstone)
    }

    fn climb(&self, mut slot: u32, target_depth: u32) -> u32 {
        let mut delta = self.node(slot).depth - target_depth;
        // One array index per set bit: climb by the lowest set bit, clear it, repeat.
        while delta > 0 {
            let k = delta.trailing_zeros() as usize;
            slot = self.node(slot).jump[k];
            delta &= delta - 1;
        }
        slot
    }

    /// The ancestor of `branch` at exactly `depth`. O(log depth).
    pub fn level_ancestor(&self, branch: BranchId, depth: u32) -> Result<BranchId, AncestryError> {
        let slot = self.slot(branch)?;
        let node_depth = self.node(slot).depth;
        if depth > node_depth {
            return Err(AncestryError::DepthOutOfRange { branch, depth, node_depth });
        }
        Ok(self.node(self.climb(slot, depth)).branch)
    }

    /// Is `a` an ancestor of `b`? O(log depth).
    ///
    /// A branch is **not** its own ancestor: this is the strict relation, because the caller that
    /// wants the reflexive one can ask `a == b` for free while the caller that wants the strict
    /// one cannot un-ask it.
    pub fn is_ancestor(&self, a: BranchId, b: BranchId) -> Result<bool, AncestryError> {
        let (sa, sb) = (self.slot(a)?, self.slot(b)?);
        let (da, db) = (self.node(sa).depth, self.node(sb).depth);
        if da >= db {
            return Ok(false);
        }
        Ok(self.climb(sb, da) == sa)
    }

    /// Lowest common ancestor. O(log depth).
    ///
    /// `Ok(None)` means the two branches are in different trees and genuinely have no common
    /// ancestor. It is not an error and it is not the trunk: answering "trunk" for two branches
    /// that never met there would be a fabricated fork point, and a three-way merge against a
    /// fabricated fork point silently treats unrelated rows as concurrent edits.
    ///
    /// The LCA of a branch with itself is that branch, and the LCA of a parent and its child is
    /// the parent — i.e. this is the reflexive ancestor relation, which is what a merge wants:
    /// the fork point of a child and its own parent IS the parent, which is the case ferrodb
    /// already relies on everywhere.
    pub fn lca(&self, a: BranchId, b: BranchId) -> Result<Option<BranchId>, AncestryError> {
        let (mut sa, mut sb) = (self.slot(a)?, self.slot(b)?);
        let (da, db) = (self.node(sa).depth, self.node(sb).depth);

        // Lift the deeper one to the shallower one's depth; now both tables are the same length.
        let target = da.min(db);
        sa = self.climb(sa, target);
        sb = self.climb(sb, target);
        if sa == sb {
            return Ok(Some(self.node(sa).branch));
        }

        // Descend the powers of two together, taking every jump that keeps them apart. What
        // survives is the deepest pair that still differs, so their parent is the LCA.
        //
        // **`k` must be re-bounded against the CURRENT node on every step, not once.** Both
        // tables are `floor(log2(depth)) + 1` long, so each jump taken shortens them, and `k`
        // falls by one per level while the depth can fall by a factor of two. Taking `k` from the
        // starting length alone therefore indexes past the end: a pair at depth 1000 descends
        // 488 -> 232 -> 104 -> 40 -> 8, and at depth 8 the table holds 4 entries while `k` is
        // still 4. That was a panic, found by `deep_chain_agrees_with_an_independent_walk`; the
        // shallow fixtures never reached a level where the two could diverge.
        //
        // Skipping an out-of-range level is not merely safe, it is the same algorithm: the
        // textbook form pads every table to a fixed `ceil(log2(max_depth))` with the root, and a
        // padded entry is equal on both sides, so the `ja != jb` test declines it anyway.
        let mut k = self.node(sa).jump.len();
        while k > 0 {
            k -= 1;
            let (na, nb) = (self.node(sa), self.node(sb));
            if k >= na.jump.len() || k >= nb.jump.len() {
                continue;
            }
            let (ja, jb) = (na.jump[k], nb.jump[k]);
            if ja != jb {
                sa = ja;
                sb = jb;
            }
        }

        // Disjoint trees fall out here: the climb ran to two different roots, and a root has no
        // parent, so the answer is `None` rather than an invented one.
        Ok(self.node(sa).parent.map(|p| self.node(p).branch))
    }

    /// Mark a branch reaped, then collect whatever that makes collectable.
    ///
    /// Returns how many nodes were physically removed, which is 0 when the branch still has
    /// children — the ordinary case for an interior branch, and the case the module docs turn on.
    ///
    /// The cascade is the same one `reaper::detach_from_parent` performs over the catalog: a
    /// tombstoned parent may have been retained only for the child just removed, so removal walks
    /// up as far as it stays justified. Each step is O(1) here.
    pub fn reap(&mut self, branch: BranchId) -> Result<usize, AncestryError> {
        let slot = self.slot(branch)?;
        {
            let n = self.nodes[slot as usize].as_mut().expect("slot live");
            if !n.tombstone {
                n.tombstone = true;
                self.live -= 1;
            }
        }

        let mut removed = 0usize;
        let mut cur = slot;
        loop {
            let n = self.node(cur);
            if n.children > 0 || !n.tombstone {
                break;
            }
            let parent = n.parent;
            self.remove_slot(cur);
            removed += 1;
            match parent {
                Some(p) => {
                    let pn = self.nodes[p as usize].as_mut().expect("parent slot live");
                    pn.children -= 1;
                    cur = p;
                }
                None => break,
            }
        }
        Ok(removed)
    }

    /// Free one slot. Refuses rather than warns if the no-descendant invariant does not hold,
    /// because the failure it would otherwise cause is a stale slot index answering a query with
    /// somebody else's lineage — silent, and indistinguishable from a correct answer.
    fn remove_slot(&mut self, slot: u32) {
        let n = self.nodes[slot as usize].as_ref().expect("slot live");
        assert_eq!(
            n.children, 0,
            "refusing to free branch {} while it still has {} children: a jump pointer aimed at \
             it would survive into a reused slot",
            n.branch, n.children
        );
        let branch = n.branch;
        self.nodes[slot as usize] = None;
        self.index.remove(&branch);
        self.free.push(slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(id: u64) -> BranchId {
        BranchId::new(id, 0)
    }

    /// Build a graph and return it, given `(child, parent)` edges applied in order.
    fn graph(root: BranchId, edges: &[(BranchId, BranchId)]) -> VersionGraph {
        let mut g = VersionGraph::new();
        g.insert_root(root).unwrap();
        for (c, p) in edges {
            g.insert_child(*c, *p).unwrap();
        }
        g
    }

    /// The child -> parent map the test itself declared. The oracles below read only this, never
    /// the structure under test, so an expected value can never come from calling the subject.
    fn parents(edges: &[(BranchId, BranchId)]) -> HashMap<BranchId, BranchId> {
        edges.iter().copied().collect()
    }

    /// An independent oracle: walk parent pointers, the way `reaper::detach_from_parent` does.
    fn walk_is_ancestor(par: &HashMap<BranchId, BranchId>, a: BranchId, b: BranchId) -> bool {
        let mut cur = b;
        while let Some(&p) = par.get(&cur) {
            if p == a {
                return true;
            }
            cur = p;
        }
        false
    }

    fn walk_chain(par: &HashMap<BranchId, BranchId>, mut x: BranchId) -> Vec<BranchId> {
        let mut v = vec![x];
        while let Some(&p) = par.get(&x) {
            v.push(p);
            x = p;
        }
        v
    }

    fn walk_lca(
        par: &HashMap<BranchId, BranchId>,
        a: BranchId,
        b: BranchId,
    ) -> Option<BranchId> {
        // A set for the second chain, so an all-pairs sweep stays linear per pair rather than
        // quadratic. Still nothing but the declared edge list.
        let cb: std::collections::HashSet<BranchId> = walk_chain(par, b).into_iter().collect();
        walk_chain(par, a).into_iter().find(|x| cb.contains(x))
    }

    #[test]
    fn depth_counts_from_the_root() {
        let g = graph(b(0), &[(b(1), b(0)), (b(2), b(1)), (b(3), b(2))]);
        assert_eq!(g.depth(b(0)).unwrap(), 0);
        assert_eq!(g.depth(b(1)).unwrap(), 1);
        assert_eq!(g.depth(b(3)).unwrap(), 3);
    }

    #[test]
    fn is_ancestor_is_strict_and_directional() {
        let g = graph(b(0), &[(b(1), b(0)), (b(2), b(1))]);
        assert!(g.is_ancestor(b(0), b(2)).unwrap());
        assert!(g.is_ancestor(b(1), b(2)).unwrap());
        // Not reflexive.
        assert!(!g.is_ancestor(b(2), b(2)).unwrap());
        // Not symmetric: the whole point.
        assert!(!g.is_ancestor(b(2), b(0)).unwrap());
    }

    #[test]
    fn siblings_are_not_ancestors_of_each_other() {
        let g = graph(b(0), &[(b(1), b(0)), (b(2), b(0))]);
        assert!(!g.is_ancestor(b(1), b(2)).unwrap());
        assert!(!g.is_ancestor(b(2), b(1)).unwrap());
    }

    #[test]
    fn lca_of_siblings_is_the_fork_point() {
        let g = graph(b(0), &[(b(1), b(0)), (b(2), b(1)), (b(3), b(1))]);
        assert_eq!(g.lca(b(2), b(3)).unwrap(), Some(b(1)));
    }

    #[test]
    fn lca_is_reflexive_and_handles_the_parent_case() {
        let g = graph(b(0), &[(b(1), b(0)), (b(2), b(1))]);
        assert_eq!(g.lca(b(2), b(2)).unwrap(), Some(b(2)));
        // The case ferrodb already relies on: a child merged into its own parent.
        assert_eq!(g.lca(b(1), b(2)).unwrap(), Some(b(1)));
    }

    #[test]
    fn lca_of_disjoint_trees_is_none_not_the_trunk() {
        let mut g = VersionGraph::new();
        g.insert_root(b(0)).unwrap();
        g.insert_root(b(100)).unwrap();
        g.insert_child(b(1), b(0)).unwrap();
        g.insert_child(b(101), b(100)).unwrap();
        assert_eq!(g.lca(b(1), b(101)).unwrap(), None);
    }

    #[test]
    fn level_ancestor_hits_each_level_and_refuses_below_the_node() {
        let g = graph(b(0), &[(b(1), b(0)), (b(2), b(1)), (b(3), b(2)), (b(4), b(3))]);
        assert_eq!(g.level_ancestor(b(4), 0).unwrap(), b(0));
        assert_eq!(g.level_ancestor(b(4), 2).unwrap(), b(2));
        assert_eq!(g.level_ancestor(b(4), 4).unwrap(), b(4));
        assert!(matches!(
            g.level_ancestor(b(4), 5),
            Err(AncestryError::DepthOutOfRange { .. })
        ));
    }

    /// The brief's requirement 3, stated directly: reap an INTERIOR branch and every ancestry
    /// answer about the branches around it must stay correct.
    #[test]
    fn reaping_an_interior_branch_preserves_ancestry() {
        let edges = [(b(1), b(0)), (b(2), b(1)), (b(3), b(2))];
        let mut g = graph(b(0), &edges);

        // b(1) and b(2) are interior: both have children.
        assert_eq!(g.reap(b(1)).unwrap(), 0, "an interior branch cannot be collected");
        assert_eq!(g.reap(b(2)).unwrap(), 0);

        assert!(g.is_tombstoned(b(1)).unwrap());
        assert!(g.is_tombstoned(b(2)).unwrap());

        // Every relation still answers, and answers what the independent walk says.
        let par = parents(&edges);
        for (x, y) in [(b(0), b(3)), (b(1), b(3)), (b(2), b(3)), (b(0), b(2))] {
            assert_eq!(
                g.is_ancestor(x, y).unwrap(),
                walk_is_ancestor(&par, x, y),
                "is_ancestor({x}, {y}) changed when an interior branch was reaped"
            );
        }
        assert_eq!(g.depth(b(3)).unwrap(), 3, "reaping must not renumber anything");
        assert_eq!(g.lca(b(3), b(1)).unwrap(), Some(b(1)));
    }

    #[test]
    fn a_tombstone_is_collected_only_once_its_last_descendant_goes() {
        let mut g = graph(b(0), &[(b(1), b(0)), (b(2), b(1)), (b(3), b(2))]);
        assert_eq!(g.len(), 4);

        g.reap(b(1)).unwrap();
        g.reap(b(2)).unwrap();
        assert_eq!(g.len(), 4, "tombstones retained while b(3) needs them");
        assert_eq!(g.live_len(), 2);
        assert_eq!(g.tombstone_len(), 2);

        // Reaping the leaf collects it AND cascades through both tombstoned ancestors, exactly
        // like `detach_from_parent` walking up a chain of reaped parents.
        let removed = g.reap(b(3)).unwrap();
        assert_eq!(removed, 3, "leaf plus the two tombstones it was pinning");
        assert_eq!(g.len(), 1, "only the live root remains");
        assert_eq!(g.live_len(), 1);
        assert!(matches!(g.depth(b(2)), Err(AncestryError::Unknown(_))));
    }

    #[test]
    fn a_live_sibling_keeps_a_tombstoned_parent_alive() {
        let mut g = graph(b(0), &[(b(1), b(0)), (b(2), b(1)), (b(3), b(1))]);
        g.reap(b(1)).unwrap();
        assert_eq!(g.reap(b(2)).unwrap(), 1, "only b(2) itself");
        assert_eq!(g.len(), 3, "b(1) retained: b(3) is still under it");
        assert!(g.is_ancestor(b(1), b(3)).unwrap());
        assert_eq!(g.lca(b(3), b(0)).unwrap(), Some(b(0)));
    }

    /// A reused slot must never be reachable from a surviving node. Churn the graph hard enough
    /// that the free list is exercised, then check every answer against the independent walk.
    #[test]
    fn slot_reuse_never_leaks_a_stale_lineage() {
        let mut g = VersionGraph::new();
        g.insert_root(b(0)).unwrap();
        for round in 0..50u64 {
            let leaf = b(1000 + round);
            g.insert_child(leaf, b(0)).unwrap();
            g.insert_child(b(2000 + round), leaf).unwrap();
            // Drop both, freeing two slots for the next round to reuse.
            g.reap(b(2000 + round)).unwrap();
            g.reap(leaf).unwrap();
            assert_eq!(g.len(), 1, "round {round} leaked a node");
        }
        // The root survived every reuse with its own identity intact.
        assert_eq!(g.depth(b(0)).unwrap(), 0);
        assert_eq!(g.parent(b(0)).unwrap(), None);
    }

    /// The same id slot at a new generation is a DIFFERENT branch — which is the entire reason
    /// `BranchId` carries a generation (`types.rs`). Keying on `id` alone would make a recycled
    /// id inherit the dead branch's lineage.
    #[test]
    fn a_recycled_id_does_not_inherit_the_old_lineage() {
        let old = BranchId::new(7, 0);
        let new = old.bump();
        let mut g = graph(b(0), &[(b(1), b(0)), (old, b(1))]);

        g.reap(old).unwrap();
        // Same id, next generation, forked somewhere else entirely.
        g.insert_child(new, b(0)).unwrap();

        assert_eq!(g.depth(new).unwrap(), 1, "the recycled id must not inherit depth 2");
        assert!(!g.is_ancestor(b(1), new).unwrap(), "b(1) was the OLD generation's parent");
        assert_eq!(g.lca(new, b(1)).unwrap(), Some(b(0)));
    }

    #[test]
    fn an_unknown_branch_refuses_rather_than_answering_false() {
        let g = graph(b(0), &[(b(1), b(0))]);
        assert!(matches!(g.is_ancestor(b(1), b(99)), Err(AncestryError::Unknown(_))));
        assert!(matches!(g.lca(b(99), b(1)), Err(AncestryError::Unknown(_))));
        assert!(matches!(g.depth(b(99)), Err(AncestryError::Unknown(_))));
    }

    #[test]
    fn reinserting_a_branch_is_refused() {
        let mut g = graph(b(0), &[(b(1), b(0))]);
        assert!(matches!(g.insert_child(b(1), b(0)), Err(AncestryError::Duplicate(_))));
        assert!(matches!(g.insert_root(b(0)), Err(AncestryError::Duplicate(_))));
        assert!(matches!(g.insert_child(b(5), b(42)), Err(AncestryError::Unknown(_))));
    }

    /// Cross-check every answer on a deep chain against the independent parent walk. The oracle
    /// is the walk, never the structure under test.
    #[test]
    fn deep_chain_agrees_with_an_independent_walk() {
        const D: u64 = 1000;
        let mut edges: Vec<(BranchId, BranchId)> = Vec::new();
        for i in 1..=D {
            edges.push((b(i), b(i - 1)));
        }
        // A second chain off the root, so LCA has a non-trivial answer.
        for i in 1..=D {
            edges.push((b(10_000 + i), if i == 1 { b(0) } else { b(10_000 + i - 1) }));
        }
        let g = graph(b(0), &edges);
        let par = parents(&edges);

        for (x, y) in [
            (b(0), b(D)),
            (b(D / 2), b(D)),
            (b(D), b(D / 2)),
            (b(7), b(D - 3)),
            (b(D), b(10_000 + D)),
        ] {
            assert_eq!(
                g.is_ancestor(x, y).unwrap(),
                walk_is_ancestor(&par, x, y),
                "is_ancestor({x}, {y})"
            );
            assert_eq!(g.lca(x, y).unwrap(), walk_lca(&par, x, y), "lca({x}, {y})");
        }
        assert_eq!(g.lca(b(D), b(10_000 + D)).unwrap(), Some(b(0)));
        assert_eq!(g.depth(b(10_000 + D)).unwrap(), D as u32);
    }

    /// The descent in `lca` re-bounds `k` per step because a pair can fall past a power-of-two
    /// boundary mid-descent. One depth cannot show that; this sweeps every depth that could hide
    /// it, including each power of two and its neighbours.
    ///
    /// This is the test that would have caught the original panic at depth 9, not depth 1000.
    #[test]
    fn lca_descends_correctly_at_every_level_boundary() {
        for t in (1u64..=40)
            .chain([63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025])
        {
            // Two chains of depth `t` off one root: their LCA is the root, which is the case
            // that forces the descent all the way down.
            let mut edges = Vec::new();
            for i in 1..=t {
                edges.push((b(i), if i == 1 { b(0) } else { b(i - 1) }));
            }
            for i in 1..=t {
                edges.push((b(1_000_000 + i), if i == 1 { b(0) } else { b(1_000_000 + i - 1) }));
            }
            let g = graph(b(0), &edges);
            let par = parents(&edges);

            let (x, y) = (b(t), b(1_000_000 + t));
            assert_eq!(g.lca(x, y).unwrap(), Some(b(0)), "depth {t}: LCA of two chains");
            assert_eq!(g.lca(x, y).unwrap(), walk_lca(&par, x, y), "depth {t}");
            assert!(g.is_ancestor(b(0), x).unwrap(), "depth {t}");
            assert!(!g.is_ancestor(x, y).unwrap(), "depth {t}: chains are disjoint below the root");
            assert_eq!(g.depth(x).unwrap(), t as u32, "depth {t}");
            // Every level of the left chain, so `level_ancestor`'s climb is swept too.
            for d in 0..=t {
                assert_eq!(g.level_ancestor(x, d as u32).unwrap(), b(d), "depth {t}, level {d}");
            }
        }
    }

    /// Exhaustive all-pairs agreement on a bushy, irregular tree. A chain exercises one shape;
    /// this exercises branching at every depth, which is what the real workload produces.
    #[test]
    fn all_pairs_agree_with_the_walk_on_an_irregular_tree() {
        // **The shape matters more than the size.** An earlier version of this test attached each
        // node to a pseudo-randomly chosen recent ancestor, which produced one bushy tree ~33 deep
        // whose deep nodes nearly all shared a single depth-1 ancestor. It passed against the
        // known-bad `lca` (verified by reintroducing that bug), because a descent only indexes
        // past the end of a jump table once the two sides actually diverge high up.
        //
        // So: several DISTINCT deep subtrees off the root, which is what forces a cross-subtree
        // pair to descend the whole way, plus side leaves so the tree is bushy as well as deep.
        const CHAINS: u64 = 4;
        const LEN: u64 = 50;
        let mut edges: Vec<(BranchId, BranchId)> = Vec::new();
        let mut next = 1u64;
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut deep_leaves: Vec<BranchId> = Vec::new();
        for _ in 0..CHAINS {
            let mut prev = b(0);
            for _ in 0..LEN {
                let node = b(next);
                next += 1;
                edges.push((node, prev));
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                if seed % 4 == 0 {
                    edges.push((b(next), node));
                    next += 1;
                }
                prev = node;
            }
            deep_leaves.push(prev);
        }
        let n = next;
        let g = graph(b(0), &edges);
        let par = parents(&edges);

        let mut deepest = 0;
        for i in 0..n {
            deepest = deepest.max(g.depth(b(i)).unwrap());
        }
        assert!(deepest >= 40, "tree too shallow to cross a level boundary (depth {deepest})");
        // The subtrees must really be distinct, or no pair forces a full descent and this test
        // is vacuous in exactly the way the previous version was.
        assert_eq!(
            g.lca(deep_leaves[0], deep_leaves[1]).unwrap(),
            Some(b(0)),
            "two deep leaves in different subtrees must meet only at the root"
        );
        assert_eq!(g.depth(deep_leaves[0]).unwrap(), LEN as u32);

        for i in 0..n {
            for j in 0..n {
                let (x, y) = (b(i), b(j));
                assert_eq!(
                    g.is_ancestor(x, y).unwrap(),
                    walk_is_ancestor(&par, x, y),
                    "is_ancestor({x}, {y})"
                );
                assert_eq!(g.lca(x, y).unwrap(), walk_lca(&par, x, y), "lca({x}, {y})");
            }
        }
    }
}
