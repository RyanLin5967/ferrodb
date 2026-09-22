//! **Structural three-way merge (D92).** Descend base/ours/theirs together; skip any subtree the
//! three agree on; report a conflict only at a cell all three disagree on.
//!
//! Frontier gap #5. Dolt and ForkBase merge this way. ferrodb, until this file, did not.
//!
//! # What ferrodb already had, and why this is not a replacement for it
//!
//! `agent_sql::runtime` merges by **replaying recorded ops**. `AppliedOp` is one published effect;
//! `State::applied` is the log of every effect the runtime ever published; `applied_by_cell` keys
//! that log by `(tbl, row, col)`; `concurrent_op` asks the index "what landed on this cell after I
//! forked?" and `merge_engine::compose_ops` folds the answer into one effect. Five lines on how the
//! two differ:
//!
//! 1. **Input.** Op-replay needs the ops to have been *recorded* — it merges a history. A structural
//!    merge needs only the three trees; it merges *states* and can merge a branch whose log was
//!    pruned, truncated, or never written.
//! 2. **Cost.** Op-replay is O(ops since the fork) — `concurrent_op` is O(log k) per cell *after*
//!    D86 indexed it, but the log itself is never pruned and every changed cell pays a probe. A
//!    structural merge is O(delta x log N): it never looks at history at all, and a subtree nobody
//!    touched is skipped by one id comparison however many ops passed through its neighbours.
//! 3. **Granularity.** Op-replay resolves per *cell* and knows which column moved. A structural
//!    merge resolves per *key* at whatever granularity the tree stores — here, whole leaf values.
//! 4. **Semantics on collision.** Op-replay can resolve a collision that a structural merge cannot
//!    see a way through, because it holds the *intent* and not just the outcome. See below.
//! 5. **What it proves.** Op-replay's answer depends on the log being complete; a lost op is a
//!    silently wrong merge. A structural merge's answer is a function of the three trees alone, so
//!    it is reproducible from the data at rest and can *audit* an op-replay result.
//!
//! ## Where op-replay is STRICTLY better, and it is not close
//!
//! **Commuting effects on one cell.** `OpKind` is an effect algebra, not a value: `Add(Delta)`,
//! `Max`, `Min`, `SetInsert`/`SetRemove` with observed-remove dots. Take a counter at base 10; ours
//! runs `Add(5)`, theirs runs `Add(3)`. Op-replay reads two `Add`s, `OpKind::commutes_with` says
//! they commute, `compose_ops` folds them to `Add(8)`, and the merge is **clean at 18**.
//!
//! A structural merge sees base=10, ours=15, theirs=13. Three distinct values, one cell, both sides
//! moved it: by the truth table below that is row 5, **conflict**, and there is no repair available
//! — the information that 15 *meant* "+5" was never in the tree. The same holds for a set-valued
//! column: op-replay unions two `SetInsert`s and honours observed-remove; structural sees two
//! different serialised blobs and conflicts.
//!
//! This is the general shape of it. A structural merge is a function of three *states*; a state
//! cannot distinguish "assigned 15" from "incremented by 5", so every merge that needs the
//! distinction is one op-replay wins outright. The two are complements, and the reason to have both
//! is that each one's strength is the other's blind spot: op-replay cannot merge what it did not
//! record, and structural cannot merge what the values do not say.
//!
//! # The one rule, at three granularities
//!
//! Everything below is the same three comparisons — `ours == theirs`, `ours == base`,
//! `theirs == base` — applied at the root, at a subtree, and at a cell. That is not a coincidence
//! and it is the whole idea of a structural merge: the identity of a subtree stands in for the
//! identity of every cell beneath it, so one comparison retires a million of them.
//!
//! # The conflict truth table
//!
//! `b`/`o`/`t` are the value of one key in base/ours/theirs, each `Some` or absent. Every row is a
//! test in `mod tests`, named `row_NN_*`, and the fourteen rows are exhaustive over the space: with
//! `b` absent there are four reachable combinations, and with `b` present, `o` and `t` each range
//! over {absent, equal to b, different from b}, whose nine pairs become ten rows because
//! (different, different) splits on whether the two differ from each other.
//!
//! | row | base   | ours      | theirs    | result                    | rule |
//! |-----|--------|-----------|-----------|---------------------------|------|
//! |  1  | x      | x         | x         | keep x                    | o==t |
//! |  2  | x      | y         | x         | take ours (y)             | t==b |
//! |  3  | x      | x         | z         | take theirs (z)           | o==b |
//! |  4  | x      | y         | y         | keep y (convergent edit)  | o==t |
//! |  5  | x      | y         | z, y!=z   | **CONFLICT** BothModified | -    |
//! |  6  | x      | *deleted* | x         | delete                    | t==b |
//! |  7  | x      | x         | *deleted* | delete                    | o==b |
//! |  8  | x      | *deleted* | z, z!=x   | **CONFLICT** OursDeletedTheirsModified | - |
//! |  9  | x      | y, y!=x   | *deleted* | **CONFLICT** OursModifiedTheirsDeleted | - |
//! | 10  | x      | *deleted* | *deleted* | delete (convergent)       | o==t |
//! | 11  | absent | y         | absent    | insert y                  | t==b |
//! | 12  | absent | absent    | z         | insert z                  | o==b |
//! | 13  | absent | y         | y         | insert y (convergent)     | o==t |
//! | 14  | absent | y         | z, y!=z   | **CONFLICT** BothModified (add/add) | - |
//!
//! The four conflict rows are exactly the rows no rule fires on. [`resolve_cell`] is the table.
//!
//! # What `merged_root` is when there are conflicts
//!
//! The merged tree is still produced, with every conflicted key left at **ours'** value, and the
//! conflicts reported beside it. That is Dolt's shape: a merge with conflicts yields a working set
//! plus a conflict list for a human or an agent to settle, rather than nothing. Callers that want
//! all-or-nothing check `conflicts.is_empty()` before publishing the root — which is what
//! `agent_sql`'s gate already does with `MergeOutcome::Conflict`.

use std::collections::BTreeMap;

use crate::branch::types::{BranchId, Epoch, PageId};
use crate::cow::btree::CowTree;
use crate::cow::cid::{self, NodeShape};
use crate::error::FerroError;

/// Descent guard, matching `btree`'s. A tree deeper than this is a cycle, and looping forever
/// inside a merge is worse than failing.
const MAX_DEPTH: usize = 64;

// ---- identity ---------------------------------------------------------------------------------
//
// merge3 owns NO identity, NO hasher and NO memo. All three are `cow::diff`'s, and this section is
// the argument for why, because the version of this file that landed owned all three and each was
// a defect:
//
//   * a private `H128`, the weaker of two 128-bit hashers in this directory, sitting on the path
//     that declares a merge conflict-free — which `cow::cid` forbids in as many words;
//   * a memo keyed on `PageId`, whose stated soundness argument ("a modified page gets a NEW id")
//     is false twice over: `ArenaPageStore::cow_page` mutates a branch's own page IN PLACE without
//     restamping `birth_epoch`, and `alloc_page` pops recycled ids;
//   * and no reserved tag byte, so a content id and a page id could in principle compare equal.
//
// `cow::diff` had already solved all three — `NodeIdentity`, `PageIdentity`, `SubtreeHash`,
// `MemoIdentity`, a `misses()` counter and the reserved byte-0 domain tag — so the fix is to
// delete this file's copies rather than to repair them.
//
// WHICH IDENTITY TO PASS, AND THE MEASUREMENT THAT DECIDES IT. `bench/d92_content_identity_trade.txt`
// drives the convergent edit (row 4), the one case page identity provably cannot skip:
//
//     n=500   saved 6 page reads, cost 70 hashed      (12x)
//     n=16000 saved 9 page reads, cost 2021 hashed   (225x)
//
// The saving is CONSTANT — it is bounded by the descent's depth. The cost is O(N). So the ratio
// does not converge, and there is no size at which content identity starts paying. That is also
// why the hash *function* is second-order here: truncated SHA-256 would remove the collision
// argument but only makes the O(N) side dearer, and a faster hash cannot rescue an O(N)-for-
// O(log N) trade. Hence [`diff::PageIdentity`] is the identity to pass, and it is exact.
//
// ⚠ THAT CONCLUSION IS SCOPED TO THE IN-LINEAGE CASE, which is the one a merge is normally handed:
// a base and two copy-on-write descendants of it, sharing pages. "The digest never pays" would be
// a stronger claim than anything measured, and it is not made. Two cases where it does pay:
//
//   * **Cross-lineage**, i.e. three roots that are not COW relatives — which this function does
//     accept. Measured by `fix-diff-memo` at 1200 rows with no shared pages: page identity reads
//     180 and skips nothing, the digest reads 2 and skips 1, and the stamp costs 180 reads once.
//     One comparison is a wash; it pays from the SECOND off a single stamp.
//   * **Write-time stamping**, as ForkBase does and as `diff::SubtreeHash`'s doc says it models.
//     Then the O(N) term is not paid at merge time at all. Nothing in ferrodb stamps at write
//     time today, so that configuration is not measurable here and is not claimed.

pub use crate::cow::diff::{IdentityProof, MemoIdentity, NodeIdentity, PageIdentity, SubtreeHash};


// ---- result types -----------------------------------------------------------------------------

/// Why one key could not be merged. The four rows of the truth table that no rule fires on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictKind {
    /// Rows 5 and 14: both sides moved the key, to different values.
    BothModified,
    /// Row 8: ours deleted the key, theirs changed it.
    OursDeletedTheirsModified,
    /// Row 9: ours changed the key, theirs deleted it.
    OursModifiedTheirsDeleted,
}

/// One key all three disagree on, carrying all three values so a resolver has what it needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub ours: Option<Vec<u8>>,
    pub theirs: Option<Vec<u8>>,
    pub kind: ConflictKind,
}

/// Which O(1) rule retired the whole merge at the root, if one did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootFastPath {
    /// `ours == theirs`: nothing to merge, either root is the answer.
    SidesAgree,
    /// `ours == base`: ours changed nothing, so **take theirs whole**. Zero pages read.
    OursUnchanged,
    /// `theirs == base`: theirs changed nothing, so take ours whole. Zero pages read.
    TheirsUnchanged,
}

/// What the descent cost and what it skipped. Every field is a count of something that happened,
/// not a timing, so it reads the same on an idle machine and a loaded one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeStats {
    /// Pages the descent read. The curve's y-axis, and the cost that actually touches disk.
    pub nodes_read: usize,
    /// Identity comparisons the descent made. Free for [`PageIdentity`], so this is CPU and not
    /// I/O — but it is the traversal's real shape and it is reported rather than folded into
    /// `nodes_read`, because a skip is only cheap if the test that produced it was cheap.
    ///
    /// It grows like `fanout x depth x changed paths`: every child of a node that must be
    /// descended gets tested, and most of them are retired by that test alone.
    pub ids_compared: usize,
    /// Subtree triples retired by `ours == theirs` (this subsumes all-three-equal, which is the
    /// common case and the reason a merge is cheap).
    pub skip_sides_agree: usize,
    /// Subtree triples retired by `theirs == base`: theirs touched nothing here, and the merged
    /// tree is built on ours, so there is nothing to do.
    pub skip_theirs_unchanged: usize,
    /// Subtree triples where `ours == base`. **Not a skip** below the root: ours touched nothing
    /// here, so everything theirs changed has to be harvested. The descent continues, but with two
    /// distinct trees instead of three, so no conflict is reachable in this region.
    pub descend_ours_unchanged: usize,
    /// Triples where all three sides bottomed out at leaves and keys were compared one by one.
    pub leaf_triples: usize,
    /// Keys the truth table was evaluated on.
    pub keys_compared: usize,
    /// Keys whose merged value differs from ours, i.e. the writes applied to build `merged_root`.
    pub edits_applied: usize,
    /// Set when one of the three O(1) root rules retired the merge without reading a page.
    pub root_fast_path: Option<RootFastPath>,
}

impl MergeStats {
    /// Every subtree retired without being read. The detector's output: a run where this is zero
    /// did no skipping at all.
    pub fn skips(&self) -> usize {
        self.skip_sides_agree + self.skip_theirs_unchanged
    }
}

/// The merged tree, what could not be merged, and what it cost.
#[derive(Debug)]
pub struct MergeResult {
    /// Root of the merged tree. Conflicted keys sit at ours' value; see the module brief.
    pub merged_root: PageId,
    pub conflicts: Vec<Conflict>,
    pub stats: MergeStats,
    /// **What an empty `conflicts` rests on**, from the identity the caller supplied.
    ///
    /// Every skip in this file — including the root rules, which return with zero pages read — is
    /// an identity comparison. With [`IdentityProof::Exact`] that comparison is a proof. With
    /// [`IdentityProof::Fingerprint`] it is a 128-bit fingerprint's word, and `cid.rs` forbids such
    /// a word being the *sole* authority for declaring a merge conflict-free without the caller
    /// having said it accepts that. This field is where it says so, on the way back out.
    pub identity_proof: IdentityProof,
}

impl MergeResult {
    /// No conflicts were reported.
    ///
    /// **What that is worth is [`MergeResult::identity_proof`]**, and a caller gating a publish on
    /// this should read it: with [`IdentityProof::Fingerprint`] this is a strong claim about
    /// subtrees that were never read, not a demonstration that they agree.
    pub fn is_clean(&self) -> bool {
        self.conflicts.is_empty()
    }
}

// ---- the cell rule ----------------------------------------------------------------------------

/// What the merge does with one key. **This function is the truth table.**
///
/// Fourteen rows collapse to three comparisons, in this order, and the order matters: `o == t` has
/// to be tested first so that a convergent edit (rows 4, 10, 13) is *agreement* rather than two
/// sides having both moved away from base.
pub fn resolve_cell(
    base: Option<&Vec<u8>>,
    ours: Option<&Vec<u8>>,
    theirs: Option<&Vec<u8>>,
) -> Result<Option<Vec<u8>>, ConflictKind> {
    // Rows 1, 4, 10, 13: the two sides agree, whatever base said.
    if ours == theirs {
        return Ok(ours.cloned());
    }
    // Rows 3, 7, 12: ours never moved, so theirs is the only opinion.
    if ours == base {
        return Ok(theirs.cloned());
    }
    // Rows 2, 6, 11: theirs never moved, so ours is the only opinion.
    if theirs == base {
        return Ok(ours.cloned());
    }
    // Rows 5, 8, 9, 14: both moved, and not to the same place.
    Err(match (ours, theirs) {
        (None, Some(_)) => ConflictKind::OursDeletedTheirsModified,
        (Some(_), None) => ConflictKind::OursModifiedTheirsDeleted,
        _ => ConflictKind::BothModified,
    })
}

// ---- the descent ------------------------------------------------------------------------------

/// Merge `theirs` into `ours` against their common ancestor `base`.
///
/// The three roots are the first three arguments, as the shape of the operation demands. The rest
/// are structurally required and cannot be defaulted: `ids` because the choice of identity changes
/// what can be skipped (see [`PageIdentity`]), and `into`/`epoch` because building the merged tree
/// allocates pages and every allocation in this store is stamped with the branch that owns it and
/// the epoch it was born at — `cow/mod.rs` makes the epoch a required parameter rather than
/// something a store reads from a clock, and this function is not the place to break that.
///
/// # Cost
///
/// O(delta x log N) page reads, where delta is what the two sides changed — **not** O(N). The
/// root-level rules are O(1) with zero reads. Below the root, a subtree the two sides agree on, or
/// that theirs did not touch, is retired by one id comparison without being read; only a subtree
/// theirs actually changed is descended. `bench/d92_merge3_curve.txt` measures this across a 256x
/// range of N with the delta held at 4 keys per side.
///
/// This is why it is a *synchronised* descent and not two calls to `CowTree::diff`. That function
/// is honest that its page-identity traversal is proportional to the tree — it walks both roots in
/// full before it can prune either against the other. Descending the three together prunes at the
/// moment of comparison, which is the whole difference between O(N) and O(delta x log N).
pub fn merge3(
    tree: &CowTree,
    base: PageId,
    ours: PageId,
    theirs: PageId,
    ids: &dyn NodeIdentity,
    into: BranchId,
    epoch: Epoch,
) -> Result<MergeResult, FerroError> {
    let mut stats = MergeStats::default();

    // What an empty conflict list will rest on, recorded before anything can forget it.
    let identity_proof = ids.proof();

    // The same three comparisons as `resolve_cell`, at the coarsest granularity there is. Each one
    // retires the entire merge without reading a page.
    let (ib, io, it) = (ids.id_of(base), ids.id_of(ours), ids.id_of(theirs));
    stats.ids_compared += 3;
    if io == it {
        stats.root_fast_path = Some(RootFastPath::SidesAgree);
        return Ok(MergeResult {
            merged_root: ours,
            conflicts: Vec::new(),
            stats,
            identity_proof,
        });
    }
    if ib == io {
        // "If ours == base, take theirs whole." O(1), no descent — and it is a *splice*, not a
        // copy: the merged root IS theirs' root, and not one page is allocated.
        stats.root_fast_path = Some(RootFastPath::OursUnchanged);
        return Ok(MergeResult {
            merged_root: theirs,
            conflicts: Vec::new(),
            stats,
            identity_proof,
        });
    }
    if ib == it {
        stats.root_fast_path = Some(RootFastPath::TheirsUnchanged);
        return Ok(MergeResult {
            merged_root: ours,
            conflicts: Vec::new(),
            stats,
            identity_proof,
        });
    }

    let mut plan = Plan::default();
    descend(tree, ids, None, None, base, ours, theirs, 0, &mut stats, &mut plan)?;

    // Build the merged tree ON OURS. That is what makes the two skip rules above O(1): a subtree
    // ours already holds needs no work, and `edits` is by construction only what theirs contributed.
    let mut root = ours;
    for (key, value) in &plan.edits {
        root = match value {
            Some(v) => tree.insert(root, into, epoch, key, v)?,
            None => tree.delete(root, into, epoch, key)?,
        };
    }
    stats.edits_applied = plan.edits.len();

    Ok(MergeResult { merged_root: root, conflicts: plan.conflicts, stats, identity_proof })
}

/// What the descent accumulated. Keyed, so a key reached twice (possible only where the three trees
/// disagree on *structure*, and then only within one band) resolves to one edit.
#[derive(Default)]
struct Plan {
    edits: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    conflicts: Vec<Conflict>,
}

/// Decode one page as a B+tree node, refusing anything that is not one.
///
/// This is `cid::shape_of`, and deliberately not a second decoder. The one that used to live here
/// matched `BTreeLeaf` and took **everything else** as an internal node — so a `Heap`, a `Free`,
/// an overflow or a page that had just failed to be what it claimed decoded into
/// `n.leftmost()` and `n.internal_entries()`, whose `u32`s the descent then followed as child page
/// ids. A freshly allocated `Heap` page is zeroed, which reads as an internal node with no
/// separators and a leftmost child of **page 0**, so the catch-all was not even loud when it was
/// wrong. `cid::shape_of` names the page type in the error, and
/// `cid::a_non_btree_page_is_refused_rather_than_hashed` is its test.
fn read_node(tree: &CowTree, page: PageId) -> Result<NodeShape, FerroError> {
    cid::shape_of(tree, page)
}

/// One synchronised step over the key range `[lo, hi)`, which all three of `b`/`o`/`t` cover.
///
/// # Why the recursion is keyed on a range and not on a child slot
///
/// Slot-aligned descent is only correct while the three trees have the same shape. They need not:
/// a split on one side changes that side's separators, and a root split changes its *height*. The
/// range is the thing all three agree on by construction, so the invariant "these three subtrees
/// cover exactly `[lo, hi)`" is maintained whatever the shapes are. A side that is still internal
/// descends; a side that has already bottomed out at a leaf stays where it is and is re-read
/// against each sub-range — which can only happen where the shapes diverged, and shapes only
/// diverge where somebody wrote.
#[allow(clippy::too_many_arguments)]
fn descend(
    tree: &CowTree,
    ids: &dyn NodeIdentity,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    b: PageId,
    o: PageId,
    t: PageId,
    depth: usize,
    stats: &mut MergeStats,
    plan: &mut Plan,
) -> Result<(), FerroError> {
    if depth > MAX_DEPTH {
        return Err(FerroError::Cow("merge3 exceeded the depth guard".into()));
    }

    let (ib, io, it) = (ids.id_of(b), ids.id_of(o), ids.id_of(t));
    stats.ids_compared += 3;
    // Rule 1, and the one that pays for the whole design: the two sides agree below here, so
    // whatever base said is irrelevant and ours already holds the answer. Subsumes all-three-equal.
    if io == it {
        stats.skip_sides_agree += 1;
        return Ok(());
    }
    // Rule 2: theirs never touched this subtree. Everything here is ours by row 2 / 6 / 11, and the
    // merged tree is built on ours, so there is literally nothing to do.
    if ib == it {
        stats.skip_theirs_unchanged += 1;
        return Ok(());
    }
    // Rule 3: ours never touched this subtree, so every key here resolves to theirs (rows 3/7/12)
    // and no conflict is reachable. This is NOT a skip — the merged tree is built on ours, so
    // theirs' changes still have to be found. Descending with `b` and `o` equal makes the pair
    // (base, theirs) a two-way diff, and rule 2 above prunes it exactly as `CowTree::diff` would.
    if ib == io {
        stats.descend_ours_unchanged += 1;
    }

    let bv = read_node(tree, b)?;
    let ov = read_node(tree, o)?;
    let tv = read_node(tree, t)?;
    // **Distinct** pages, not three. Rule 3 leaves `b` and `o` as literally the same page id over
    // most of a typical merge, and the second read of it is a hit on a frame this call already
    // pinned. Counting it twice would inflate this file's own curve by half.
    stats.nodes_read += 1 + usize::from(o != b) + usize::from(t != b && t != o);

    if let (NodeShape::Leaf(be), NodeShape::Leaf(oe), NodeShape::Leaf(te)) = (&bv, &ov, &tv) {
        stats.leaf_triples += 1;
        merge_leaves(lo, hi, be, oe, te, stats, plan);
        return Ok(());
    }

    // At least one side is still internal. Cut `[lo, hi)` at every separator any internal side
    // places inside it; each resulting band is covered by exactly one child on every side.
    let mut cuts: Vec<Vec<u8>> = Vec::new();
    for v in [&bv, &ov, &tv] {
        if let NodeShape::Internal(_, seps) = v {
            for (k, _) in seps {
                // STRICTLY inside. A separator equal to `lo` is already this band's lower bound;
                // admitting it would open an empty band that reads three pages to find nothing.
                if lo.map(|l| k.as_slice() > l).unwrap_or(true)
                    && hi.map(|h| k.as_slice() < h).unwrap_or(true)
                {
                    cuts.push(k.clone());
                }
            }
        }
    }
    cuts.sort();
    cuts.dedup();

    let mut left = lo.map(|s| s.to_vec());
    for i in 0..=cuts.len() {
        let right: Option<Vec<u8>> =
            if i < cuts.len() { Some(cuts[i].clone()) } else { hi.map(|s| s.to_vec()) };
        let l = left.as_deref();
        let r = right.as_deref();
        let cb = child_for(&bv, l, b)?;
        let co = child_for(&ov, l, o)?;
        let ct = child_for(&tv, l, t)?;
        descend(tree, ids, l, r, cb, co, ct, depth + 1, stats, plan)?;
        left = right;
    }
    Ok(())
}

/// The child of `v` covering a band whose lower bound is `lo`. A leaf has already bottomed out and
/// stands for itself, which is how a height mismatch between the three trees resolves.
fn child_for(v: &NodeShape, lo: Option<&[u8]>, self_page: PageId) -> Result<PageId, FerroError> {
    Ok(match v {
        NodeShape::Leaf(_) => self_page,
        NodeShape::Internal(leftmost, seps) => match lo {
            // A band open at the bottom can only be served by the leftmost child.
            None => *leftmost,
            // Separators are inclusive lower bounds: a key >= seps[i].0 lives at or right of
            // seps[i].1. The band's own lower bound therefore selects its child.
            Some(k) => match seps.binary_search_by(|(s, _)| s.as_slice().cmp(k)) {
                Ok(i) => seps[i].1,
                Err(0) => *leftmost,
                Err(i) => seps[i - 1].1,
            },
        },
    })
}

fn in_range(k: &[u8], lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
    lo.map(|l| k >= l).unwrap_or(true) && hi.map(|h| k < h).unwrap_or(true)
}

/// Apply the truth table to every key the three leaves place in `[lo, hi)`.
///
/// Only keys whose merged value differs from **ours'** become edits, because the merged tree is
/// built on ours. A key all three agree on produces nothing; so does a key only ours moved.
fn merge_leaves(
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    base: &[(Vec<u8>, Vec<u8>)],
    ours: &[(Vec<u8>, Vec<u8>)],
    theirs: &[(Vec<u8>, Vec<u8>)],
    stats: &mut MergeStats,
    plan: &mut Plan,
) {
    let idx = |e: &[(Vec<u8>, Vec<u8>)]| -> BTreeMap<Vec<u8>, Vec<u8>> {
        e.iter()
            .filter(|(k, _)| in_range(k, lo, hi))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    let (b, o, t) = (idx(base), idx(ours), idx(theirs));

    let mut keys: Vec<&Vec<u8>> = b.keys().chain(o.keys()).chain(t.keys()).collect();
    keys.sort();
    keys.dedup();

    for k in keys {
        stats.keys_compared += 1;
        let (bv, ov, tv) = (b.get(k), o.get(k), t.get(k));
        match resolve_cell(bv, ov, tv) {
            // Already what ours holds — the merged tree is built on ours, so no write.
            Ok(v) if v.as_ref() == ov => {}
            Ok(v) => {
                plan.edits.insert(k.clone(), v);
            }
            Err(kind) => plan.conflicts.push(Conflict {
                key: k.clone(),
                base: bv.cloned(),
                ours: ov.cloned(),
                theirs: tv.cloned(),
                kind,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::cow::page_header::PageType;
    use crate::cow::store::CowStore;
    use crate::cow::PageStore;
    use crate::storage::disk_manager::DiskManager;

    const TRUNK: BranchId = BranchId::TRUNK;

    struct Fx {
        _dir: TempDir,
        store: Arc<CowStore>,
        tree: CowTree,
        clock: AtomicU64,
    }

    impl Fx {
        fn new() -> Fx {
            let dir = TempDir::new().unwrap();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(dir.path().join("merge3.db"))
                .unwrap();
            let dm = Arc::new(DiskManager::new(file).unwrap());
            let pool = Arc::new(BufferPoolManager::new(dm));
            let store = Arc::new(CowStore::with_extent_pages(pool, 64));
            let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);
            // `CowStore::with_extent_pages` registers the trunk itself, at Epoch::ZERO.
            Fx { _dir: dir, store, tree, clock: AtomicU64::new(1) }
        }

        fn tick(&self) -> Epoch {
            Epoch(self.clock.fetch_add(1, Ordering::SeqCst))
        }

        fn branch(&self, id: u64, parent: BranchId) -> BranchId {
            let b = BranchId::new(id, 0);
            let e = self.tick();
            self.store.register_branch(b, Some(parent), e).unwrap();
            b
        }

        fn put(&self, root: PageId, br: BranchId, k: &[u8], v: &[u8]) -> PageId {
            let e = self.tick();
            self.tree.insert(root, br, e, k, v).unwrap()
        }

        fn del(&self, root: PageId, br: BranchId, k: &[u8]) -> PageId {
            let e = self.tick();
            self.tree.delete(root, br, e, k).unwrap()
        }

        fn get(&self, root: PageId, k: &[u8]) -> Option<Vec<u8>> {
            self.tree.get(root, k).unwrap()
        }

        /// A base tree holding `entries`, plus two branches forked off it. The child's root IS the
        /// parent's root — that is the fork, and it is what makes every id comparison below real.
        fn forked(&self, entries: &[(&[u8], &[u8])]) -> (PageId, BranchId, BranchId) {
            let e = self.tick();
            let mut root = self.tree.create(TRUNK, e).unwrap();
            for (k, v) in entries {
                root = self.put(root, TRUNK, k, v);
            }
            let ours = self.branch(2, TRUNK);
            let theirs = self.branch(3, TRUNK);
            (root, ours, theirs)
        }

        fn merge(&self, base: PageId, ours: PageId, theirs: PageId) -> MergeResult {
            let into = BranchId::new(9, 0);
            if self.store.register_branch(into, Some(TRUNK), self.tick()).is_err() {
                // already registered by an earlier call in the same test
            }
            let e = self.tick();
            merge3(&self.tree, base, ours, theirs, &PageIdentity, into, e).unwrap()
        }
    }

    // ---- the truth table, one test per row ----------------------------------------------------
    //
    // Every row drives the same fixture: a base tree, two forks, the row's edits on each side, then
    // a merge. The assertion is on the MERGED TREE, read back through `get` — never on the plan, so
    // a test cannot pass by agreeing with the implementation's bookkeeping.

    fn row(
        base_entries: &[(&[u8], &[u8])],
        ours_edit: Option<Option<&[u8]>>,
        theirs_edit: Option<Option<&[u8]>>,
    ) -> (Fx, MergeResult) {
        let fx = Fx::new();
        let mut seed: Vec<(&[u8], &[u8])> = vec![(b"aaa", b"filler"), (b"zzz", b"filler")];
        seed.extend_from_slice(base_entries);
        let (base, ob, tb) = fx.forked(&seed);
        let apply = |root: PageId, br: BranchId, edit: Option<Option<&[u8]>>| match edit {
            None => root,
            Some(None) => fx.del(root, br, b"k"),
            Some(Some(v)) => fx.put(root, br, b"k", v),
        };
        let ours = apply(base, ob, ours_edit);
        let theirs = apply(base, tb, theirs_edit);
        let r = fx.merge(base, ours, theirs);
        (fx, r)
    }

    #[test]
    fn row_01_all_three_hold_x_keeps_x() {
        // Both sides edited something ELSE, so the root fast paths do not fire and `k` is really
        // walked to. Without the decoy this row would prove nothing about the descent.
        let fx = Fx::new();
        let (base, ob, tb) = fx.forked(&[(b"k", b"x"), (b"o", b"1"), (b"t", b"1")]);
        let ours = fx.put(base, ob, b"o", b"2");
        let theirs = fx.put(base, tb, b"t", b"2");
        let r = fx.merge(base, ours, theirs);
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"x".to_vec()));
    }

    #[test]
    fn row_02_ours_moved_theirs_did_not_takes_ours() {
        let (fx, r) = row(&[(b"k", b"x")], Some(Some(b"y")), Some(Some(b"x")));
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"y".to_vec()));
    }

    #[test]
    fn row_03_theirs_moved_ours_did_not_takes_theirs() {
        let (fx, r) = row(&[(b"k", b"x")], Some(Some(b"x")), Some(Some(b"z")));
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"z".to_vec()));
    }

    #[test]
    fn row_04_convergent_edit_is_not_a_conflict() {
        let (fx, r) = row(&[(b"k", b"x")], Some(Some(b"y")), Some(Some(b"y")));
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"y".to_vec()));
    }

    #[test]
    fn row_05_both_moved_to_different_values_conflicts() {
        let (fx, r) = row(&[(b"k", b"x")], Some(Some(b"y")), Some(Some(b"z")));
        assert_eq!(r.conflicts.len(), 1);
        assert_eq!(r.conflicts[0].kind, ConflictKind::BothModified);
        assert_eq!(r.conflicts[0].base, Some(b"x".to_vec()));
        assert_eq!(r.conflicts[0].ours, Some(b"y".to_vec()));
        assert_eq!(r.conflicts[0].theirs, Some(b"z".to_vec()));
        // The merged tree still exists and holds ours, as the module brief states.
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"y".to_vec()));
    }

    #[test]
    fn row_06_ours_deleted_theirs_unchanged_deletes() {
        let (fx, r) = row(&[(b"k", b"x")], Some(None), Some(Some(b"x")));
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), None);
    }

    #[test]
    fn row_07_theirs_deleted_ours_unchanged_deletes() {
        let (fx, r) = row(&[(b"k", b"x")], Some(Some(b"x")), Some(None));
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), None);
    }

    #[test]
    fn row_08_ours_deleted_theirs_modified_conflicts() {
        let (fx, r) = row(&[(b"k", b"x")], Some(None), Some(Some(b"z")));
        assert_eq!(r.conflicts.len(), 1);
        assert_eq!(r.conflicts[0].kind, ConflictKind::OursDeletedTheirsModified);
        assert_eq!(r.conflicts[0].ours, None);
        assert_eq!(r.conflicts[0].theirs, Some(b"z".to_vec()));
        assert_eq!(fx.get(r.merged_root, b"k"), None);
    }

    #[test]
    fn row_09_ours_modified_theirs_deleted_conflicts() {
        let (fx, r) = row(&[(b"k", b"x")], Some(Some(b"y")), Some(None));
        assert_eq!(r.conflicts.len(), 1);
        assert_eq!(r.conflicts[0].kind, ConflictKind::OursModifiedTheirsDeleted);
        assert_eq!(r.conflicts[0].ours, Some(b"y".to_vec()));
        assert_eq!(r.conflicts[0].theirs, None);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"y".to_vec()));
    }

    #[test]
    fn row_10_both_deleted_is_convergent_not_a_conflict() {
        let (fx, r) = row(&[(b"k", b"x")], Some(None), Some(None));
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), None);
    }

    #[test]
    fn row_11_ours_inserted_theirs_absent_takes_ours() {
        // `theirs` must move something OTHER than `k`, or the root fast path retires the merge
        // before the descent this row is about ever runs.
        let fx = Fx::new();
        let (base, ob, tb) = fx.forked(&[(b"aaa", b"f"), (b"zzz", b"f")]);
        let ours = fx.put(base, ob, b"k", b"y");
        let theirs = fx.put(base, tb, b"zzz", b"moved");
        let r = fx.merge(base, ours, theirs);
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"y".to_vec()));
        assert_eq!(fx.get(r.merged_root, b"zzz"), Some(b"moved".to_vec()));
    }

    #[test]
    fn row_12_theirs_inserted_ours_absent_takes_theirs() {
        let fx = Fx::new();
        let (base, ob, tb) = fx.forked(&[(b"aaa", b"f"), (b"zzz", b"f")]);
        let ours = fx.put(base, ob, b"aaa", b"moved");
        let theirs = fx.put(base, tb, b"k", b"z");
        let r = fx.merge(base, ours, theirs);
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"z".to_vec()));
    }

    #[test]
    fn row_13_convergent_insert_is_not_a_conflict() {
        let fx = Fx::new();
        let (base, ob, tb) = fx.forked(&[(b"aaa", b"f"), (b"zzz", b"f")]);
        let ours = fx.put(base, ob, b"k", b"y");
        let theirs = fx.put(base, tb, b"k", b"y");
        let r = fx.merge(base, ours, theirs);
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"y".to_vec()));
    }

    #[test]
    fn row_14_add_add_with_different_values_conflicts() {
        let fx = Fx::new();
        let (base, ob, tb) = fx.forked(&[(b"aaa", b"f"), (b"zzz", b"f")]);
        let ours = fx.put(base, ob, b"k", b"y");
        let theirs = fx.put(base, tb, b"k", b"z");
        let r = fx.merge(base, ours, theirs);
        assert_eq!(r.conflicts.len(), 1);
        assert_eq!(r.conflicts[0].kind, ConflictKind::BothModified);
        assert_eq!(r.conflicts[0].base, None);
        assert_eq!(fx.get(r.merged_root, b"k"), Some(b"y".to_vec()));
    }

    // ---- the rule itself, without a tree under it ---------------------------------------------

    #[test]
    fn resolve_cell_covers_the_table_exhaustively() {
        let x = Some(b"x".to_vec());
        let y = Some(b"y".to_vec());
        let z = Some(b"z".to_vec());
        let n: Option<Vec<u8>> = None;
        let c = |b: &Option<Vec<u8>>, o: &Option<Vec<u8>>, t: &Option<Vec<u8>>| {
            resolve_cell(b.as_ref(), o.as_ref(), t.as_ref())
        };
        assert_eq!(c(&x, &x, &x), Ok(x.clone())); // 1
        assert_eq!(c(&x, &y, &x), Ok(y.clone())); // 2
        assert_eq!(c(&x, &x, &z), Ok(z.clone())); // 3
        assert_eq!(c(&x, &y, &y), Ok(y.clone())); // 4
        assert_eq!(c(&x, &y, &z), Err(ConflictKind::BothModified)); // 5
        assert_eq!(c(&x, &n, &x), Ok(None)); // 6
        assert_eq!(c(&x, &x, &n), Ok(None)); // 7
        assert_eq!(c(&x, &n, &z), Err(ConflictKind::OursDeletedTheirsModified)); // 8
        assert_eq!(c(&x, &y, &n), Err(ConflictKind::OursModifiedTheirsDeleted)); // 9
        assert_eq!(c(&x, &n, &n), Ok(None)); // 10
        assert_eq!(c(&n, &y, &n), Ok(y.clone())); // 11
        assert_eq!(c(&n, &n, &z), Ok(z.clone())); // 12
        assert_eq!(c(&n, &y, &y), Ok(y.clone())); // 13
        assert_eq!(c(&n, &y, &z), Err(ConflictKind::BothModified)); // 14
    }

    // ---- the three O(1) root rules ------------------------------------------------------------

    #[test]
    fn ours_unchanged_takes_theirs_whole_without_reading_a_page() {
        let fx = Fx::new();
        let (base, _ob, tb) = fx.forked(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);
        let theirs = fx.put(base, tb, b"b", b"CHANGED");
        let r = fx.merge(base, base, theirs);
        assert_eq!(r.stats.root_fast_path, Some(RootFastPath::OursUnchanged));
        assert_eq!(r.stats.nodes_read, 0, "the fast path must not read a page");
        // A splice, not a copy: the merged root IS theirs' root.
        assert_eq!(r.merged_root, theirs);
        assert_eq!(fx.get(r.merged_root, b"b"), Some(b"CHANGED".to_vec()));
    }

    #[test]
    fn theirs_unchanged_takes_ours_whole_without_reading_a_page() {
        let fx = Fx::new();
        let (base, ob, _tb) = fx.forked(&[(b"a", b"1"), (b"b", b"2")]);
        let ours = fx.put(base, ob, b"a", b"CHANGED");
        let r = fx.merge(base, ours, base);
        assert_eq!(r.stats.root_fast_path, Some(RootFastPath::TheirsUnchanged));
        assert_eq!(r.stats.nodes_read, 0);
        assert_eq!(r.merged_root, ours);
    }

    #[test]
    fn sides_agree_takes_either_without_reading_a_page() {
        let fx = Fx::new();
        let (base, ob, _tb) = fx.forked(&[(b"a", b"1")]);
        let ours = fx.put(base, ob, b"a", b"2");
        let r = fx.merge(base, ours, ours);
        assert_eq!(r.stats.root_fast_path, Some(RootFastPath::SidesAgree));
        assert_eq!(r.stats.nodes_read, 0);
        assert_eq!(r.merged_root, ours);
    }

    // ---- the detector has to be able to fire --------------------------------------------------

    #[test]
    fn three_unrelated_trees_produce_zero_skips() {
        // Not forks. Three trees built independently over the same keys with DIFFERENT values, so
        // no page id is shared anywhere and no subtree rule can fire. If the skip counters are
        // nonzero here they are counting something other than what they claim.
        let fx = Fx::new();
        let mk = |br: BranchId, tag: u8| {
            let e = fx.tick();
            let mut root = fx.tree.create(br, e).unwrap();
            for i in 0..200u32 {
                root = fx.put(root, br, &i.to_be_bytes(), &[tag; 24]);
            }
            root
        };
        let b1 = fx.branch(11, TRUNK);
        let b2 = fx.branch(12, TRUNK);
        let b3 = fx.branch(13, TRUNK);
        let (base, ours, theirs) = (mk(b1, 1), mk(b2, 2), mk(b3, 3));

        let r = fx.merge(base, ours, theirs);
        assert_eq!(r.stats.root_fast_path, None);
        assert_eq!(r.stats.skips(), 0, "three unrelated trees share no subtree: {:?}", r.stats);
        assert!(r.stats.nodes_read > 0);
        // And it is not vacuous: all 200 keys were really compared and all 200 really conflict.
        assert_eq!(r.stats.keys_compared, 200);
        assert_eq!(r.conflicts.len(), 200);
    }

    #[test]
    fn the_same_workload_on_forks_skips_almost_everything() {
        // The control for the test above: identical key space, but built by FORKING, so the skip
        // rules can see shared subtrees. A detector that fires in both is measuring nothing.
        let fx = Fx::new();
        let mut seed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..200u32 {
            seed.push((i.to_be_bytes().to_vec(), vec![1u8; 24]));
        }
        let e = fx.tick();
        let mut base = fx.tree.create(TRUNK, e).unwrap();
        for (k, v) in &seed {
            base = fx.put(base, TRUNK, k, v);
        }
        let ob = fx.branch(21, TRUNK);
        let tb = fx.branch(22, TRUNK);
        let ours = fx.put(base, ob, &7u32.to_be_bytes(), b"ours");
        let theirs = fx.put(base, tb, &150u32.to_be_bytes(), b"theirs");

        let r = fx.merge(base, ours, theirs);
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert!(r.stats.skips() > 0, "forked trees must skip: {:?}", r.stats);
        assert!(
            r.stats.keys_compared < 200,
            "a skip that still compares every key is not a skip: {:?}",
            r.stats
        );
        assert_eq!(fx.get(r.merged_root, &7u32.to_be_bytes()), Some(b"ours".to_vec()));
        assert_eq!(fx.get(r.merged_root, &150u32.to_be_bytes()), Some(b"theirs".to_vec()));
    }

    // ---- the merged tree has to be a whole tree, not just the changed keys --------------------

    #[test]
    fn every_untouched_key_survives_the_merge() {
        let fx = Fx::new();
        let e = fx.tick();
        let mut base = fx.tree.create(TRUNK, e).unwrap();
        for i in 0..400u32 {
            base = fx.put(base, TRUNK, &i.to_be_bytes(), &[9u8; 20]);
        }
        let ob = fx.branch(31, TRUNK);
        let tb = fx.branch(32, TRUNK);
        let mut ours = base;
        for i in [3u32, 44, 111, 399] {
            ours = fx.put(ours, ob, &i.to_be_bytes(), b"OURS");
        }
        let mut theirs = base;
        for i in [7u32, 88, 222, 350] {
            theirs = fx.put(theirs, tb, &i.to_be_bytes(), b"THEIRS");
        }
        let r = fx.merge(base, ours, theirs);
        assert!(r.is_clean(), "{:?}", r.conflicts);
        for i in 0..400u32 {
            let want = match i {
                3 | 44 | 111 | 399 => b"OURS".to_vec(),
                7 | 88 | 222 | 350 => b"THEIRS".to_vec(),
                _ => vec![9u8; 20],
            };
            assert_eq!(fx.get(r.merged_root, &i.to_be_bytes()), Some(want), "key {i}");
        }
    }

    #[test]
    fn a_merge_that_splits_a_leaf_on_both_sides_still_merges() {
        // Both sides insert enough NEW keys in the same region to force splits, so the two trees
        // diverge structurally and the separator-union path is exercised rather than the aligned
        // one. The assertion is on every key, so a lost band shows up.
        let fx = Fx::new();
        let e = fx.tick();
        let mut base = fx.tree.create(TRUNK, e).unwrap();
        for i in (0..2000u32).step_by(4) {
            base = fx.put(base, TRUNK, &i.to_be_bytes(), &[5u8; 60]);
        }
        let ob = fx.branch(41, TRUNK);
        let tb = fx.branch(42, TRUNK);
        let mut ours = base;
        for i in (1..2000u32).step_by(4) {
            ours = fx.put(ours, ob, &i.to_be_bytes(), b"O");
        }
        let mut theirs = base;
        for i in (2..2000u32).step_by(4) {
            theirs = fx.put(theirs, tb, &i.to_be_bytes(), b"T");
        }
        let r = fx.merge(base, ours, theirs);
        assert!(r.is_clean(), "{:?}", r.conflicts);
        for i in 0..2000u32 {
            let want = match i % 4 {
                0 => Some(vec![5u8; 60]),
                1 => Some(b"O".to_vec()),
                2 => Some(b"T".to_vec()),
                _ => None,
            };
            assert_eq!(fx.get(r.merged_root, &i.to_be_bytes()), want, "key {i}");
        }
    }

    #[test]
    fn merging_the_other_direction_agrees_except_on_which_side_wins_a_conflict() {
        // Structural merge is symmetric on every clean row. Asserting that catches an asymmetry
        // bug that a one-direction test cannot see.
        let fx = Fx::new();
        let (base, ob, tb) = fx.forked(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);
        let ours = fx.put(base, ob, b"a", b"OURS");
        let theirs = fx.put(base, tb, b"c", b"THEIRS");
        let fwd = fx.merge(base, ours, theirs);
        let rev = fx.merge(base, theirs, ours);
        assert!(fwd.is_clean() && rev.is_clean());
        for k in [&b"a"[..], b"b", b"c"] {
            assert_eq!(fx.get(fwd.merged_root, k), fx.get(rev.merged_root, k), "key {k:?}");
        }
    }

    // ---- content identity buys the convergent-edit skip, and what it costs to do it -----------

    #[test]
    fn content_identity_skips_a_convergent_edit_that_page_identity_must_descend() {
        // Both sides make the SAME edit. The resulting subtrees are byte-identical but live at
        // different page ids, so page identity cannot see the agreement and descends; a content
        // id can — see `bench/d92_content_identity_trade.txt` for what that costs.
        let fx = Fx::new();
        let e = fx.tick();
        let mut base = fx.tree.create(TRUNK, e).unwrap();
        for i in 0..300u32 {
            base = fx.put(base, TRUNK, &i.to_be_bytes(), &[3u8; 30]);
        }
        let ob = fx.branch(51, TRUNK);
        let tb = fx.branch(52, TRUNK);
        let ours = fx.put(base, ob, &100u32.to_be_bytes(), b"same");
        let theirs = fx.put(base, tb, &100u32.to_be_bytes(), b"same");

        let into = BranchId::new(59, 0);
        fx.store.register_branch(into, Some(TRUNK), fx.tick()).unwrap();

        let shadow = merge3(&fx.tree, base, ours, theirs, &PageIdentity, into, fx.tick()).unwrap();
        assert_eq!(shadow.stats.root_fast_path, None, "different page ids at the root");
        assert!(shadow.stats.nodes_read > 0, "page identity has to descend to find the agreement");

        // Content identity, from `cow::diff` — merge3 owns no hasher of its own. The stamping is
        // the caller's explicit, O(N) precompute, which is exactly the cost
        // `bench/d92_content_identity_trade.txt` measures and argues is not worth paying here.
        let hash = SubtreeHash::new(fx.store.clone() as Arc<dyn PageStore>);
        for r in [base, ours, theirs] {
            hash.stamp(r).unwrap();
        }
        let m = merge3(&fx.tree, base, ours, theirs, &hash, into, fx.tick()).unwrap();
        assert_eq!(
            m.stats.root_fast_path,
            Some(RootFastPath::SidesAgree),
            "identical content must be identical identity"
        );
        assert_eq!(m.stats.nodes_read, 0);
        assert!(m.is_clean());
        // ...and the result says what that empty conflict list rests on. A fingerprint match, not
        // a proof — which is exactly the difference `shadow` above demonstrates it is worth.
        assert_eq!(m.identity_proof, IdentityProof::Fingerprint);
        assert_eq!(shadow.identity_proof, IdentityProof::Exact);
        assert_eq!(fx.get(m.merged_root, &100u32.to_be_bytes()), Some(b"same".to_vec()));
        // And the cost that buys it is real and reported, not hidden: every node under all three
        // roots had to be folded before the merge could compare three ids.
        assert!(
            hash.stamped_nodes() > 3 * shadow.stats.nodes_read,
            "stamping {} nodes to save {} page reads is the trade this test exists to show",
            hash.stamped_nodes(),
            shadow.stats.nodes_read
        );
    }

    // ---- the memo staleness guard, and that it HOLDS ------------------------------------------

    /// **A guard that `cow::diff`'s version key holds. This was a pinned HAZARD until `fddf13c`.**
    ///
    /// History, kept because the guard is only legible with it. The review that started this work
    /// found `merge3`'s own `MerkleId` memo keyed on `PageId`, arguing it could never go stale
    /// because "a modified page gets a NEW id". That is false by two routes, neither needing a
    /// reap:
    ///
    /// 1. `ArenaPageStore::cow_page` hands a branch its own page straight back for **in-place**
    ///    mutation once it owns the arena and the page was born at or after its privacy barrier.
    ///    Same page id, different contents, and `birth_epoch` is **not** restamped — which is why
    ///    the first proposed repair, `(PageId, birth_epoch)`, would not have caught it. This test
    ///    drives that route.
    /// 2. `ArenaPageStore` recycles freed page ids, which `birth_epoch` *does* catch.
    ///
    /// `merge3` owns no memo any more, but the defect did **not** die with it: `cow::diff`'s
    /// `SubtreeHash` and `MemoIdentity` were keyed on `PageId` too. This test used to pin that as
    /// a silent wrong answer — a merge reported clean with the other side's edit dropped — and
    /// said in its own doc that the day it failed would be the day the fix landed.
    ///
    /// **It failed. `fddf13c` keyed the memos on `(birth_epoch, checksum)`** — crc32 covers the
    /// header, so it moves on an in-place write where `birth_epoch` alone does not. So the test is
    /// inverted rather than deleted: it now asserts the fix, and will fail again if the version
    /// key is ever weakened back.
    ///
    /// It asserts the **mechanism** and not only the outcome. A correct merge here could also come
    /// from the provider never having stamped anything, so the middle assertion checks that the
    /// row was taken and is then *refused* — which is the version key doing its job.
    ///
    /// Route C — an in-place write to a *descendant* — is still open by construction and no
    /// per-page key reaches it; `cow::diff` asserts that limit on its own side, and
    /// `birth_epoch_discriminates_a_recycled_page_but_not_an_in_place_write` measures all three.
    #[test]
    fn a_subtree_hash_reused_across_an_in_place_write_refuses_its_stale_row() {
        let fx = Fx::new();
        let (base, ob, tb) = fx.forked(&[(b"k", b"v0")]);

        // Convergent edit: ours and theirs hold equal CONTENT at different page ids, so content
        // identity retires the whole merge at the root and the stamp is really consulted.
        let ours = fx.put(base, ob, b"a", b"1");
        let theirs1 = fx.put(base, tb, b"a", b"1");
        assert_ne!(ours, theirs1, "a convergent edit must land on two different page ids");

        let into = BranchId::new(71, 0);
        fx.store.register_branch(into, Some(TRUNK), fx.tick()).unwrap();

        let hash = SubtreeHash::new(fx.store.clone() as Arc<dyn PageStore>);
        for r in [base, ours, theirs1] {
            hash.stamp(r).unwrap();
        }
        let first = merge3(&fx.tree, base, ours, theirs1, &hash, into, fx.tick()).unwrap();
        assert_eq!(
            first.stats.root_fast_path,
            Some(RootFastPath::SidesAgree),
            "the stamps have to be populated by a merge that really compared the two roots"
        );
        assert!(
            hash.nodes_under(theirs1).is_some(),
            "precondition: theirs' root must be stamped before we invalidate it"
        );

        // The write that used to break everything. If the store ever stops taking the in-place
        // path here, this says so rather than the test quietly proving nothing.
        let theirs2 = fx.put(theirs1, tb, b"k", b"vT");
        assert_eq!(
            theirs2, theirs1,
            "this test needs cow_page's in-place arm; the store shadowed the page instead"
        );

        // THE MECHANISM. The row is still in the memo, and the provider must now refuse it.
        assert!(
            hash.nodes_under(theirs2).is_none(),
            "the stamp must go stale when its page is rewritten in place — if this is Some, the \
             version key has been weakened and the merge below is about to drop a change"
        );

        // THE CONSEQUENCE. Same provider, reused across the write, and the merge is correct.
        let r = merge3(&fx.tree, base, ours, theirs2, &hash, into, fx.tick()).unwrap();
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert_eq!(
            fx.get(r.merged_root, b"k"),
            Some(b"vT".to_vec()),
            "a reused provider must not drop theirs' edit"
        );
    }

    /// A page that is not a B+tree node must be refused, not decoded as an internal node and its
    /// `u32`s followed as child page ids. A freshly allocated `Heap` page is zeroed, so the arm
    /// that used to catch it read an internal node with no separators and a leftmost child of
    /// page 0 — wrong, and silent about being wrong.
    #[test]
    fn a_non_btree_page_is_refused_rather_than_walked_as_an_internal_node() {
        let fx = Fx::new();
        let (base, ob, tb) = fx.forked(&[(b"k", b"v0")]);
        let ours = fx.put(base, ob, b"a", b"1");
        let into = BranchId::new(73, 0);
        fx.store.register_branch(into, Some(TRUNK), fx.tick()).unwrap();

        let heap = fx.store.alloc_for(tb, PageType::Heap, fx.tick()).unwrap();
        let err =
            merge3(&fx.tree, base, ours, heap, &PageIdentity, into, fx.tick()).unwrap_err();
        assert!(
            format!("{err}").contains("not a btree node"),
            "expected a refusal naming the page type, got: {err}"
        );
    }

    // ---- the curve's own instrument, checked at a size a human can verify ---------------------

    #[test]
    fn nodes_read_is_far_below_the_page_count() {
        let fx = Fx::new();
        let e = fx.tick();
        let mut base = fx.tree.create(TRUNK, e).unwrap();
        for i in 0..4000u32 {
            base = fx.put(base, TRUNK, &i.to_be_bytes(), &[7u8; 60]);
        }
        let pages = fx.tree.walk_pages(base).unwrap().len();
        let ob = fx.branch(61, TRUNK);
        let tb = fx.branch(62, TRUNK);
        let mut ours = base;
        for i in [10u32, 1000, 2000, 3000] {
            ours = fx.put(ours, ob, &i.to_be_bytes(), b"O");
        }
        let mut theirs = base;
        for i in [20u32, 1010, 2010, 3010] {
            theirs = fx.put(theirs, tb, &i.to_be_bytes(), b"T");
        }
        let r = fx.merge(base, ours, theirs);
        assert!(r.is_clean(), "{:?}", r.conflicts);
        assert!(
            r.stats.nodes_read * 4 < pages,
            "merge read {} of {} pages — that is not a structural merge",
            r.stats.nodes_read,
            pages
        );
    }

    /// **What a memo may be keyed on, measured rather than argued.**
    ///
    /// Two memos in `src/cow/` were keyed on `PageId` and both went stale. The repair first
    /// proposed was `(PageId, birth_epoch)`, on the grounds that a recycled page is restamped with
    /// a fresh epoch — true, and one of three routes. This test measures all three, so the next
    /// person proposing a per-page key is answered by the suite rather than by a document.
    ///
    /// **What actually shipped is `(birth_epoch, checksum)`** (`cow::diff::PageVersion`, landed in
    /// `fddf13c`), which catches A **and** B: crc32 covers the header, so it moves on an in-place
    /// write where `birth_epoch` alone does not. This test is about `birth_epoch`, which is still
    /// exactly as measured below, and it is deliberately **not** duplicated into `diff.rs` — two
    /// tests asserting one store property is the shape where a mutant in either is masked by the
    /// other. Route C is asserted as a limit on `diff`'s side too.
    ///
    /// | route | discriminates? |
    /// |---|---|
    /// | A — a freed page id handed out again | **yes**, `write_fresh_page` restamps |
    /// | B — an in-place write to the page itself | **no** |
    /// | C — an in-place write to a DESCENDANT | **no** |
    ///
    /// B is `ArenaPageStore::cow_page`'s in-place arm: when the branch owns the arena and the page
    /// was born at or after its privacy barrier, the page is handed back for mutation and the
    /// function returns *before* touching the header, so `birth_epoch` is not restamped. It needs
    /// no reap and fires on a branch's second write.
    ///
    /// C is the one that cannot be repaired by choosing a better key, and it is the case that
    /// matters for a **recursive** digest like `cow::diff::SubtreeHash` or [`cid::subtree_cid`]:
    /// mutate a descendant in place and the ancestor's own bytes never change, so its id, epoch,
    /// page type and checksum are all identical while the subtree id it stands for is not. Any
    /// per-page key is blind here, by construction.
    ///
    /// The conclusion the callers need, and it survives the better key: **no per-page key makes a
    /// recursive digest's memo sound**, because of route C. `(birth_epoch, checksum)` closes A and
    /// B and leaves C open by construction. The only scope that is sound is a window in which
    /// nothing writes to the stamped trees — stamp, use, discard.
    #[test]
    fn birth_epoch_discriminates_a_recycled_page_but_not_an_in_place_write() {
        use crate::cow::page_header::PageHeader;
        let key = |fx: &Fx, p: PageId| -> (PageId, u64) {
            let h = fx.store.read_page(p).unwrap();
            let f = h.read();
            (p, PageHeader::read_from(&f.data).unwrap().birth_epoch.0)
        };

        // ---- A. A freed page id comes back, carrying a fresh epoch. ----
        let fx = Fx::new();
        let br = fx.branch(81, TRUNK);
        let arena = fx.store.arena_for(br).unwrap();
        let p1 = fx.store.alloc_in_arena(arena, PageType::Heap, fx.tick()).unwrap();
        let a_before = key(&fx, p1);
        fx.store.free_page(p1, fx.tick()).unwrap();
        let p2 = fx.store.alloc_in_arena(arena, PageType::Heap, fx.tick()).unwrap();
        assert_eq!(p2, p1, "this route needs the id to actually be recycled");
        assert_ne!(
            a_before,
            key(&fx, p2),
            "route A: a recycled page must carry a fresh birth_epoch, or the proposed key buys \
             nothing at all"
        );

        // ---- B. An in-place write leaves the key untouched. ----
        let fx = Fx::new();
        let (base, _ob, tb) = fx.forked(&[(b"k", b"v0")]);
        let t1 = fx.put(base, tb, b"a", b"1"); // copies out of trunk's arena
        let b_before = key(&fx, t1);
        let cid_before = cid::subtree_cid(&fx.tree, t1).unwrap();
        let t2 = fx.put(t1, tb, b"k", b"vT"); // second write: in place
        assert_eq!(t2, t1, "route B needs cow_page's in-place arm; the store took a copy instead");
        assert_ne!(cid_before, cid::subtree_cid(&fx.tree, t2).unwrap(), "contents really changed");
        assert_eq!(
            b_before,
            key(&fx, t2),
            "route B: (PageId, birth_epoch) is UNCHANGED across an in-place write. If this now \
             differs, cow_page restamps and this half of the hazard is gone — say so where the \
             memos cite it."
        );

        // ---- C. And the ancestor of an in-place write is blind to it. ----
        let fx = Fx::new();
        let e = fx.tick();
        let mut root = fx.tree.create(TRUNK, e).unwrap();
        for i in 0..400u32 {
            root = fx.put(root, TRUNK, &i.to_be_bytes(), &[7u8; 40]);
        }
        let wb = fx.branch(83, TRUNK);
        let r1 = fx.put(root, wb, &10u32.to_be_bytes(), b"first");
        assert!(
            matches!(cid::shape_of(&fx.tree, r1).unwrap(), NodeShape::Internal(..)),
            "route C is only meaningful with an internal root"
        );
        let c_before = key(&fx, r1);
        let c_cid_before = cid::subtree_cid(&fx.tree, r1).unwrap();
        let r2 = fx.put(r1, wb, &11u32.to_be_bytes(), b"second");
        assert_eq!(r2, r1, "route C needs the root to be mutated in place");
        assert_ne!(
            c_cid_before,
            cid::subtree_cid(&fx.tree, r2).unwrap(),
            "the subtree digest really did change"
        );
        assert_eq!(
            c_before,
            key(&fx, r2),
            "route C: the ancestor's (PageId, birth_epoch) is UNCHANGED while its subtree digest \
             moved. This is why no per-page key can make a recursive digest's memo sound."
        );
    }
}
