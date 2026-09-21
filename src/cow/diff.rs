//! Subtree-skipping structural diff over two CoW B+tree roots (D91).
//!
//! # What was already here, and what was missing
//!
//! [`crate::cow::btree::CowTree::diff`] already skips shared subtrees — but it finds them by
//! building a `HashSet<PageId>` of **every page reachable from each root** first, via two
//! `walk_pages` calls. Its own doc comment says so plainly: "page *identity* traversal is
//! proportional to the tree". So entry *decoding* is O(delta) there and total traversal is O(N).
//! For an agent branch that changed four rows in a million-row table, that is a million page
//! reads to discover four changes.
//!
//! ForkBase (arXiv:1802.04949) and Dolt reach O(delta · log_m N) *including* traversal, because
//! the two versions are descended **together**: at each level the children are compared pairwise,
//! an equal pair is skipped whole in O(1), and only unequal pairs are descended into. No side is
//! ever enumerated on its own. That is what this module implements.
//!
//! # Identity is supplied, not assumed
//!
//! The skip test is `identity.id_of(a) == identity.id_of(b)`, and [`NodeIdentity`] is a parameter.
//! Two providers ship here:
//!
//! * [`PageIdentity`] — the page id *is* the identity. Sound in this store and nowhere else:
//!   DESIGN.md rules out content addressing and refcounts, so a subtree that did not change is
//!   not merely equal to its old self, it is the same page. Zero precompute, zero collision risk,
//!   and it is the provider to prefer inside ferrodb.
//! * [`SubtreeHash`] — a content hash folded bottom-up, which is what a content-addressed store
//!   compares. It is here so this file is not blocked on `cow::cid`, and it is a **placeholder**:
//!   see its own docs for why a collision is worse here than in most places.
//!
//! Swapping in `cow::cid`'s digest is one line: implement [`NodeIdentity`] for it.
//!
//! # The cost claim is measured, not asserted
//!
//! [`DiffStats`] counts nodes whose payload was decoded (`visited`) and skip events
//! (`skipped_subtrees`). `examples/d91_diff_curve.rs` drives N over 1k..256k with exactly four
//! rows changed and banks the curve to `bench/d91_diff_skip_curve.txt`. A single before/after
//! ratio cannot separate a constant from a complexity class; the slope can.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

use crate::branch::types::PageId;
use crate::consensus::replicate::{fnv64_update, FNV_OFFSET};
use crate::cow::btree::CowTree;
use crate::cow::node::Node;
use crate::cow::page_header::{PageHeader, PageType};
use crate::cow::PageStore;
use crate::error::FerroError;

/// Descent guard, matching `btree::MAX_DESCENT`. A well-formed tree is far shallower; exceeding
/// it means a cycle, and looping forever inside a page store is worse than failing.
const MAX_DESCENT: usize = 64;

// -------------------------------------------------------------------------------------------
// Identity
// -------------------------------------------------------------------------------------------

/// How two subtrees are tested for equality without reading either one.
///
/// The whole mechanism rests on this being **O(1)** and **sound in one direction**: equal ids
/// must imply equal subtrees. The converse is not required — a provider that reports two equal
/// subtrees as different only costs work, while one that reports two different subtrees as equal
/// silently drops a change from the diff.
pub trait NodeIdentity {
    fn id_of(&self, page: PageId) -> [u8; 16];
}

/// Byte 0 of an id says which domain produced it, so the two can never compare equal.
///
/// This is not decoration. [`SubtreeHash`] falls back to page identity for a page it has not
/// stamped, and without a reserved tag a fallback value could in principle equal some other
/// page's real content hash — a false skip, i.e. a silently missing change. Reserving one byte
/// makes that unrepresentable rather than merely unlikely, at the cost of 8 hash bits.
const TAG_CONTENT: u8 = 0x00;
const TAG_PAGE: u8 = 0x01;

/// The page id, widened to an id. Sound **only** in a store that never mutates a shared page in
/// place, which is exactly what copy-on-write guarantees and why ferrodb can use it.
fn page_id_identity(page: PageId) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0] = TAG_PAGE;
    out[1..5].copy_from_slice(&page.to_be_bytes());
    out
}

/// Identity by page id. No precompute, no hashing, no collisions.
#[derive(Debug, Default, Clone, Copy)]
pub struct PageIdentity;

impl NodeIdentity for PageIdentity {
    fn id_of(&self, page: PageId) -> [u8; 16] {
        page_id_identity(page)
    }
}

/// One memo row: the subtree's content id and how many nodes are under it.
#[derive(Debug, Clone, Copy)]
struct Stamp {
    id: [u8; 16],
    nodes: usize,
}

/// Content identity, folded bottom-up: a leaf commits to its entries, an internal node commits to
/// its children's ids and separators.
///
/// **This is a placeholder and the docs say so on purpose.** 120 effective bits of FNV-1a is not
/// a cryptographic commitment, and a collision here does not produce a loud wrong answer — it
/// produces a *skip*, so the change inside that subtree is silently absent from the diff. FNV is
/// used because this crate carries zero runtime dependencies (a product claim, see
/// `consensus::replicate::fnv64_update`) and because `cow::cid` is landing a real digest; when it
/// does, the swap is one `impl NodeIdentity`.
///
/// [`SubtreeHash::stamp`] walks a root once and memoises. That walk is **O(N) and it is not part
/// of the diff's cost** — it models the write-time cid stamping that ForkBase does as nodes are
/// built. A harness that wants an honest diff-time number stamps both roots before it starts
/// timing, and `examples/d91_diff_curve.rs` does.
pub struct SubtreeHash {
    store: Arc<dyn PageStore>,
    memo: RwLock<HashMap<PageId, Stamp>>,
}

impl SubtreeHash {
    pub fn new(store: Arc<dyn PageStore>) -> Self {
        SubtreeHash { store, memo: RwLock::new(HashMap::new()) }
    }

    /// Fold the subtree at `root`, memoising every node on the way. Returns the root's id.
    pub fn stamp(&self, root: PageId) -> Result<[u8; 16], FerroError> {
        self.stamp_inner(root, 0)
    }

    /// Nodes under `page`, if it has been stamped. `None` means "not stamped", never "zero".
    pub fn nodes_under(&self, page: PageId) -> Option<usize> {
        self.memo.read().unwrap().get(&page).map(|s| s.nodes)
    }

    /// How many nodes have been stamped. Diagnostic; a harness uses it to prove the precompute
    /// actually ran rather than silently no-oped.
    pub fn stamped_nodes(&self) -> usize {
        self.memo.read().unwrap().len()
    }

    fn stamp_inner(&self, page: PageId, depth: usize) -> Result<[u8; 16], FerroError> {
        if depth > MAX_DESCENT {
            return Err(FerroError::Cow("subtree hash exceeded the depth guard".into()));
        }
        if let Some(s) = self.memo.read().unwrap().get(&page) {
            return Ok(s.id);
        }
        let payload = read_payload(&self.store, page, &Span::unbounded())?;
        let stamp = match payload {
            Payload::Leaf(entries) => {
                let mut a = fnv64_update(FNV_OFFSET, b"ferrodb/d91/leaf");
                let mut b = fnv64_update(SECOND_IV, b"ferrodb/d91/leaf");
                a = fold_u64(a, entries.len() as u64);
                b = fold_u64(b, entries.len() as u64);
                for (k, v) in &entries {
                    a = fold_bytes(a, k);
                    a = fold_bytes(a, v);
                    b = fold_bytes(b, v);
                    b = fold_bytes(b, k);
                }
                Stamp { id: compose(a, b), nodes: 1 }
            }
            Payload::Internal(children) => {
                let mut a = fnv64_update(FNV_OFFSET, b"ferrodb/d91/internal");
                let mut b = fnv64_update(SECOND_IV, b"ferrodb/d91/internal");
                a = fold_u64(a, children.len() as u64);
                b = fold_u64(b, children.len() as u64);
                let mut nodes = 1usize;
                for (span, child) in &children {
                    let child_id = self.stamp_inner(*child, depth + 1)?;
                    nodes += self.nodes_under(*child).unwrap_or(1);
                    let sep = span.lo.clone().unwrap_or_default();
                    a = fold_bytes(a, &child_id);
                    a = fold_bytes(a, &sep);
                    b = fold_bytes(b, &sep);
                    b = fold_bytes(b, &child_id);
                }
                Stamp { id: compose(a, b), nodes }
            }
        };
        self.memo.write().unwrap().insert(page, stamp);
        Ok(stamp.id)
    }
}

impl NodeIdentity for SubtreeHash {
    /// A page this provider has not stamped falls back to **page identity**, never to a constant.
    /// A constant sentinel would make every unstamped page compare equal to every other, which is
    /// the one failure mode that looks like success: the diff would skip everything and report no
    /// changes. The fallback's reserved tag byte keeps it from colliding with a real content id.
    fn id_of(&self, page: PageId) -> [u8; 16] {
        match self.memo.read().unwrap().get(&page) {
            Some(s) => s.id,
            None => page_id_identity(page),
        }
    }
}

/// Memoising adapter for an identity function that is **computed on demand and keeps no memo of
/// its own** — notably `cow::cid::subtree_cid`, whose own documentation says "Cost is the whole
/// subtree, every time — there is no memo table".
///
/// Handing such a function straight to [`NodeIdentity::id_of`] is a trap: every skip test would
/// cost a full subtree walk, so the diff would do strictly *more* work than the O(N) path it
/// replaces while still reporting skips. This wrapper is the one line that fixes it:
///
/// ```ignore
/// let ident = MemoIdentity::new(|p| ferrodb::cow::cid::subtree_cid(&tree, p));
/// ident.warm(&tree, base)?;
/// ident.warm(&tree, head)?;
/// let report = diff(&tree, base, head, &ident)?;
/// assert_eq!(ident.misses(), 0);
/// ```
///
/// **`warm` is not optional.** An unwarmed page falls back to page identity rather than computing
/// on the spot, because computing there is exactly the per-comparison blowup this type exists to
/// prevent. [`MemoIdentity::misses`] counts those fallbacks so an unwarmed provider is visible
/// instead of silently slow; the fallback itself is sound in this store, so the *answer* is right
/// either way.
///
/// Warming costs O(N · depth) here, because the wrapped function re-walks each subtree from
/// scratch. [`SubtreeHash`] folds bottom-up and warms in O(N) — prefer it unless the wrapped
/// digest is specifically what you need.
pub struct MemoIdentity<F> {
    compute: F,
    memo: RwLock<HashMap<PageId, [u8; 16]>>,
    misses: AtomicUsize,
}

impl<F> MemoIdentity<F>
where
    F: Fn(PageId) -> Result<[u8; 16], FerroError>,
{
    pub fn new(compute: F) -> Self {
        MemoIdentity { compute, memo: RwLock::new(HashMap::new()), misses: AtomicUsize::new(0) }
    }

    /// Compute and memoise an id for every page under `root`. Returns how many were added.
    pub fn warm(&self, tree: &CowTree, root: PageId) -> Result<usize, FerroError> {
        let mut added = 0usize;
        for p in tree.walk_pages(root)? {
            if self.memo.read().unwrap().contains_key(&p) {
                continue;
            }
            let id = tag_content((self.compute)(p)?);
            self.memo.write().unwrap().insert(p, id);
            added += 1;
        }
        Ok(added)
    }

    /// Pages that were compared without having been warmed, i.e. answered by the page-identity
    /// fallback. Non-zero after a diff means the skip was cruder than the wrapped digest allows.
    pub fn misses(&self) -> usize {
        self.misses.load(AtomicOrdering::Relaxed)
    }

    pub fn warmed(&self) -> usize {
        self.memo.read().unwrap().len()
    }
}

impl<F> NodeIdentity for MemoIdentity<F>
where
    F: Fn(PageId) -> Result<[u8; 16], FerroError>,
{
    fn id_of(&self, page: PageId) -> [u8; 16] {
        match self.memo.read().unwrap().get(&page) {
            Some(id) => *id,
            None => {
                self.misses.fetch_add(1, AtomicOrdering::Relaxed);
                page_id_identity(page)
            }
        }
    }
}

/// Stamp a wrapped digest into the content domain.
///
/// The wrapped function's codomain is the whole 16 bytes, so one of its outputs could in principle
/// equal a page-identity fallback value and produce a false skip. Overwriting byte 0 spends 8 of
/// the wrapped digest's bits to buy the same structural non-collision the rest of this module has,
/// which is the better side of that trade: the fallback is reachable on any unwarmed page, while
/// 120 bits is still far more than the birthday bound for any tree that fits on a disk.
fn tag_content(mut id: [u8; 16]) -> [u8; 16] {
    id[0] = TAG_CONTENT;
    id
}

/// Second FNV stream IV, so the two halves of the id are not the same function of the input.
const SECOND_IV: u64 = FNV_OFFSET ^ 0x9e37_79b9_7f4a_7c15;

/// Fold a variable-length piece in, length-prefixed. Without the prefix `("ab","c")` and
/// `("a","bc")` fold identically, so two subtrees that framed the same bytes differently would
/// agree — the same reasoning as `consensus::replicate::fold_bytes`, which is module-private.
fn fold_bytes(h: u64, bytes: &[u8]) -> u64 {
    let h = fnv64_update(h, &(bytes.len() as u64).to_be_bytes());
    fnv64_update(h, bytes)
}

fn fold_u64(h: u64, v: u64) -> u64 {
    fnv64_update(h, &v.to_be_bytes())
}

/// splitmix64's finalizer. FNV's avalanche is poor in the high bits; this decorrelates the two
/// streams before they are spliced together.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Splice two 64-bit streams into a tagged 16-byte id. Byte 0 is [`TAG_CONTENT`], so a content id
/// can never equal a page-identity fallback.
fn compose(a: u64, b: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0] = TAG_CONTENT;
    out[1..8].copy_from_slice(&mix64(a).to_be_bytes()[1..8]);
    out[8..16].copy_from_slice(&mix64(b).to_be_bytes());
    out
}

// -------------------------------------------------------------------------------------------
// Result
// -------------------------------------------------------------------------------------------

/// One key that differs between the two roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Added { key: Vec<u8>, value: Vec<u8> },
    Removed { key: Vec<u8>, value: Vec<u8> },
    Modified { key: Vec<u8>, before: Vec<u8>, after: Vec<u8> },
}

impl Change {
    pub fn key(&self) -> &[u8] {
        match self {
            Change::Added { key, .. } | Change::Removed { key, .. } | Change::Modified { key, .. } => key,
        }
    }
}

/// Live counters for one diff. Shared so a harness can read them while the diff runs.
///
/// `visited` counts nodes whose **payload was decoded**. `skipped_subtrees` counts skip *events*
/// — pairs found equal and abandoned without a read. Node counts for the skipped subtrees are
/// deliberately not computed here: counting them would cost exactly the walk the skip avoided.
/// [`skipped_node_count`] does it as an audit, outside the measurement.
#[derive(Debug, Default)]
pub struct DiffStats {
    visited: AtomicUsize,
    skipped_subtrees: AtomicUsize,
}

impl DiffStats {
    pub fn visited(&self) -> usize {
        self.visited.load(AtomicOrdering::Relaxed)
    }

    pub fn skipped_subtrees(&self) -> usize {
        self.skipped_subtrees.load(AtomicOrdering::Relaxed)
    }

    fn note_visit(&self) {
        self.visited.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn note_skip(&self) {
        self.skipped_subtrees.fetch_add(1, AtomicOrdering::Relaxed);
    }
}

/// What the diff found, and what it cost.
#[derive(Debug)]
pub struct DiffReport {
    pub changes: Vec<Change>,
    /// Nodes whose payload was decoded.
    pub visited: usize,
    /// Skip events: subtree pairs found equal by identity and never read.
    pub skipped_subtrees: usize,
    /// The `root_a`-side page at the top of each skipped subtree, in descent order. Bounded by
    /// `visited * fanout`, so recording it is cheap; [`skipped_node_count`] turns it into an exact
    /// node count when a test needs one.
    pub skipped_roots: Vec<PageId>,
}

/// Exact node count under the skipped subtrees. **O(skipped) — an audit, not part of the diff.**
///
/// Deduplicates by page id first: a misaligned split can make the descent meet the same child
/// under two different key sub-ranges, and counting it twice would overstate the skip.
pub fn skipped_node_count(tree: &CowTree, report: &DiffReport) -> Result<usize, FerroError> {
    let mut seen = std::collections::HashSet::new();
    let mut total = 0usize;
    for p in &report.skipped_roots {
        if seen.insert(*p) {
            total += tree.walk_pages(*p)?.len();
        }
    }
    Ok(total)
}

// -------------------------------------------------------------------------------------------
// Key spans
// -------------------------------------------------------------------------------------------

/// A half-open key range `[lo, hi)`. `lo == None` is -inf, `hi == None` is +inf.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Span {
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
}

impl Span {
    fn unbounded() -> Span {
        Span { lo: None, hi: None }
    }

    fn contains(&self, key: &[u8]) -> bool {
        if let Some(lo) = &self.lo {
            if key < lo.as_slice() {
                return false;
            }
        }
        if let Some(hi) = &self.hi {
            if key >= hi.as_slice() {
                return false;
            }
        }
        true
    }

    fn is_empty(&self) -> bool {
        match (&self.lo, &self.hi) {
            (Some(lo), Some(hi)) => lo >= hi,
            _ => false,
        }
    }

    /// Intersection. Both operands partition the same enclosing range, so this is the range the
    /// two children genuinely share.
    fn intersect(&self, other: &Span) -> Span {
        let lo = match (&self.lo, &other.lo) {
            (None, x) | (x, None) => x.clone(),
            (Some(a), Some(b)) => Some(if a >= b { a.clone() } else { b.clone() }),
        };
        let hi = match (&self.hi, &other.hi) {
            (None, x) | (x, None) => x.clone(),
            (Some(a), Some(b)) => Some(if a <= b { a.clone() } else { b.clone() }),
        };
        Span { lo, hi }
    }
}

/// Compare two upper bounds, `None` being +inf.
fn cmp_hi(a: &Option<Vec<u8>>, b: &Option<Vec<u8>>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(x), Some(y)) => x.cmp(y),
    }
}

// -------------------------------------------------------------------------------------------
// Node reading
// -------------------------------------------------------------------------------------------

enum Payload {
    Leaf(Vec<(Vec<u8>, Vec<u8>)>),
    /// Children with the key span each one covers, already clipped to the enclosing span and with
    /// empty spans dropped. The remaining spans partition the enclosing span exactly.
    Internal(Vec<(Span, PageId)>),
}

/// Read one node and decode it. The page handle is dropped before returning so a deep descent
/// does not pin one frame per level — the same discipline `btree::collect_unshared` uses.
fn read_payload(
    store: &Arc<dyn PageStore>,
    pid: PageId,
    enclosing: &Span,
) -> Result<Payload, FerroError> {
    let h = store.read_page(pid)?;
    let f = h.read();
    let ty = PageHeader::read_from(&f.data)?.page_type;
    let n = Node::new(&f.data);
    match ty {
        PageType::BTreeLeaf => Ok(Payload::Leaf(n.leaf_entries()?)),
        PageType::BTreeInternal => {
            let count = n.count();
            let mut out = Vec::with_capacity(count + 1);
            // Child spans follow `Node::child_slot_for` exactly: key k lands in the leftmost
            // child when k < key(0), otherwise in child(i) for the greatest i with key(i) <= k.
            // So leftmost covers (-inf, key(0)) and child(i) covers [key(i), key(i+1)).
            let first = if count == 0 { None } else { Some(n.key(0)?.to_vec()) };
            out.push((Span { lo: None, hi: first }, n.leftmost()));
            for i in 0..count {
                let lo = Some(n.key(i)?.to_vec());
                let hi = if i + 1 < count { Some(n.key(i + 1)?.to_vec()) } else { None };
                out.push((Span { lo, hi }, n.child(i)?));
            }
            let clipped: Vec<(Span, PageId)> = out
                .into_iter()
                .map(|(s, c)| (s.intersect(enclosing), c))
                .filter(|(s, _)| !s.is_empty())
                .collect();
            Ok(Payload::Internal(clipped))
        }
        other => Err(FerroError::Cow(format!(
            "page {} is a {:?}, not a btree node",
            pid, other
        ))),
    }
}

// -------------------------------------------------------------------------------------------
// The diff
// -------------------------------------------------------------------------------------------

/// Diff two roots of the same tree by **synchronised descent**.
///
/// At every level the two sides' children are merge-joined on their key spans; a pair whose
/// [`NodeIdentity`] ids are equal is skipped in O(1) without either page being read. Neither side
/// is ever enumerated on its own, so for a small change set the work is O(delta · log_m N) rather
/// than O(N).
///
/// `changes` come back in key order.
pub fn diff(
    tree: &CowTree,
    root_a: PageId,
    root_b: PageId,
    identity: &dyn NodeIdentity,
) -> Result<DiffReport, FerroError> {
    let stats = DiffStats::default();
    diff_with_stats(tree, root_a, root_b, identity, &stats)
}

/// [`diff`], with the counters supplied by the caller so they can be read while it runs.
pub fn diff_with_stats(
    tree: &CowTree,
    root_a: PageId,
    root_b: PageId,
    identity: &dyn NodeIdentity,
    stats: &DiffStats,
) -> Result<DiffReport, FerroError> {
    let mut d = Differ {
        store: tree.store().clone(),
        identity,
        stats,
        changes: Vec::new(),
        skipped_roots: Vec::new(),
    };
    d.walk(root_a, root_b, &Span::unbounded(), 0)?;
    d.changes.sort_by(|x, y| x.key().cmp(y.key()));
    Ok(DiffReport {
        changes: d.changes,
        visited: stats.visited(),
        skipped_subtrees: stats.skipped_subtrees(),
        skipped_roots: d.skipped_roots,
    })
}

struct Differ<'a> {
    store: Arc<dyn PageStore>,
    identity: &'a dyn NodeIdentity,
    stats: &'a DiffStats,
    changes: Vec<Change>,
    skipped_roots: Vec<PageId>,
}

impl Differ<'_> {
    fn walk(&mut self, a: PageId, b: PageId, span: &Span, depth: usize) -> Result<(), FerroError> {
        if depth > MAX_DESCENT {
            return Err(FerroError::Cow("diff descent exceeded the depth guard".into()));
        }
        // The skip. Equal identity => equal subtrees => equal on every sub-range of them, so this
        // holds whether `span` is the children's full span or a clipped piece of it.
        if self.identity.id_of(a) == self.identity.id_of(b) {
            self.stats.note_skip();
            self.skipped_roots.push(a);
            return Ok(());
        }

        let pa = read_payload(&self.store, a, span)?;
        self.stats.note_visit();
        let pb = read_payload(&self.store, b, span)?;
        self.stats.note_visit();

        match (pa, pb) {
            (Payload::Leaf(ea), Payload::Leaf(eb)) => {
                self.join_entries(&ea, &eb, span);
                Ok(())
            }
            (Payload::Internal(ca), Payload::Internal(cb)) => {
                // Both child lists partition `span` exactly, so a two-pointer merge over their
                // upper bounds visits every sub-range once. When the two trees have the same
                // shape — the common case for a handful of updates — the spans line up and this
                // degenerates to the pairwise comparison the mechanism is described as.
                let (mut i, mut j) = (0usize, 0usize);
                while i < ca.len() && j < cb.len() {
                    let inter = ca[i].0.intersect(&cb[j].0);
                    if !inter.is_empty() {
                        self.walk(ca[i].1, cb[j].1, &inter, depth + 1)?;
                    }
                    match cmp_hi(&ca[i].0.hi, &cb[j].0.hi) {
                        Ordering::Equal => {
                            i += 1;
                            j += 1;
                        }
                        Ordering::Less => i += 1,
                        Ordering::Greater => j += 1,
                    }
                }
                Ok(())
            }
            // Height mismatch: one side split its root and the other did not. There is no pairing
            // to exploit, so both sides are materialised over `span` and merge-joined. This is the
            // honest cost of a structural change and it is confined to the span where it happened.
            (pa, pb) => {
                let ea = self.materialise(pa, depth)?;
                let eb = self.materialise(pb, depth)?;
                self.join_entries(&ea, &eb, span);
                Ok(())
            }
        }
    }

    /// Flatten one already-decoded node's subtree to its entries.
    ///
    /// No span filtering here: the children were clipped when the node was read, and a leaf's
    /// entries are filtered by [`Differ::join_entries`] against the same span. Filtering in both
    /// places would be a second guard over the first, and the one in front would never be tested.
    fn materialise(
        &mut self,
        payload: Payload,
        depth: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FerroError> {
        match payload {
            Payload::Leaf(e) => Ok(e),
            Payload::Internal(children) => {
                let mut out = Vec::new();
                for (child_span, child) in children {
                    self.collect(child, &child_span, depth + 1, &mut out)?;
                }
                Ok(out)
            }
        }
    }

    fn collect(
        &mut self,
        pid: PageId,
        span: &Span,
        depth: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), FerroError> {
        if depth > MAX_DESCENT {
            return Err(FerroError::Cow("diff collect exceeded the depth guard".into()));
        }
        let payload = read_payload(&self.store, pid, span)?;
        self.stats.note_visit();
        match payload {
            Payload::Leaf(e) => {
                out.extend(e);
                Ok(())
            }
            Payload::Internal(children) => {
                for (child_span, child) in children {
                    self.collect(child, &child_span, depth + 1, out)?;
                }
                Ok(())
            }
        }
    }

    /// Merge-join two sorted entry lists, emitting only what differs inside `span`.
    ///
    /// The span filter is what keeps an unchanged neighbour out of the changeset when a clipped
    /// descent hands back a leaf that straddles the range boundary.
    fn join_entries(
        &mut self,
        ea: &[(Vec<u8>, Vec<u8>)],
        eb: &[(Vec<u8>, Vec<u8>)],
        span: &Span,
    ) {
        let (mut i, mut j) = (0usize, 0usize);
        while i < ea.len() || j < eb.len() {
            let ord = match (ea.get(i), eb.get(j)) {
                (Some((ka, _)), Some((kb, _))) => ka.cmp(kb),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => break,
            };
            match ord {
                Ordering::Less => {
                    let (k, v) = &ea[i];
                    if span.contains(k) {
                        self.changes.push(Change::Removed { key: k.clone(), value: v.clone() });
                    }
                    i += 1;
                }
                Ordering::Greater => {
                    let (k, v) = &eb[j];
                    if span.contains(k) {
                        self.changes.push(Change::Added { key: k.clone(), value: v.clone() });
                    }
                    j += 1;
                }
                Ordering::Equal => {
                    let (k, va) = &ea[i];
                    let (_, vb) = &eb[j];
                    if va != vb && span.contains(k) {
                        self.changes.push(Change::Modified {
                            key: k.clone(),
                            before: va.clone(),
                            after: vb.clone(),
                        });
                    }
                    i += 1;
                    j += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::sync::atomic::{AtomicU64, Ordering as AtomOrd};

    use tempfile::TempDir;

    use super::*;
    use crate::branch::types::{BranchId, Epoch};
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::cow::store::CowStore;
    use crate::storage::disk_manager::DiskManager;

    struct Fixture {
        _dir: TempDir,
        store: Arc<CowStore>,
        tree: CowTree,
        clock: AtomicU64,
    }

    impl Fixture {
        fn new() -> Fixture {
            let dir = TempDir::new().unwrap();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(dir.path().join("cow.db"))
                .unwrap();
            let dm = Arc::new(DiskManager::new(file).unwrap());
            let pool = Arc::new(BufferPoolManager::new(dm));
            let store = Arc::new(CowStore::with_extent_pages(pool, 256));
            let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);
            Fixture { _dir: dir, store, tree, clock: AtomicU64::new(1) }
        }

        fn tick(&self) -> Epoch {
            Epoch(self.clock.fetch_add(1, AtomOrd::SeqCst))
        }

        fn put(&self, root: PageId, b: BranchId, k: &str, v: &str) -> PageId {
            let e = self.tick();
            self.tree.insert(root, b, e, k.as_bytes(), v.as_bytes()).unwrap()
        }

        /// A trunk tree of `n` fixed-width rows. Fixed width so an update can never split a node,
        /// which is what keeps the two versions the same shape.
        fn build(&self, n: usize) -> PageId {
            let e = self.tick();
            let mut root = self.tree.create(BranchId::TRUNK, e).unwrap();
            for i in 0..n {
                root = self.put(root, BranchId::TRUNK, &key(i), &value(i, 0));
            }
            root
        }

        fn fork(&self, child: BranchId, parent_root: PageId) -> PageId {
            let e = self.tick();
            self.store.register_branch(child, Some(BranchId::TRUNK), e).unwrap();
            parent_root
        }

        fn store_dyn(&self) -> Arc<dyn PageStore> {
            self.store.clone() as Arc<dyn PageStore>
        }
    }

    fn key(i: usize) -> String {
        format!("k{:08}", i)
    }

    fn value(i: usize, ver: u32) -> String {
        format!("v{:08}-{:03}", i, ver)
    }

    const B1: BranchId = BranchId::new(1, 0);

    /// Every provider must agree on the changes; only the cost may differ.
    fn both_providers(f: &Fixture, a: PageId, b: PageId) -> (DiffReport, DiffReport) {
        let by_page = diff(&f.tree, a, b, &PageIdentity).unwrap();
        let hash = SubtreeHash::new(f.store_dyn());
        hash.stamp(a).unwrap();
        hash.stamp(b).unwrap();
        let by_hash = diff(&f.tree, a, b, &hash).unwrap();
        assert_eq!(by_page.changes, by_hash.changes, "providers disagree on the changeset");
        (by_page, by_hash)
    }

    // -- forcing the detector to fire, both ways ----------------------------------------------

    #[test]
    fn identical_trees_skip_everything_and_read_nothing() {
        let f = Fixture::new();
        let root = f.build(4000);
        let total = f.tree.walk_pages(root).unwrap().len();
        assert!(total > 20, "test needs a multi-level tree, got {} pages", total);

        let (by_page, by_hash) = both_providers(&f, root, root);
        for (label, r) in [("page-id", &by_page), ("subtree-hash", &by_hash)] {
            assert!(r.changes.is_empty(), "{label} invented changes: {:?}", r.changes);
            assert_eq!(r.visited, 0, "{label} read a page for two identical roots");
            assert_eq!(r.skipped_subtrees, 1, "{label} should skip at the root, once");
            assert_eq!(
                skipped_node_count(&f.tree, r).unwrap(),
                total,
                "{label} skipped fewer than all {} nodes",
                total
            );
        }
    }

    /// The other direction. A skip counter that never fires is not a clean result, and one that
    /// always fires is worse — it would report every diff as empty.
    #[test]
    fn rewriting_every_leaf_skips_nothing() {
        let f = Fixture::new();
        let n = 2000;
        let base = f.build(n);
        let mut head = f.fork(B1, base);
        for i in 0..n {
            head = f.put(head, B1, &key(i), &value(i, 1));
        }

        let (by_page, by_hash) = both_providers(&f, base, head);
        for (label, r) in [("page-id", &by_page), ("subtree-hash", &by_hash)] {
            assert_eq!(r.changes.len(), n, "{label} lost changes");
            assert_eq!(r.skipped_subtrees, 0, "{label} skipped a subtree where every leaf differs");
            assert!(
                r.visited >= f.tree.walk_pages(base).unwrap().len(),
                "{label} visited {} nodes but the base tree alone has {}",
                r.visited,
                f.tree.walk_pages(base).unwrap().len()
            );
            assert!(
                matches!(&r.changes[0], Change::Modified { .. }),
                "{label} classified an overwrite as something other than Modified"
            );
        }
    }

    // -- correctness ---------------------------------------------------------------------------

    #[test]
    fn four_changed_rows_are_found_exactly() {
        let f = Fixture::new();
        let n = 4000;
        let base = f.build(n);
        let mut head = f.fork(B1, base);
        let touched = [7usize, 1013, 2500, 3999];
        for i in touched {
            head = f.put(head, B1, &key(i), &value(i, 9));
        }

        let (by_page, _) = both_providers(&f, base, head);
        assert_eq!(by_page.changes.len(), 4, "got {:?}", by_page.changes);
        for (c, i) in by_page.changes.iter().zip(touched) {
            match c {
                Change::Modified { key: k, before, after } => {
                    assert_eq!(k, key(i).as_bytes());
                    assert_eq!(before, value(i, 0).as_bytes());
                    assert_eq!(after, value(i, 9).as_bytes());
                }
                other => panic!("expected Modified, got {:?}", other),
            }
        }
        assert!(by_page.skipped_subtrees > 0, "no subtree was skipped for a 4-row change");
    }

    #[test]
    fn inserts_and_deletes_are_classified_and_the_answer_matches_a_full_scan() {
        let f = Fixture::new();
        let n = 1200;
        let base = f.build(n);
        let mut head = f.fork(B1, base);
        head = f.put(head, B1, "k00000500", "changed!!!!!");
        head = f.put(head, B1, "zzz-new-key", "brand new");
        let e = f.tick();
        head = f.tree.delete(head, B1, e, key(900).as_bytes()).unwrap();

        let (r, _) = both_providers(&f, base, head);

        // Expected value taken from a full ordered scan of each side, never from the subject.
        let scan = |root: PageId| -> Vec<(Vec<u8>, Vec<u8>)> {
            f.tree.range_scan(root, None, None).unwrap().map(|e| e.unwrap()).collect()
        };
        let (sa, sb) = (scan(base), scan(head));
        let mut expected = Vec::new();
        let mut ia = sa.iter().peekable();
        let mut ib = sb.iter().peekable();
        loop {
            match (ia.peek(), ib.peek()) {
                (None, None) => break,
                (Some((ka, va)), Some((kb, vb))) => match ka.cmp(kb) {
                    Ordering::Equal => {
                        if va != vb {
                            expected.push(Change::Modified {
                                key: ka.clone(),
                                before: va.clone(),
                                after: vb.clone(),
                            });
                        }
                        ia.next();
                        ib.next();
                    }
                    Ordering::Less => {
                        expected.push(Change::Removed { key: ka.clone(), value: va.clone() });
                        ia.next();
                    }
                    Ordering::Greater => {
                        expected.push(Change::Added { key: kb.clone(), value: vb.clone() });
                        ib.next();
                    }
                },
                (Some((ka, va)), None) => {
                    expected.push(Change::Removed { key: ka.clone(), value: va.clone() });
                    ia.next();
                }
                (None, Some((kb, vb))) => {
                    expected.push(Change::Added { key: kb.clone(), value: vb.clone() });
                    ib.next();
                }
            }
        }
        assert_eq!(r.changes, expected);
        assert_eq!(r.changes.len(), 3, "got {:?}", r.changes);
    }

    #[test]
    fn a_growing_insert_run_that_splits_nodes_still_diffs_correctly() {
        let f = Fixture::new();
        let base = f.build(600);
        let mut head = f.fork(B1, base);
        // Interleaved new keys, enough of them to force splits and change the tree's shape.
        for i in 0..300 {
            head = f.put(head, B1, &format!("k{:08}-mid", i * 2), "inserted");
        }
        let (r, _) = both_providers(&f, base, head);
        assert_eq!(r.changes.len(), 300, "got {} changes", r.changes.len());
        assert!(r.changes.iter().all(|c| matches!(c, Change::Added { .. })));
    }

    #[test]
    fn the_diff_is_antisymmetric() {
        let f = Fixture::new();
        let base = f.build(800);
        let mut head = f.fork(B1, base);
        head = f.put(head, B1, &key(42), "different");
        head = f.put(head, B1, "k99999999", "appended");

        let fwd = diff(&f.tree, base, head, &PageIdentity).unwrap();
        let rev = diff(&f.tree, head, base, &PageIdentity).unwrap();
        assert_eq!(fwd.changes.len(), rev.changes.len());
        for (a, b) in fwd.changes.iter().zip(&rev.changes) {
            match (a, b) {
                (
                    Change::Modified { key: k1, before: x1, after: y1 },
                    Change::Modified { key: k2, before: x2, after: y2 },
                ) => {
                    assert_eq!(k1, k2);
                    assert_eq!((x1, y1), (y2, x2));
                }
                (Change::Added { key: k1, value: v1 }, Change::Removed { key: k2, value: v2 }) => {
                    assert_eq!((k1, v1), (k2, v2));
                }
                other => panic!("not a mirrored pair: {:?}", other),
            }
        }
    }

    #[test]
    fn an_empty_tree_against_a_populated_one_reports_every_row() {
        let f = Fixture::new();
        let e = f.tick();
        let empty = f.tree.create(BranchId::TRUNK, e).unwrap();
        let full = f.build(300);
        let r = diff(&f.tree, empty, full, &PageIdentity).unwrap();
        assert_eq!(r.changes.len(), 300);
        assert!(r.changes.iter().all(|c| matches!(c, Change::Added { .. })));
    }

    // -- identity ------------------------------------------------------------------------------

    #[test]
    fn an_unstamped_page_falls_back_to_page_identity_and_cannot_collide_with_a_content_id() {
        let f = Fixture::new();
        let root = f.build(300);
        let h = SubtreeHash::new(f.store_dyn());

        // Nothing stamped yet: every id is a distinct page-identity value.
        assert_eq!(h.stamped_nodes(), 0);
        let pages = f.tree.walk_pages(root).unwrap();
        assert!(pages.len() >= 2);
        assert_eq!(h.id_of(pages[0])[0], TAG_PAGE);
        assert_ne!(
            h.id_of(pages[0]),
            h.id_of(pages[1]),
            "two distinct unstamped pages collided — every diff would skip everything"
        );

        h.stamp(root).unwrap();
        assert_eq!(h.stamped_nodes(), pages.len(), "stamp did not cover the tree");
        assert_eq!(h.id_of(root)[0], TAG_CONTENT);
        assert_eq!(h.nodes_under(root), Some(pages.len()));
    }

    /// The property a content hash has and page identity does not: two physically distinct
    /// subtrees holding the same rows are equal. Overwriting a key with the value it already had
    /// still shadows the leaf, so the two roots differ by page id while the contents match.
    #[test]
    fn a_content_hash_skips_a_rewritten_but_unchanged_leaf_where_page_identity_cannot() {
        let f = Fixture::new();
        let base = f.build(1500);
        let mut head = f.fork(B1, base);
        head = f.put(head, B1, &key(700), &value(700, 0)); // same value, new page
        assert_ne!(head, base, "the overwrite did not shadow anything");

        let by_page = diff(&f.tree, base, head, &PageIdentity).unwrap();
        let h = SubtreeHash::new(f.store_dyn());
        h.stamp(base).unwrap();
        h.stamp(head).unwrap();
        let by_hash = diff(&f.tree, base, head, &h).unwrap();

        assert!(by_page.changes.is_empty(), "a no-op overwrite is not a change");
        assert!(by_hash.changes.is_empty());
        assert!(by_page.visited > 0, "page identity had to read the shadowed path");
        assert_eq!(
            by_hash.visited, 0,
            "the content hash should have matched at the root and read nothing"
        );
    }

    /// The adapter an on-demand digest such as `cow::cid::subtree_cid` has to go through. The
    /// stand-in below is folded the same way; what is under test is the wrapper's contract, not
    /// the digest.
    #[test]
    fn a_warmed_memo_identity_skips_and_never_falls_back() {
        let f = Fixture::new();
        let base = f.build(2000);
        let mut head = f.fork(B1, base);
        head = f.put(head, B1, &key(11), "changed");

        let inner = SubtreeHash::new(f.store_dyn());
        let m = MemoIdentity::new(|p| inner.stamp(p));
        let warmed = m.warm(&f.tree, base).unwrap() + m.warm(&f.tree, head).unwrap();
        assert_eq!(warmed, m.warmed());

        let r = diff(&f.tree, base, head, &m).unwrap();
        assert_eq!(r.changes.len(), 1);
        assert_eq!(m.misses(), 0, "a fully warmed memo still fell back to page identity");
        assert!(r.skipped_subtrees > 0);
        assert_eq!(m.id_of(base)[0], TAG_CONTENT, "a warmed id must sit in the content domain");
    }

    /// The unwarmed half. The fallback must keep the ANSWER right — it is only the skip that gets
    /// cruder — and it must be visible in `misses()` rather than silent.
    #[test]
    fn an_unwarmed_memo_identity_still_answers_correctly_and_says_so() {
        let f = Fixture::new();
        let base = f.build(2000);
        let mut head = f.fork(B1, base);
        head = f.put(head, B1, &key(11), "changed");

        let inner = SubtreeHash::new(f.store_dyn());
        let m = MemoIdentity::new(|p| inner.stamp(p));
        assert_eq!(m.warmed(), 0);

        let cold = diff(&f.tree, base, head, &m).unwrap();
        let warm = diff(&f.tree, base, head, &PageIdentity).unwrap();
        assert_eq!(cold.changes, warm.changes, "the fallback changed the answer");
        assert!(m.misses() > 0, "an unwarmed memo reported no misses — the counter is dead");
    }

    #[test]
    fn the_hash_folds_length_prefixed_so_a_reframing_cannot_agree() {
        // ("ab","c") and ("a","bc") must not fold to the same digest.
        let one = fold_bytes(fold_bytes(FNV_OFFSET, b"ab"), b"c");
        let two = fold_bytes(fold_bytes(FNV_OFFSET, b"a"), b"bc");
        assert_ne!(one, two);
    }

    /// Build an internal node whose only child is itself. Not reachable through the write path;
    /// it exists so the depth guards can be made to fire on purpose rather than asserted about.
    fn self_referential_internal(f: &Fixture) -> PageId {
        let e = f.tick();
        let pid = f
            .store
            .alloc_for(BranchId::TRUNK, crate::cow::page_header::PageType::BTreeInternal, e)
            .unwrap();
        let h = f.store.read_page(pid).unwrap();
        let mut fr = h.write();
        let mut n = crate::cow::node::NodeMut::new(&mut fr.data);
        n.init();
        n.set_leftmost(pid);
        crate::cow::page_header::stamp_checksum(&mut fr.data);
        drop(fr);
        pid
    }

    /// Both descent guards, fired. A guard that has never been made to trip is not a guard —
    /// nothing distinguishes it from a condition that cannot happen.
    #[test]
    fn a_cyclic_page_graph_fails_instead_of_looping() {
        let f = Fixture::new();

        // Internal vs internal takes the paired-descent path, so `walk`'s guard is the one that
        // has to stop it.
        let (a, b) = (self_referential_internal(&f), self_referential_internal(&f));
        let err = diff(&f.tree, a, b, &PageIdentity).unwrap_err();
        assert!(
            format!("{err:?}").contains("diff descent exceeded the depth guard"),
            "walk guard did not fire, got {err:?}"
        );

        // Internal vs leaf takes the materialise path, whose recursion has its own guard.
        let leaf = f.build(4);
        let err = diff(&f.tree, a, leaf, &PageIdentity).unwrap_err();
        assert!(
            format!("{err:?}").contains("diff collect exceeded the depth guard"),
            "collect guard did not fire, got {err:?}"
        );

        // And the identity provider's own walk is guarded the same way.
        let h = SubtreeHash::new(f.store_dyn());
        let err = h.stamp(a).unwrap_err();
        assert!(
            format!("{err:?}").contains("subtree hash exceeded the depth guard"),
            "stamp guard did not fire, got {err:?}"
        );
    }

    /// The counters must not be quietly reusable across runs: a fresh [`DiffStats`] starts at
    /// zero, and a shared one accumulates. A harness that reused one without knowing would read
    /// the previous run's cost as this one's.
    #[test]
    fn shared_stats_accumulate_across_diffs() {
        let f = Fixture::new();
        let base = f.build(1500);
        let mut head = f.fork(B1, base);
        head = f.put(head, B1, &key(3), "changed");

        let stats = DiffStats::default();
        assert_eq!((stats.visited(), stats.skipped_subtrees()), (0, 0));
        let first = diff_with_stats(&f.tree, base, head, &PageIdentity, &stats).unwrap();
        assert!(first.visited > 0);
        let second = diff_with_stats(&f.tree, base, head, &PageIdentity, &stats).unwrap();
        assert_eq!(second.visited, first.visited * 2, "the counter is not shared");
        assert_eq!(second.changes.len(), 1);
    }
}
