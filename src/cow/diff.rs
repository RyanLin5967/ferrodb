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
//! Swapping in `cow::cid`'s digest is one line: implement [`NodeIdentity`] for it. Doing it by
//! wrapping `cid::subtree_cid` in [`MemoIdentity`] and warming a whole tree is **not** that line:
//! measured at 5410 page reads against the 2188 of the O(N) path this module exists to beat, it is
//! that path wearing a skip counter. [`MemoIdentity`]'s own docs carry the numbers and the rule.
//!
//! # A memo over page ids is a cache over recycled keys
//!
//! Both identity providers below memoise, and both are caches keyed on a [`PageId`] — an id both
//! stores hand back out after it is freed. `CowStore::format_page` and
//! `ArenaPageStore::write_fresh_page` each say so at the point of reuse, and each drops its own
//! stale cached image there. A memo here that did not would not go slow, it would go **wrong**: a
//! hit answers for the page's previous life, the diff finds the two sides equal, and a real change
//! is absent from the changeset with no error raised and no counter moved. Every memo row in this
//! file therefore carries the [`PageVersion`] it was taken from and is re-validated before it is
//! trusted — see [`PageVersion`] for what that catches and, just as importantly, what it cannot.
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

use crate::branch::types::{Epoch, PageId};
use crate::consensus::replicate::{fnv64_update, FNV_OFFSET};
use crate::cow::btree::CowTree;
use crate::cow::node::Node;
use crate::cow::page_header::{PageHeader, PageType};
use crate::cow::{PageHandle, PageStore};
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

// -------------------------------------------------------------------------------------------
// Memo keys: page ids are recycled, so a page id alone does not name a page's contents
// -------------------------------------------------------------------------------------------

/// What a memo row is keyed on, on top of the [`PageId`]: the discriminator that separates one
/// page id's *life* from the next.
///
/// **A `PageId` on its own is not a cache key in this store.** Both stores hand a freed id back
/// out — `store.rs`'s allocator pops `free_pages`, `arena.rs`'s pops `recycled` — and both say so
/// where they do it: `CowStore::format_page` warns that "a recycled page may still be cached from
/// its previous life", and `ArenaPageStore::write_fresh_page` drops "any stale cached image of a
/// recycled id before the fresh write" by calling `evict`. Every other cache in `cow` already
/// obeys this; the memos in this file are two more caches on the same key and must obey it too.
/// Keyed on the id alone they do not go *slow* when a page is recycled, they go **wrong**: a hit
/// answers for content that is no longer on the page, and the diff skips a subtree that differs.
///
/// The two fields are not independent, and the asymmetry matters:
///
/// * `checksum` is the crc32 `stamp_checksum` writes over the **whole page, header included**, so
///   it moves on any change to the page's bytes — a fresh allocation (which restamps the header)
///   and an in-place rewrite alike. The in-place case is not a corner: `btree.rs` mutates a node
///   where it lies whenever it is already private to the writer, and that path leaves `birth`
///   exactly where it was. So `checksum` is the discriminator that carries the guard.
/// * `birth` is therefore **not** catching a class `checksum` misses — `birth` lives inside the
///   bytes `checksum` covers, and an attempt to build a recycled page that agrees on `checksum`
///   but not on `birth` is unconstructable for that reason. What it buys is *exactness where
///   crc32 is probabilistic*: a recycled id always starts a new epoch, so comparing `birth` makes
///   the recycle case an exact decision instead of one a 32-bit collision could get wrong. It
///   rides along on the same header read and costs nothing, which is the whole argument for it.
///
/// Keying on `birth` **alone** — the obvious reading, since `birth` is the field the stores
/// restamp — is the version of this guard that does not work: it cannot see an in-place rewrite
/// at all. `an_in_place_rewrite_invalidates_the_row_although_the_birth_epoch_is_unchanged` is the
/// test that fails when the `checksum` half is removed.
///
/// # What this cannot see, stated where the guard is
///
/// Neither field can see a change *below* the page it describes. A memo row for an internal node
/// commits to that node's whole **subtree**, but the copy-up walk in `btree::insert` stops at the
/// first node already private to the writer — so a leaf can be rewritten in place while every
/// ancestor keeps its id, its `birth` and its bytes, and therefore its version. No page-local
/// token can catch that; validating it would mean re-reading the subtree, which is the walk the
/// memo exists to avoid. The rule that covers it is a usage rule, not a check: **stamp after the
/// last write to either side, and do not let a provider outlive a write to a branch whose pages
/// it has stamped.** [`SubtreeHash`] and [`MemoIdentity`] repeat it where a caller will read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageVersion {
    birth: Epoch,
    checksum: u32,
}

impl PageVersion {
    fn of(header: &PageHeader) -> PageVersion {
        PageVersion { birth: header.birth_epoch, checksum: header.checksum }
    }

    /// Read a page's current version. One page fetch and a header parse — no payload decode and
    /// no descent, which is what keeps a validated memo hit cheaper than the read it replaces.
    fn read(handle: &PageHandle) -> Result<PageVersion, FerroError> {
        Ok(PageVersion::of(&handle.header()?))
    }
}

/// One memo row: the page version it was taken from, the subtree's content id, and how many nodes
/// are under it. The version is what makes a stale row **miss** instead of lying.
#[derive(Debug, Clone, Copy)]
struct Stamp {
    version: PageVersion,
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
///
/// # The memo is keyed on the page's version, not on its id
///
/// Page ids are recycled by both stores, so a row keyed on [`PageId`] alone answers for whatever
/// used to be on that page — see [`PageVersion`] for the two stores' own words on it. Every row
/// here carries the version it was taken from and is re-read before it is trusted, so a stale row
/// **misses** (and is recomputed, or falls back to page identity) instead of reporting a subtree
/// that is no longer there. The cost of that is one page fetch and a header parse per query, with
/// no payload decode and no descent.
///
/// The one thing the version cannot see is a change *below* a stamped node: `btree::insert`
/// mutates a node in place once it is private to the writer and stops copying up there, leaving
/// every ancestor byte-identical. So **stamp after the last write to either root**, and do not
/// reuse a provider across a write to a branch it has stamped. [`PageVersion`] says why.
pub struct SubtreeHash {
    store: Arc<dyn PageStore>,
    memo: RwLock<HashMap<PageId, Stamp>>,
}

impl SubtreeHash {
    pub fn new(store: Arc<dyn PageStore>) -> Self {
        SubtreeHash { store, memo: RwLock::new(HashMap::new()) }
    }

    /// The memo row for `page`, **only if it still describes the page that is there now**.
    ///
    /// Takes the version from the caller rather than reading it, so a caller that already has the
    /// page open pays for one fetch, not two. Never holds the page latch and the memo lock at the
    /// same time: `handle.header()` releases the latch before this is called.
    fn fresh_row(&self, page: PageId, version: PageVersion) -> Option<Stamp> {
        let row = *self.memo.read().unwrap().get(&page)?;
        (row.version == version).then_some(row)
    }

    /// [`SubtreeHash::fresh_row`] for a caller that does not already hold the page. `None` also
    /// covers a page that cannot be read at all, which is the sound direction: an unusable page
    /// falls back to page identity rather than to a stale answer.
    fn fresh_row_of(&self, page: PageId) -> Option<Stamp> {
        let handle = self.store.read_page(page).ok()?;
        let version = PageVersion::read(&handle).ok()?;
        self.fresh_row(page, version)
    }

    /// Fold the subtree at `root`, memoising every node on the way. Returns the root's id.
    pub fn stamp(&self, root: PageId) -> Result<[u8; 16], FerroError> {
        Ok(self.stamp_inner(root, 0)?.id)
    }

    /// Nodes under `page`, if it has been stamped **and the stamp still describes it**. `None`
    /// means "not stamped, or stamped in a previous life of this page id", never "zero".
    pub fn nodes_under(&self, page: PageId) -> Option<usize> {
        self.fresh_row_of(page).map(|s| s.nodes)
    }

    /// How many rows the memo holds. Diagnostic; a harness uses it to prove the precompute
    /// actually ran rather than silently no-oped.
    ///
    /// Rows are **not** re-validated to answer this — that would cost one page fetch per row — so
    /// this is "rows taken", not "rows still current". A row whose page has been recycled since is
    /// still counted here and still refuses to answer [`NodeIdentity::id_of`].
    pub fn stamped_nodes(&self) -> usize {
        self.memo.read().unwrap().len()
    }

    /// The fold. Returns the whole row so a parent can take its child's `nodes` count without a
    /// second lookup — and, after the memo became version-keyed, without a second page fetch.
    fn stamp_inner(&self, page: PageId, depth: usize) -> Result<Stamp, FerroError> {
        if depth > MAX_DESCENT {
            return Err(FerroError::Cow("subtree hash exceeded the depth guard".into()));
        }
        // One fetch serves both the version check and the payload, and the handle is dropped
        // before the recursion so a deep fold does not pin one frame per level. What a memo hit
        // saves is the decode and the descent below it, which is the part that costs.
        let (version, payload) = {
            let handle = self.store.read_page(page)?;
            let version = PageVersion::read(&handle)?;
            if let Some(row) = self.fresh_row(page, version) {
                return Ok(row);
            }
            (version, decode_payload(&handle, page, &Span::unbounded())?)
        };
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
                Stamp { version, id: compose(a, b), nodes: 1 }
            }
            Payload::Internal(children) => {
                let mut a = fnv64_update(FNV_OFFSET, b"ferrodb/d91/internal");
                let mut b = fnv64_update(SECOND_IV, b"ferrodb/d91/internal");
                a = fold_u64(a, children.len() as u64);
                b = fold_u64(b, children.len() as u64);
                let mut nodes = 1usize;
                for (span, child) in &children {
                    let child = self.stamp_inner(*child, depth + 1)?;
                    nodes += child.nodes;
                    let sep = span.lo.clone().unwrap_or_default();
                    a = fold_bytes(a, &child.id);
                    a = fold_bytes(a, &sep);
                    b = fold_bytes(b, &sep);
                    b = fold_bytes(b, &child.id);
                }
                Stamp { version, id: compose(a, b), nodes }
            }
        };
        self.memo.write().unwrap().insert(page, stamp);
        Ok(stamp)
    }
}

impl NodeIdentity for SubtreeHash {
    /// A page this provider has not stamped — **or has stamped in a previous life of that page
    /// id** — falls back to **page identity**, never to a constant. A constant sentinel would make
    /// every unstamped page compare equal to every other, which is the one failure mode that looks
    /// like success: the diff would skip everything and report no changes. The fallback's reserved
    /// tag byte keeps it from colliding with a real content id.
    ///
    /// The stale case used to be indistinguishable from a hit, and answered with the *old* page's
    /// id — a false skip, i.e. a change silently missing from the diff with no error and no
    /// counter moving. Re-reading the page's [`PageVersion`] turns it into a fallback, which is
    /// merely cruder.
    fn id_of(&self, page: PageId) -> [u8; 16] {
        match self.fresh_row_of(page) {
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
/// ```
/// # use std::sync::Arc;
/// # use ferrodb::branch::types::{BranchId, Epoch, PageId};
/// # use ferrodb::buffer::buffer_pool::BufferPoolManager;
/// # use ferrodb::cow::btree::CowTree;
/// # use ferrodb::cow::store::CowStore;
/// # use ferrodb::cow::PageStore;
/// # use ferrodb::storage::disk_manager::DiskManager;
/// use ferrodb::cow::cid;
/// use ferrodb::cow::diff::{diff, MemoIdentity};
/// # let dir = tempfile::tempdir().unwrap();
/// # let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true)
/// #     .open(dir.path().join("cow.db")).unwrap();
/// # let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
/// # let store = Arc::new(CowStore::with_extent_pages(pool, 256));
/// # let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);
/// # let agent = BranchId::new(1, 0);
/// # let mut base = tree.create(BranchId::TRUNK, Epoch(1))?;
/// # for i in 0..64u32 {
/// #     base = tree.insert(base, BranchId::TRUNK, Epoch(2), &i.to_be_bytes(), b"v0")?;
/// # }
/// # store.register_branch(agent, Some(BranchId::TRUNK), Epoch(3))?;
/// # let head = tree.insert(base, agent, Epoch(4), &7u32.to_be_bytes(), b"v1")?;
/// let ident = MemoIdentity::new(&tree, |t: &CowTree, p: PageId| cid::subtree_cid(t, p));
/// ident.warm(base)?;
/// ident.warm(head)?;
/// let report = diff(&tree, base, head, &ident)?;
/// assert_eq!(report.changes.len(), 1);
/// assert_eq!(ident.misses(), 0);
/// # Ok::<(), ferrodb::error::FerroError>(())
/// ```
///
/// # One tree, fixed at construction
///
/// A row is `(the page's version, the digest of that page)`, and those two halves are only a
/// *pair* if they were read from the same file. This type therefore takes the tree at [`new`] and
/// **hands it to the wrapped digest** on every call, so `warm` has no tree argument to get wrong
/// and the digest has no second tree to reach for. A memo over one file's pages cannot be pointed
/// at another's:
///
/// ```compile_fail
/// # use ferrodb::branch::types::PageId;
/// # use ferrodb::cow::btree::CowTree;
/// # use ferrodb::cow::cid;
/// # use ferrodb::cow::diff::MemoIdentity;
/// # fn demo(tree_a: &CowTree, tree_b: &CowTree, root_b: PageId) {
/// let ident = MemoIdentity::new(tree_a, |t: &CowTree, p: PageId| cid::subtree_cid(t, p));
/// ident.warm(tree_b, root_b).unwrap(); // `warm` takes a root and nothing else
/// # }
/// ```
///
/// This replaced a runtime check that could not fire on the case that mattered. The store used to
/// be captured on the **first** `warm`, so `Arc::ptr_eq` was trivially true there and only a
/// *second* warm from a different store was refused: a memo whose digest closed over tree A and
/// was warmed once against tree B was accepted, filled with `(B's page version, A's digest)` rows,
/// and then answered [`NodeIdentity::id_of`] with the wrong file's id — validated against the
/// right store, so [`MemoIdentity::misses`] read zero throughout.
///
/// What remains is one wilful act, not an accident: a closure may ignore the `&CowTree` it is
/// handed and capture a different one. That is visible at the call site as an ignored argument,
/// which is the most a generic adapter over an arbitrary digest can make it.
///
/// [`new`]: MemoIdentity::new
///
/// **`warm` is not optional.** An unwarmed page falls back to page identity rather than computing
/// on the spot, because computing there is exactly the per-comparison blowup this type exists to
/// prevent. [`MemoIdentity::misses`] counts those fallbacks so an unwarmed provider is visible
/// instead of silently slow; the fallback itself is sound in this store, so the *answer* is right
/// either way.
///
/// # ⚠ Do not warm a whole tree through `cow::cid::subtree_cid`
///
/// The example above is the *shape*, not a size recommendation. This adapter is only as cheap as
/// the per-page cost of the function it wraps, and `subtree_cid`'s per-page cost is the whole
/// subtree below that page. Warming N pages therefore re-reads each page once per level above it:
/// **O(N · depth), which is more than the O(N) path `cow::diff` exists to beat.** Counted through
/// a `PageStore` that tallies reads, on a 16k-row tree of 1085 nodes, in
/// `warming_a_whole_tree_through_an_on_demand_digest_costs_more_than_the_o_n_path`:
///
/// ```text
/// diff + PageIdentity, 4 rows changed         :    18 page reads
/// CowTree::diff, the O(N) path being replaced :  2188 page reads
/// SubtreeHash::stamp, bottom-up               :  1085 page reads   <- one per node, the floor
/// MemoIdentity(subtree_cid).warm              :  5410 page reads   <- 2.5x the control
///   of which subtree_cid itself               :  4325 page reads   <- 2.0x it on its own
/// ```
///
/// The split is measured, by removing the version check and re-counting: the other 1085 are this
/// adapter's own validation, exactly one per page. The verdict does not rest on them — warming
/// cost twice the path it replaces before there was anything to validate.
///
/// It still reports a healthy `skipped_subtrees`, so the counter does not give the cost away. Use
/// [`SubtreeHash`] to warm a whole tree — it folds bottom-up and reads each page once — and keep
/// this adapter for the case it is actually for: a **small, explicit** set of pages where the
/// wrapped digest is specifically what you need, such as the cross-lineage root comparison
/// `cow::cid` was built for, where page identity wins no skips at all.
///
/// # Rows are keyed on the page's version
///
/// Like [`SubtreeHash`]'s, this memo is a cache over page ids, and page ids are recycled — see
/// [`PageVersion`]. Every row carries the version it was computed at, so:
///
/// * a row whose page has been recycled or rewritten **misses** rather than answering for the
///   page's previous life, and the miss shows up in [`MemoIdentity::misses`];
/// * `warm` **re-computes** such a row instead of stepping over it. It used to skip any page id
///   already present, which made re-warming after a recycle a no-op that added zero entries and
///   moved zero counters while the memo went on returning the wrong id.
///
/// Validating costs one page fetch and a header parse per query — the 1085 reads broken out of
/// `warm`'s total above. That is the price of a stale row missing rather than lying, and
/// [`SubtreeHash`] pays it without an extra fetch at all, because it reads the version off the
/// page it was going to open anyway.
pub struct MemoIdentity<'t, F> {
    /// The one tree this memo describes, fixed at construction. Every row's version is read from
    /// it and every row's digest is computed over it, so the two halves of a row cannot come from
    /// different files. It is also what [`NodeIdentity::id_of`] validates against, which is all it
    /// can do with the [`PageId`] it is handed.
    tree: &'t CowTree,
    compute: F,
    memo: RwLock<HashMap<PageId, (PageVersion, [u8; 16])>>,
    misses: AtomicUsize,
}

impl<'t, F> MemoIdentity<'t, F>
where
    F: Fn(&CowTree, PageId) -> Result<[u8; 16], FerroError>,
{
    /// Bind a memo to `tree`. `compute` is handed that same tree on every call — see the type's
    /// docs for why it is a parameter rather than something the closure captures.
    pub fn new(tree: &'t CowTree, compute: F) -> Self {
        MemoIdentity {
            tree,
            compute,
            memo: RwLock::new(HashMap::new()),
            misses: AtomicUsize::new(0),
        }
    }

    /// Compute and memoise an id for every page under `root`. Returns how many rows were
    /// **written**, which counts a refreshed row as well as a new one.
    ///
    /// A page whose memoised row still matches the page's current [`PageVersion`] is stepped over;
    /// one whose row is stale is recomputed and overwritten. That second half is the whole point:
    /// keyed on the page id alone, re-warming a recycled page did nothing at all, so the documented
    /// remedy for a stale memo was a no-op that reported success.
    ///
    /// Read [`MemoIdentity`]'s own docs before warming a whole tree through `cid::subtree_cid` —
    /// it costs more than the path it replaces.
    ///
    /// There is no tree argument: the tree is the one given to [`MemoIdentity::new`], so a row's
    /// version and its digest are read from the same file by construction rather than by a check.
    pub fn warm(&self, root: PageId) -> Result<usize, FerroError> {
        let store = self.tree.store();
        let mut written = 0usize;
        for p in self.tree.walk_pages(root)? {
            let version = PageVersion::read(&store.read_page(p)?)?;
            if self.fresh_row(p, version).is_some() {
                continue;
            }
            let id = tag_content((self.compute)(self.tree, p)?);
            self.memo.write().unwrap().insert(p, (version, id));
            written += 1;
        }
        Ok(written)
    }

    /// Pages that were compared without a row that still describes them: never warmed, or warmed
    /// in a previous life of that page id. Both are answered by the page-identity fallback.
    /// Non-zero after a diff means the skip was cruder than the wrapped digest allows.
    pub fn misses(&self) -> usize {
        self.misses.load(AtomicOrdering::Relaxed)
    }

    /// Rows held. Like [`SubtreeHash::stamped_nodes`], rows are not re-validated to answer this,
    /// so it is "rows taken", not "rows still current".
    pub fn warmed(&self) -> usize {
        self.memo.read().unwrap().len()
    }

    /// The row for `page`, only if it still describes the page that is there now.
    fn fresh_row(&self, page: PageId, version: PageVersion) -> Option<[u8; 16]> {
        let (row_version, id) = *self.memo.read().unwrap().get(&page)?;
        (row_version == version).then_some(id)
    }
}

impl<F> NodeIdentity for MemoIdentity<'_, F>
where
    F: Fn(&CowTree, PageId) -> Result<[u8; 16], FerroError>,
{
    fn id_of(&self, page: PageId) -> [u8; 16] {
        if let Ok(handle) = self.tree.store().read_page(page)
            && let Ok(version) = PageVersion::read(&handle)
            && let Some(id) = self.fresh_row(page, version)
        {
            return id;
        }
        // Not warmed, warmed in a previous life of this page id, or unreadable. All three are
        // answered by the crude-but-sound fallback, and all three are counted: a stale row used to
        // be answered by the *wrong* id with `misses()` reading zero.
        self.misses.fetch_add(1, AtomicOrdering::Relaxed);
        page_id_identity(page)
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
/// — pairs found equal and abandoned without a decode. Node counts for the skipped subtrees are
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
    decode_payload(&store.read_page(pid)?, pid, enclosing)
}

/// [`read_payload`] for a caller that already holds the page — [`SubtreeHash::stamp_inner`] opens
/// it to read the [`PageVersion`] and would otherwise fetch the same page twice.
fn decode_payload(h: &PageHandle, pid: PageId, enclosing: &Span) -> Result<Payload, FerroError> {
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
/// [`NodeIdentity`] ids are equal is skipped in O(1) — neither page is decoded and neither subtree
/// is descended. Neither side is ever enumerated on its own, so for a small change set the work is
/// O(delta · log_m N) rather than O(N).
///
/// "O(1)" is the provider's cost, and the memoising providers here spend one page fetch and a
/// header parse in it, to check that the row they are about to answer from still describes the
/// page ([`PageVersion`]). That is bounded by the same O(delta · log_m N) comparisons, and it buys
/// the difference between a stale row *missing* and a stale row *lying*. [`PageIdentity`] has no
/// memo and so spends nothing.
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
    use crate::cow::cid;
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
            Fixture::with_extent_pages(256)
        }

        /// A fixture whose extents are small enough that freeing a branch and building another
        /// hands the same page ids back out, which is what the recycling tests need.
        fn with_extent_pages(extent_pages: u32) -> Fixture {
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
            let store = Arc::new(CowStore::with_extent_pages(pool, extent_pages));
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
    const B2: BranchId = BranchId::new(2, 0);

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
        // `inner` holds the tree's own store, so ignoring the supplied `&CowTree` here does
        // not reach a second file — see `a_memo_is_bound_to_one_tree...` for what would.
        let m = MemoIdentity::new(&f.tree, |_: &CowTree, p| inner.stamp(p));
        let warmed = m.warm(base).unwrap() + m.warm(head).unwrap();
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
        let m = MemoIdentity::new(&f.tree, |_: &CowTree, p| inner.stamp(p));
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

    // -- the memo is a cache over recycled keys ------------------------------------------------

    /// Free everything trunk owns and rebuild single-leaf trees on `B1` until the allocator hands
    /// `want` back out. Returns the recycled root, now holding `value`.
    ///
    /// Bounded on purpose: a store that stopped recycling page ids would make every test below
    /// vacuous, so the premise fails loudly instead of passing quietly.
    fn recycle_until(f: &Fixture, want: PageId, value: &str) -> PageId {
        for a in f.store.arenas_of(BranchId::TRUNK).unwrap() {
            f.store.free_arena(a).unwrap();
        }
        let e = f.tick();
        f.store.register_branch(B1, Some(BranchId::TRUNK), e).unwrap();
        for _ in 0..2000 {
            let e = f.tick();
            let r = f.tree.create(B1, e).unwrap();
            let r = f.put(r, B1, "a", value);
            if r == want {
                return r;
            }
        }
        panic!("page id {want} was never recycled; the premise of this test is false");
    }

    fn version_of(f: &Fixture, page: PageId) -> PageVersion {
        PageVersion::read(&f.store_dyn().read_page(page).unwrap()).unwrap()
    }

    /// The row's key. A `PageId` names a *slot*, not contents, and the memo must know it.
    #[test]
    fn a_recycled_page_id_misses_the_memo_instead_of_answering_for_its_previous_life() {
        let f = Fixture::with_extent_pages(8);
        let e = f.tick();
        let r1 = f.tree.create(BranchId::TRUNK, e).unwrap();
        let r1 = f.put(r1, BranchId::TRUNK, "a", "1");
        assert_eq!(f.tree.walk_pages(r1).unwrap(), vec![r1], "premise: a single-leaf tree");

        let h = SubtreeHash::new(f.store_dyn());
        let stale = h.stamp(r1).unwrap();
        assert_eq!(stale[0], TAG_CONTENT);

        let reused = recycle_until(&f, r1, "2");
        assert_eq!(reused, r1);

        // Control: the page really does hold something else now, and a provider that never saw
        // its previous life says so.
        let truth = SubtreeHash::new(f.store_dyn()).stamp(r1).unwrap();
        assert_ne!(truth, stale, "control: the two contents must hash differently");

        // The provider that did see it must not answer from the row it took back then.
        assert_ne!(h.id_of(r1), stale, "the memo answered for the page's previous life");
        assert_eq!(
            h.id_of(r1),
            page_id_identity(r1),
            "a stale row must fall back to page identity, not to some third thing"
        );
    }

    /// What the stale row cost, stated as the diff's answer rather than as an id. This is the
    /// failure that raises nothing and moves no counter: the root is skipped and the change is
    /// simply absent from the changeset.
    #[test]
    fn a_stale_row_costs_a_skip_and_never_the_change() {
        let f = Fixture::with_extent_pages(8);
        let e = f.tick();
        let r1 = f.tree.create(BranchId::TRUNK, e).unwrap();
        let r1 = f.put(r1, BranchId::TRUNK, "a", "1");

        let h = SubtreeHash::new(f.store_dyn());
        h.stamp(r1).unwrap();

        let side_a = recycle_until(&f, r1, "2");

        // A live tree holding exactly what the stale row believes is on `side_a`.
        let e = f.tick();
        f.store.register_branch(B2, Some(BranchId::TRUNK), e).unwrap();
        let e = f.tick();
        let side_b = f.tree.create(B2, e).unwrap();
        let side_b = f.put(side_b, B2, "a", "1");
        assert_ne!(side_a, side_b);
        h.stamp(side_b).unwrap();

        // The oracle. Page identity holds no memo, so it cannot be fooled by a recycle.
        let truth = diff(&f.tree, side_a, side_b, &PageIdentity).unwrap();
        assert_eq!(truth.changes.len(), 1, "oracle: there IS exactly one difference");

        let by_hash = diff(&f.tree, side_a, side_b, &h).unwrap();
        assert_eq!(
            by_hash.changes, truth.changes,
            "the memo skipped a subtree that differs: {} changes against the truth's {}",
            by_hash.changes.len(),
            truth.changes.len()
        );
    }

    /// `birth` participates in the key, pinned at the only level where it can be.
    ///
    /// The store-level test this wants cannot be written: `checksum` is a crc32 over the whole
    /// page **including** the header, so two pages that agree on `checksum` and disagree on
    /// `birth` differ only by a crc32 collision, and a collision cannot be constructed in a test.
    /// That is also why dropping `birth` breaks no store-level test in this file — it is carried
    /// for exactness on the recycle case, not to cover a class `checksum` misses. This pins that
    /// it is actually consulted; [`PageVersion`]'s docs carry the argument for keeping it.
    #[test]
    fn the_version_key_consults_the_birth_epoch_and_not_only_the_checksum() {
        let mut older = PageHeader::new(Epoch(7), crate::branch::types::ArenaId(1), PageType::BTreeLeaf);
        older.checksum = 0xdead_beef;
        let mut newer = older;
        newer.birth_epoch = Epoch(8);

        assert_ne!(
            PageVersion::of(&older),
            PageVersion::of(&newer),
            "two lives of one page id with a colliding checksum compared equal"
        );
        assert_eq!(PageVersion::of(&older), PageVersion::of(&older.clone()));
    }

    /// The half that carries the guard, and the half the obvious fix — key on `birth_epoch` —
    /// does not cover.
    ///
    /// `btree::insert` writes into a node in place once it is private to the writer and stops
    /// copying up there, so the page id and the birth epoch both stay put while the contents
    /// change. The premise is asserted before the conclusion: if this store ever stopped writing
    /// in place, the test would be proving nothing and says so instead.
    #[test]
    fn an_in_place_rewrite_invalidates_the_row_although_the_birth_epoch_is_unchanged() {
        let f = Fixture::new();
        let e = f.tick();
        let root = f.tree.create(BranchId::TRUNK, e).unwrap();
        let root = f.put(root, BranchId::TRUNK, &key(1), &value(1, 0));
        assert_eq!(f.tree.walk_pages(root).unwrap(), vec![root], "premise: a single-leaf tree");

        let h = SubtreeHash::new(f.store_dyn());
        let stale = h.stamp(root).unwrap();
        let before = version_of(&f, root);

        // Write again on the same branch: the leaf is already private, so it is rewritten where
        // it lies.
        let after_root = f.put(root, BranchId::TRUNK, &key(2), &value(2, 0));
        let after = version_of(&f, root);
        assert_eq!(after_root, root, "premise: the write did not land in place");
        assert_eq!(after.birth, before.birth, "premise: the birth epoch moved, so this is a recycle");
        assert_ne!(after.checksum, before.checksum, "premise: the page's bytes did not change");

        let truth = SubtreeHash::new(f.store_dyn()).stamp(root).unwrap();
        assert_ne!(truth, stale, "control: the leaf's contents really did change");
        assert_eq!(
            h.id_of(root),
            page_id_identity(root),
            "an in-place rewrite left a row that birth_epoch alone cannot tell is stale"
        );
    }

    /// The documented remedy has to work. `warm` used to step over any page id already in the
    /// map, so re-warming a recycled page added nothing, moved no counter, and left the wrong id
    /// in place — every instrument reading clean while the answer was wrong.
    #[test]
    fn re_warming_a_recycled_page_replaces_its_row_rather_than_stepping_over_it() {
        let f = Fixture::with_extent_pages(8);
        let e = f.tick();
        let r1 = f.tree.create(BranchId::TRUNK, e).unwrap();
        let r1 = f.put(r1, BranchId::TRUNK, "a", "1");

        let ident = MemoIdentity::new(&f.tree, |t: &CowTree, p| cid::subtree_cid(t, p).map(tag_content));
        assert_eq!(ident.warm(r1).unwrap(), 1);
        let stale = ident.id_of(r1);
        assert_eq!(ident.misses(), 0, "a freshly warmed page must not miss");

        let r1b = recycle_until(&f, r1, "2");

        // Before re-warming, the stale row must already refuse to answer — and be counted.
        let misses_before = ident.misses();
        assert_eq!(ident.id_of(r1b), page_id_identity(r1b), "a stale row answered a query");
        assert_eq!(ident.misses(), misses_before + 1, "a stale row was not counted as a miss");

        assert_eq!(ident.warm(r1b).unwrap(), 1, "re-warming added no row");
        assert_ne!(ident.id_of(r1b), stale, "re-warming did not refresh the recycled page");
        assert_eq!(
            ident.id_of(r1b),
            tag_content(cid::subtree_cid(&f.tree, r1b).unwrap()),
            "the refreshed row does not match the page that is there now"
        );
    }

    /// A memo's two halves — the page version it validates against and the digest it answers with
    /// — must come from the same file, and after D109 they do by construction: the tree is fixed
    /// at [`MemoIdentity::new`] and handed to the digest, so `warm` has no tree argument to get
    /// wrong. The fixture is the one that used to break it: two stores that hand out the *same*
    /// page ids for different content, so a memo reaching across them would answer and not miss.
    ///
    /// What this replaced: `store` was captured on the FIRST `warm` and compared with
    /// `Arc::ptr_eq`, which is trivially true there. A memo whose digest closed over tree A and
    /// was warmed once against tree B was accepted, and every row held (B's page version, A's
    /// digest) — `id_of` then validated against the right store and returned the wrong file's id
    /// with `misses()` reading zero. `bench/d109_two_store_guard_before.txt` is that run.
    #[test]
    fn a_memo_is_bound_to_one_tree_so_its_digest_and_its_version_share_a_file() {
        let a = Fixture::with_extent_pages(8);
        let b = Fixture::with_extent_pages(8);

        let ea = a.tick();
        let ra = a.tree.create(BranchId::TRUNK, ea).unwrap();
        let ra = a.put(ra, BranchId::TRUNK, "k", "A");
        let eb = b.tick();
        let rb = b.tree.create(BranchId::TRUNK, eb).unwrap();
        let rb = b.put(rb, BranchId::TRUNK, "k", "B");
        assert_eq!(ra, rb, "premise: two fresh stores hand out the same page id");

        let from_a = tag_content(cid::subtree_cid(&a.tree, ra).unwrap());
        let truth_b = tag_content(cid::subtree_cid(&b.tree, rb).unwrap());
        assert_ne!(from_a, truth_b, "control: the two files' page {ra} digest differently");

        let ident = MemoIdentity::new(&b.tree, |t: &CowTree, p| cid::subtree_cid(t, p));
        assert_eq!(ident.warm(rb).unwrap(), 1);

        let answered = ident.id_of(rb);
        assert_eq!(ident.misses(), 0, "a freshly warmed page must not miss");
        assert_ne!(
            answered, from_a,
            "the memo answered with tree A's digest for a page it validates against tree B"
        );
        assert_eq!(answered, truth_b, "the memo must answer for the tree it is bound to");

        // And the same page id in the other file is a different memo's business entirely: this
        // one holds no row that could answer for it.
        let other = MemoIdentity::new(&a.tree, |t: &CowTree, p| cid::subtree_cid(t, p));
        assert_eq!(other.warm(ra).unwrap(), 1);
        assert_eq!(other.id_of(ra), from_a);
    }

    // -- the adapter's stated purpose, over a digest that really has no memo -------------------

    /// [`MemoIdentity`] exists to wrap a digest that keeps **no memo of its own**. The wrapper's
    /// other tests hand it `SubtreeHash::stamp`, which memoises internally — so they pass just as
    /// happily against a `MemoIdentity` that memoised nothing at all. `cid::subtree_cid` has no
    /// memo, which is what makes this the test of the contract.
    #[test]
    fn a_memo_over_a_genuinely_memoless_digest_skips_and_never_falls_back() {
        let f = Fixture::new();
        let base = f.build(400);
        let mut head = f.fork(B1, base);
        head = f.put(head, B1, &key(11), "changed");

        let ident = MemoIdentity::new(&f.tree, |t: &CowTree, p| cid::subtree_cid(t, p));
        let warmed = ident.warm(base).unwrap() + ident.warm(head).unwrap();
        assert_eq!(warmed, ident.warmed());

        let r = diff(&f.tree, base, head, &ident).unwrap();
        assert_eq!(r.changes, diff(&f.tree, base, head, &PageIdentity).unwrap().changes);
        assert_eq!(r.changes.len(), 1);
        assert_eq!(ident.misses(), 0, "a fully warmed memo fell back to page identity");
        assert!(r.skipped_subtrees > 0, "the digest won no skips");
        assert_eq!(ident.id_of(base)[0], TAG_CONTENT);
    }

    // -- what whole-tree warming through an on-demand digest costs -----------------------------

    /// A [`PageStore`] that counts the pages read through it, so a cost claim in this file's docs
    /// is measured rather than estimated from `walk_pages` arithmetic. The same instrument as
    /// `cow::tests_isolation`'s `CountingStore`, which is private to that module.
    struct CountingStore {
        inner: Arc<dyn PageStore>,
        reads: AtomicUsize,
    }

    impl CountingStore {
        fn wrap(inner: Arc<dyn PageStore>) -> Arc<CountingStore> {
            Arc::new(CountingStore { inner, reads: AtomicUsize::new(0) })
        }

        /// Reads since the last call, and reset.
        fn take(&self) -> usize {
            self.reads.swap(0, AtomicOrdering::SeqCst)
        }
    }

    impl PageStore for CountingStore {
        fn read_page(&self, page_id: PageId) -> Result<crate::cow::PageHandle, FerroError> {
            self.reads.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.read_page(page_id)
        }
        fn alloc_in_arena(
            &self,
            arena: crate::branch::types::ArenaId,
            page_type: PageType,
            birth_epoch: Epoch,
        ) -> Result<PageId, FerroError> {
            self.inner.alloc_in_arena(arena, page_type, birth_epoch)
        }
        fn cow_page(
            &self,
            page_id: PageId,
            branch: BranchId,
            epoch: Epoch,
        ) -> Result<crate::cow::CowPage, FerroError> {
            self.inner.cow_page(page_id, branch, epoch)
        }
        fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), FerroError> {
            self.inner.free_page(page_id, free_epoch)
        }
        fn alloc_arena(&self, branch: BranchId) -> Result<crate::branch::types::ArenaId, FerroError> {
            self.inner.alloc_arena(branch)
        }
        fn arena_for(&self, branch: BranchId) -> Result<crate::branch::types::ArenaId, FerroError> {
            self.inner.arena_for(branch)
        }
        fn free_arena(&self, arena: crate::branch::types::ArenaId) -> Result<u32, FerroError> {
            self.inner.free_arena(arena)
        }
        fn live_page_count(&self) -> Result<u32, FerroError> {
            self.inner.live_page_count()
        }
        fn flush(&self) -> Result<(), FerroError> {
            self.inner.flush()
        }
    }

    /// The number [`MemoIdentity`]'s docs quote, and the reason they tell you not to do this.
    ///
    /// `subtree_cid`'s cost is the whole subtree below the page, so warming every page re-reads
    /// each one once per level above it. The comparison that matters is against `CowTree::diff`,
    /// the O(N) path `cow::diff` exists to beat: an adapter whose *precompute* costs more than
    /// the whole path it replaces is that path wearing a skip counter.
    ///
    /// Counted, not timed. This box is shared with an agent fleet and wall-clock is not quotable;
    /// page reads do not move when the machine is busy.
    #[test]
    fn warming_a_whole_tree_through_an_on_demand_digest_costs_more_than_the_o_n_path() {
        let f = Fixture::new();
        let base = f.build(16_000);
        let mut head = f.fork(B1, base);
        for i in [16_000 / 7, 16_000 / 3, (16_000 * 2) / 3, 15_999] {
            head = f.put(head, B1, &key(i), &value(i, 1));
        }

        let counting = CountingStore::wrap(f.store_dyn());
        let tree = CowTree::new(Arc::clone(&counting) as Arc<dyn PageStore>);
        let nodes = tree.walk_pages(base).unwrap().len();
        assert!(nodes > 100, "test needs a multi-level tree, got {nodes} pages");

        counting.take();
        let control = tree.diff(base, head).unwrap();
        let control_reads = counting.take();
        assert_eq!(control.deltas.len(), 4);

        let stamp = SubtreeHash::new(Arc::clone(&counting) as Arc<dyn PageStore>);
        stamp.stamp(base).unwrap();
        let stamp_reads = counting.take();

        let ident = MemoIdentity::new(&tree, |t: &CowTree, p| cid::subtree_cid(t, p));
        ident.warm(base).unwrap();
        let warm_reads = counting.take();

        let skipping = diff(&tree, base, head, &PageIdentity).unwrap();
        let diff_reads = counting.take();
        assert_eq!(skipping.changes.len(), 4);

        println!("  tree nodes                                 : {nodes:>8}");
        println!("  diff + PageIdentity, 4 rows changed        : {diff_reads:>8} page reads");
        println!("  CowTree::diff, the O(N) path being beaten  : {control_reads:>8} page reads");
        println!("  SubtreeHash::stamp, bottom-up              : {stamp_reads:>8} page reads");
        println!("  MemoIdentity(subtree_cid).warm             : {warm_reads:>8} page reads");

        assert!(
            diff_reads < control_reads / 10,
            "the skipping diff ({diff_reads}) is not decisively cheaper than the O(N) path \
             ({control_reads}) — the module's reason to exist is gone"
        );
        assert!(
            stamp_reads < control_reads,
            "the bottom-up precompute ({stamp_reads}) is no longer cheaper than the path it \
             replaces ({control_reads})"
        );
        assert!(
            warm_reads > control_reads,
            "warming through subtree_cid ({warm_reads}) is now CHEAPER than the O(N) path \
             ({control_reads}); MemoIdentity's docs forbid this on the strength of it being \
             dearer, and that claim has stopped being true"
        );
    }
}
