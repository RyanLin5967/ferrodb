//! Content identifiers (cids) for copy-on-write B+tree nodes: a 128-bit fingerprint of what a
//! subtree *contains*, computed without reference to where it lives.
//!
//! # Why this exists, and what it is not
//!
//! `crate::cow` states "**No content addressing**" as a deliberate non-goal, and that decision is
//! not being reversed here. That non-goal is about *page identity and liveness*: addressing a page
//! **by** its content hash makes freeing it a global question ("who else references these bytes?"),
//! which is why Dolt needs copying mark-and-sweep GC. Pages here are still addressed by `PageId`,
//! still allocated from per-branch arenas, and liveness is still the epoch interval rule in
//! `branch::record::reclaimable`. None of that changes.
//!
//! What this module adds is the *instrument*: a cid you can **compute on demand and throw away**.
//! It is never stored, never used to allocate, never consulted by the reaper, and creates no
//! reference from one page to another. Nothing here can make a page live or dead.
//!
//! # What it buys
//!
//! `CowTree::diff` prunes with **page identity**: a subtree that did not change *is* the same
//! `PageId`, so it is skipped without being read. That shortcut is exact and free, and it is why
//! the diff is cheap — but it only works **within one lineage**. Two trees built independently,
//! holding byte-identical data, share no page ids at all, so identity pruning degenerates to
//! reading both trees whole. That is the case a cid answers: "are these two subtrees the same?"
//! becomes a 16-byte comparison instead of a walk, for trees with no common ancestor.
//!
//! This is the primitive ForkBase, Noms and Dolt are built on. ferrodb had nothing like it:
//! `grep -rn 'page_hash|content_hash|blake|sha2|Merkle' src/cow/` returned zero.
//!
//! # The hash is NOT cryptographic
//!
//! [`Hasher128`] is a hand-rolled two-lane FNV-1a variant with a splitmix64-style finalizer. Zero
//! dependencies, because this crate has none and adds none. It is a *fingerprint*, not a digest,
//! and the collision assumption has two halves that must not be conflated:
//!
//! - **Sound, unconditionally:** two different cids prove the inputs differ. A hash is a function,
//!   so this direction needs no assumption at all. Any use that only has to answer "did this
//!   change?" — invalidation, skipping equal subtrees during a *scan*, a cheap changed/unchanged
//!   probe — is safe.
//! - **NOT sound against a chosen input:** equal cids do **not** prove the inputs are equal. FNV-1a
//!   is algebraically simple and trivially collidable by anyone who can choose the bytes being
//!   hashed — and in a database the bytes being hashed are user-supplied keys and values. So a cid
//!   equality must never be the *sole* authority for an operation whose wrongness is silent:
//!   deduplicating storage, declaring a merge conflict-free, or skipping a subtree in a diff whose
//!   output someone will act on. Those need either a real cryptographic hash or a verifying
//!   comparison behind the fast path.
//! - For **accidental** collisions on non-adversarial data the 128-bit width is the whole argument:
//!   assuming the finalizer approximates a random function (asserted by the avalanche test below,
//!   not merely hoped for), a collision needs on the order of 2^64 distinct subtrees.
//!
//! Upgrading to BLAKE3/SHA-256 later is a local change to [`Hasher128`] — every cid is recomputed
//! on demand, so there is nothing stored to migrate.
//!
//! # Encoding: every variable-length field is length-prefixed
//!
//! Hashing `key` and `value` by concatenation would make `("ab", "c")` and `("a", "bc")` collide
//! **by construction**, with no 2^64 anywhere in sight — a two-key tree an attacker can trigger by
//! choosing a key. [`Hasher128::field`] therefore writes the length before the bytes and is the
//! only way to feed variable-length data in; there is no raw `update(&[u8])` on the public surface
//! to reach for by mistake. `length_prefixing_prevents_field_boundary_collisions` below forces that
//! case and shows it does not collide.
//!
//! Each cid kind also starts from its own domain tag, so a leaf digest can never equal an internal
//! digest that happened to absorb the same bytes.
//!
//! # PageIds are never hashed
//!
//! That is the entire point: a cid must not change when the same content is copied to a new page.
//! `identical_trees_built_independently_have_equal_subtree_cids` forces that property rather than
//! assuming it — it builds the same tree twice, gets different roots, and demands equal cids.

use crate::branch::types::PageId;
use crate::cow::btree::CowTree;
use crate::cow::node::{InternalEntries, LeafEntries, Node};
use crate::cow::page_header::{PageHeader, PageType};
use crate::error::FerroError;

/// A 128-bit content identifier.
pub type Cid = [u8; 16];

/// Descent guard, matching `btree::MAX_DESCENT`. A well-formed tree is far shallower; exceeding it
/// means a cycle, and looping forever inside a page store is worse than failing.
const MAX_DESCENT: usize = 64;

// Domain tags. Distinct constants, so the same bytes hashed as two different kinds of thing cannot
// produce the same cid.
const TAG_LEAF: u64 = 0x6c65_6166_0000_0001; // "leaf"
const TAG_INTERNAL: u64 = 0x696e_746e_0000_0001; // "intn"
const TAG_PARTITION: u64 = 0x7061_7274_0000_0001; // "part"
const TAG_CONTENT: u64 = 0x636f_6e74_0000_0001; // "cont"

// ---- the hash ----------------------------------------------------------------------------------

const LANE_A_BASIS: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a 64-bit offset basis
const LANE_A_MULT: u64 = 0x0000_0100_0000_01b3; // FNV-1a 64-bit prime
const LANE_B_BASIS: u64 = 0x9e37_79b9_7f4a_7c15; // golden-ratio odd constant
const LANE_B_MULT: u64 = 0xff51_afd7_ed55_8ccd; // odd, from the murmur3 finalizer

/// splitmix64's finalizer. This is what turns FNV-1a's poor avalanche into something that behaves
/// like a random function for accidental inputs. It does nothing about a chosen one.
fn mix64(mut z: u64) -> u64 {
    z ^= z >> 30;
    z = z.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z ^= z >> 27;
    z = z.wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Two-lane 128-bit fingerprint. See the module header for the collision assumption; it is not a
/// cryptographic digest.
///
/// The two lanes use different constants *and* different step functions (lane B rotates before
/// multiplying), so they are not one lane observed twice — two FNV lanes differing only in their
/// starting basis would share most of their structure and the pair would be worth well under 128
/// bits.
pub struct Hasher128 {
    a: u64,
    b: u64,
    /// Total bytes absorbed, folded in at the end so a prefix can never finish like its extension.
    len: u64,
}

impl Hasher128 {
    /// Start a hash in the domain `tag`.
    pub fn new(tag: u64) -> Self {
        Hasher128 { a: LANE_A_BASIS ^ tag, b: LANE_B_BASIS ^ mix64(tag), len: 0 }
    }

    /// Absorb fixed-width bytes. Private: everything variable-length must go through [`Self::field`],
    /// and making that unreachable rather than merely discouraged is the point.
    fn absorb(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            let x = byte as u64;
            self.a = (self.a ^ x).wrapping_mul(LANE_A_MULT);
            self.b = (self.b ^ x).rotate_left(23).wrapping_mul(LANE_B_MULT);
        }
        self.len += bytes.len() as u64;
    }

    /// Absorb a variable-length field, length first. The only way in for caller-supplied bytes.
    pub fn field(&mut self, bytes: &[u8]) {
        self.number(bytes.len() as u64);
        self.absorb(bytes);
    }

    /// Absorb a fixed-width number (big-endian, like the rest of ferrodb's on-page encodings).
    pub fn number(&mut self, n: u64) {
        self.absorb(&n.to_be_bytes());
    }

    /// Absorb a child cid. Fixed width, so it needs no length prefix.
    pub fn cid(&mut self, c: &Cid) {
        self.absorb(c);
    }

    /// Finalize. Folds the length in, avalanches each lane, then crosses them so a difference
    /// confined to one lane still reaches both halves of the output.
    pub fn finish(&self) -> Cid {
        let mut a = mix64(self.a ^ self.len);
        let mut b = mix64(self.b.wrapping_add(self.len));
        a ^= b.rotate_left(31);
        b ^= a.rotate_left(17);
        let (a, b) = (mix64(a), mix64(b));
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&a.to_be_bytes());
        out[8..16].copy_from_slice(&b.to_be_bytes());
        out
    }
}

/// Render a cid as 32 lowercase hex characters, for printing a measurement.
pub fn hex(c: &Cid) -> String {
    let mut s = String::with_capacity(32);
    for byte in c {
        s.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    s
}

// ---- node decoding -----------------------------------------------------------------------------

/// A node's contents, lifted out of the page so the pin can be dropped before recursing — the same
/// discipline `CowTree::collect_unshared` uses, so a deep tree does not hold one frame per level.
enum NodeShape {
    Leaf(LeafEntries),
    /// `(leftmost child, (separator, child) pairs in key order)`.
    Internal(PageId, InternalEntries),
}

fn shape_of(tree: &CowTree, page: PageId) -> Result<NodeShape, FerroError> {
    let handle = tree.store().read_page(page)?;
    let frame = handle.read();
    let page_type = PageHeader::read_from(&frame.data)?.page_type;
    let node = Node::new(&frame.data);
    match page_type {
        PageType::BTreeLeaf => Ok(NodeShape::Leaf(node.leaf_entries()?)),
        PageType::BTreeInternal => {
            Ok(NodeShape::Internal(node.leftmost(), node.internal_entries()?))
        }
        other => Err(FerroError::Cow(format!(
            "cid: page {} is {:?}, not a btree node",
            page, other
        ))),
    }
}

// ---- cids --------------------------------------------------------------------------------------

/// The cid of one leaf's contents: its entry count, then every key and value, in key order.
///
/// Note what is absent: the leaf's own `PageId`, its arena, its birth epoch, its free space, and
/// the physical offsets of its cells. Two leaves holding the same entries have the same cid however
/// differently they are laid out.
pub fn leaf_cid(entries: &[(Vec<u8>, Vec<u8>)]) -> Cid {
    let mut h = Hasher128::new(TAG_LEAF);
    h.number(entries.len() as u64);
    for (key, value) in entries {
        h.field(key);
        h.field(value);
    }
    h.finish()
}

/// The cid of the whole subtree rooted at `page`, computed children-first.
///
/// A leaf hashes to [`leaf_cid`]. An internal node hashes its child count, then its leftmost
/// child's cid, then each `(separator key, child cid)` pair in order. No `PageId` is ever fed in,
/// so the result depends only on what the subtree holds and how it is partitioned.
///
/// **Separator keys are included deliberately.** They are not content — they are a record of where
/// the splits fell, which in this tree is a function of insertion history (see
/// [`leaf_partition_cid`]). Excluding them would hide half of the partition sensitivity while
/// leaving the other half, which is worse than either: the cid would be neither a content identity
/// nor an honest structural one.
///
/// Cost is the whole subtree, every time — there is no memo table, because a cid is computed on
/// demand and thrown away (see the module header on why nothing here is stored).
pub fn subtree_cid(tree: &CowTree, page: PageId) -> Result<Cid, FerroError> {
    subtree_cid_at(tree, page, 0)
}

fn subtree_cid_at(tree: &CowTree, page: PageId, depth: usize) -> Result<Cid, FerroError> {
    if depth > MAX_DESCENT {
        return Err(FerroError::Cow("cid: subtree walk exceeded the depth guard".into()));
    }
    match shape_of(tree, page)? {
        NodeShape::Leaf(entries) => Ok(leaf_cid(&entries)),
        NodeShape::Internal(leftmost, separators) => {
            let leftmost_cid = subtree_cid_at(tree, leftmost, depth + 1)?;
            let mut h = Hasher128::new(TAG_INTERNAL);
            h.number(separators.len() as u64 + 1);
            h.cid(&leftmost_cid);
            for (separator, child) in &separators {
                h.field(separator);
                h.cid(&subtree_cid_at(tree, *child, depth + 1)?);
            }
            Ok(h.finish())
        }
    }
}

/// Visit every leaf reachable from `root`, in key order, streaming rather than materialising.
///
/// `Node::all_children` returns the leftmost child first and then the slots in order, which *is*
/// key order; the recursion therefore needs no sort.
fn for_each_leaf<F>(
    tree: &CowTree,
    page: PageId,
    depth: usize,
    visit: &mut F,
) -> Result<(), FerroError>
where
    F: FnMut(&LeafEntries),
{
    if depth > MAX_DESCENT {
        return Err(FerroError::Cow("cid: leaf walk exceeded the depth guard".into()));
    }
    match shape_of(tree, page)? {
        NodeShape::Leaf(entries) => {
            visit(&entries);
            Ok(())
        }
        NodeShape::Internal(leftmost, separators) => {
            for_each_leaf(tree, leftmost, depth + 1, visit)?;
            for (_, child) in &separators {
                for_each_leaf(tree, *child, depth + 1, visit)?;
            }
            Ok(())
        }
    }
}

/// Every leaf's cid, in key order. The vector's **length is the partition** — it says how many
/// leaves the same data was cut into, and each entry says what landed in one of them.
///
/// This is the useful shape for a future content-based diff: two of these vectors can be compared
/// with a longest-common-subsequence walk, which finds equal runs of leaves between trees that
/// share no page ids at all.
pub fn ordered_leaf_cids(tree: &CowTree, root: PageId) -> Result<Vec<Cid>, FerroError> {
    let mut out = Vec::new();
    for_each_leaf(tree, root, 0, &mut |entries| out.push(leaf_cid(entries)))?;
    Ok(out)
}

/// The cid of the tree's **leaf partition**: the ordered sequence of per-leaf cids, folded.
///
/// Ignores `PageId`s entirely. Does **not** ignore where the leaf boundaries fall — that is the
/// measurement this module exists to make, see
/// `same_data_inserted_in_two_orders_yields_different_partition_cids`.
pub fn leaf_partition_cid(tree: &CowTree, root: PageId) -> Result<Cid, FerroError> {
    let leaves = ordered_leaf_cids(tree, root)?;
    let mut h = Hasher128::new(TAG_PARTITION);
    h.number(leaves.len() as u64);
    for leaf in &leaves {
        h.cid(leaf);
    }
    Ok(h.finish())
}

/// The cid of the tree's **logical content**: every key and value in key order, with the leaf
/// boundaries erased.
///
/// This is the control for the partition measurement. Two trees holding the same data always agree
/// here, whatever their shape — so when [`leaf_partition_cid`] disagrees while this agrees, the
/// difference is provably the partitioning and not the data. Without it, a differing partition cid
/// would be indistinguishable from the two trees simply holding different rows.
pub fn leaf_content_cid(tree: &CowTree, root: PageId) -> Result<Cid, FerroError> {
    let mut h = Hasher128::new(TAG_CONTENT);
    let mut entries = 0u64;
    for_each_leaf(tree, root, 0, &mut |leaf| {
        for (key, value) in leaf {
            h.field(key);
            h.field(value);
            entries += 1;
        }
    })?;
    h.number(entries);
    Ok(h.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::arena::ArenaPageStore;
    use crate::branch::catalog::LogBranchCatalog;
    use crate::branch::types::{BranchId, LeaseDeadline};
    use crate::branch::BranchCatalog;
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::cow::PageStore;
    use crate::storage::disk_manager::DiskManager;
    use std::sync::Arc;

    const ARENA_BASE: u32 = 1024;

    fn tree() -> (tempfile::TempDir, Arc<LogBranchCatalog>, CowTree) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("cid.db"))
            .unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let pool = Arc::new(BufferPoolManager::new(dm));
        let catalog = Arc::new(LogBranchCatalog::in_memory(1));
        let store = Arc::new(
            ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, ARENA_BASE)
                .unwrap(),
        );
        let t = CowTree::new(store as Arc<dyn PageStore>);
        (dir, catalog, t)
    }

    fn k(n: u32) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    fn v(n: u32) -> Vec<u8> {
        format!("v{n}").into_bytes()
    }

    /// Insert `keys` in the order given and return the root.
    fn build(t: &CowTree, cat: &LogBranchCatalog, keys: &[u32]) -> PageId {
        let e = cat.next_epoch();
        let mut root = t.create(BranchId::TRUNK, e).unwrap();
        for &i in keys {
            root = t.insert(root, BranchId::TRUNK, e, &k(i), &v(i)).unwrap();
        }
        root
    }

    fn child(cat: &LogBranchCatalog) -> BranchId {
        cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap().branch_id
    }

    /// A fixed permutation of `0..n`, with no dependency and no randomness.
    ///
    /// Fisher-Yates driven by a hard-coded LCG (the Numerical Recipes constants) seeded with
    /// `0x5eed_1234`. Same input, same permutation, on every machine and every run — so the cids
    /// this test prints are reproducible numbers, not samples.
    fn fixed_shuffle(n: u32) -> Vec<u32> {
        let mut v: Vec<u32> = (0..n).collect();
        let mut state: u32 = 0x5eed_1234;
        for i in (1..v.len()).rev() {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let j = (state >> 8) as usize % (i + 1);
            v.swap(i, j);
        }
        v
    }

    // ---- the hash itself -----------------------------------------------------------------------

    /// The field-boundary collision the length prefix exists to stop, forced rather than assumed.
    ///
    /// Concatenating `"ab" + "c"` and `"a" + "bc"` gives the same byte stream. If `field` did not
    /// write the length first, these two two-entry leaves would share a cid — a collision reachable
    /// by choosing a key, with none of the 2^64 the module header claims. The `absorb` line proves
    /// the underlying byte streams really are identical, so this test fails if the guard is removed
    /// rather than passing vacuously.
    #[test]
    fn length_prefixing_prevents_field_boundary_collisions() {
        let mut raw_left = Hasher128::new(TAG_LEAF);
        raw_left.absorb(b"ab");
        raw_left.absorb(b"c");
        let mut raw_right = Hasher128::new(TAG_LEAF);
        raw_right.absorb(b"a");
        raw_right.absorb(b"bc");
        assert_eq!(
            raw_left.finish(),
            raw_right.finish(),
            "without length prefixes these two inputs are the same byte stream; if this is not \
             equal the test is no longer exercising the hazard"
        );

        let left = leaf_cid(&[(b"ab".to_vec(), b"c".to_vec())]);
        let right = leaf_cid(&[(b"a".to_vec(), b"bc".to_vec())]);
        assert_ne!(left, right, "field boundaries are not encoded: ('ab','c') collides with ('a','bc')");
    }

    /// A prefix must not finish like its extension, which is what folding the length in buys.
    #[test]
    fn a_prefix_does_not_hash_like_its_extension() {
        assert_ne!(
            leaf_cid(&[(b"k".to_vec(), b"".to_vec())]),
            leaf_cid(&[(b"k".to_vec(), b"\0".to_vec())]),
            "an empty value and a one-zero-byte value must not share a cid"
        );
    }

    /// Domain separation: the same absorbed bytes in two tags must not agree.
    #[test]
    fn domain_tags_separate_otherwise_identical_inputs() {
        let mut a = Hasher128::new(TAG_LEAF);
        a.field(b"x");
        let mut b = Hasher128::new(TAG_INTERNAL);
        b.field(b"x");
        assert_ne!(a.finish(), b.finish());
    }

    /// The finalizer is load-bearing, so measure it rather than trusting it. Raw FNV-1a changes
    /// only the low bits when the last byte changes; if `mix64` were dropped this fails.
    ///
    /// Threshold 40 of 128, not 64: the expectation for a random function is 64 with a standard
    /// deviation of ~5.7, so 40 is >4 sigma below and cannot fire by chance, while still rejecting
    /// the un-finalized hash (which moves a handful of bits).
    #[test]
    fn a_one_bit_input_change_avalanches_across_the_output() {
        let mut worst = 128;
        for byte in 0u8..64 {
            for bit in 0..8 {
                let base = leaf_cid(&[(vec![byte], b"value".to_vec())]);
                let flipped = leaf_cid(&[(vec![byte ^ (1 << bit)], b"value".to_vec())]);
                let differing: u32 =
                    base.iter().zip(flipped.iter()).map(|(x, y)| (x ^ y).count_ones()).sum();
                worst = worst.min(differing);
            }
        }
        println!("    avalanche: worst-case {worst} of 128 output bits flipped by a 1-bit input change");
        assert!(
            worst >= 40,
            "worst-case avalanche was only {worst} of 128 bits; the finalizer is not mixing"
        );
    }

    // ---- the cid is independent of where the pages live ------------------------------------------

    /// **The property that makes a cid an instrument at all.** Build the same tree twice; the two
    /// roots are different pages, holding different child page ids throughout, yet the cids must
    /// be equal. The `assert_ne` on the roots keeps this from passing because both calls returned
    /// the same tree.
    #[test]
    fn identical_trees_built_independently_have_equal_subtree_cids() {
        let (_d, cat, t) = tree();
        let keys: Vec<u32> = (0..1000).collect();
        let first = build(&t, &cat, &keys);
        let second = build(&t, &cat, &keys);

        assert_ne!(first, second, "both builds returned the same root; nothing is being compared");
        let shared: Vec<PageId> = t
            .walk_pages(first)
            .unwrap()
            .into_iter()
            .filter(|p| t.walk_pages(second).unwrap().contains(p))
            .collect();
        assert!(shared.is_empty(), "the two trees share pages {shared:?}; they are not independent");

        assert_eq!(
            subtree_cid(&t, first).unwrap(),
            subtree_cid(&t, second).unwrap(),
            "same content, same shape, different page ids — cids must agree or a PageId leaked in"
        );
    }

    /// The other direction, forced: a cid that never changes is a useless one. One edited value in
    /// a 1000-key tree must move the root cid.
    #[test]
    fn one_changed_value_changes_the_root_cid() {
        let (_d, cat, t) = tree();
        let keys: Vec<u32> = (0..1000).collect();
        let base = build(&t, &cat, &keys);
        let before = subtree_cid(&t, base).unwrap();

        let b = child(&cat);
        let e = cat.next_epoch();
        let head = t.insert(base, b, e, &k(500), b"edited").unwrap();

        assert_ne!(before, subtree_cid(&t, head).unwrap(), "an edited value did not move the cid");
        assert_ne!(
            leaf_content_cid(&t, base).unwrap(),
            leaf_content_cid(&t, head).unwrap(),
            "an edited value did not move the content cid either"
        );
    }

    /// An unchanged subtree keeps its cid across an edit elsewhere. This is the O(1) skip: a diff
    /// comparing these two roots can drop this whole subtree on a 16-byte comparison.
    #[test]
    fn a_subtree_untouched_by_an_edit_keeps_its_cid() {
        let (_d, cat, t) = tree();
        let base = build(&t, &cat, &(0..1000).collect::<Vec<u32>>());
        let before = ordered_leaf_cids(&t, base).unwrap();
        assert!(before.len() > 2, "tree is {} leaves; the claim would be vacuous", before.len());

        let b = child(&cat);
        let e = cat.next_epoch();
        let head = t.insert(base, b, e, &k(0), b"edited").unwrap();
        let after = ordered_leaf_cids(&t, head).unwrap();

        assert_eq!(before.len(), after.len(), "the edit resplit the tree; wrong fixture for this claim");
        assert_ne!(before[0], after[0], "the edited leaf must have moved");
        assert_eq!(
            before[1..],
            after[1..],
            "leaves the edit did not touch changed cid; the cid is picking up something physical"
        );
    }

    // ---- THE MEASUREMENT -------------------------------------------------------------------------

    /// **The frontier gap, measured.**
    ///
    /// The same 1000 key/value pairs, inserted ascending and in a fixed shuffled order. The two
    /// trees hold byte-identical data — `leaf_content_cid` proves it, and that assertion is what
    /// stops this test from passing for the boring reason that the two trees differ. Yet their
    /// `leaf_partition_cid`s differ, because ferrodb splits a full node at a byte-balanced midpoint
    /// of *whatever it happens to contain at that moment*. Where a leaf boundary falls is therefore
    /// a function of insertion history, not of content.
    ///
    /// The consequence is the thing that matters: two branches that converge on the same rows by
    /// different routes have no equal subtrees to skip. Content addressing buys nothing here yet,
    /// because nothing is addressed by content — only by where a split happened to land.
    ///
    /// # What must change for this to become an equality assertion
    ///
    /// The leaf boundaries must be a deterministic function of the content, not of the insertion
    /// order. That is **content-defined chunking**, the prolly tree of Noms/Dolt/ForkBase: run a
    /// rolling hash over the entries in key order and cut a leaf wherever the hash's low `k` bits
    /// are zero, giving an expected leaf size of `2^k` entries and — crucially — a boundary that
    /// any tree holding these bytes will place identically. The change is in
    /// `cow::btree`'s split path (`split_point` in `cow::node` is the byte-balanced rule that would
    /// be replaced), not in this module: `leaf_partition_cid` is already order-agnostic in every
    /// respect except the one the tree itself imposes.
    ///
    /// When that lands, delete this test and remove the `#[ignore]` from
    /// [`same_data_in_two_orders_must_converge_to_one_partition_cid`], which asserts the equality
    /// the fix must produce.
    #[test]
    fn same_data_inserted_in_two_orders_yields_different_partition_cids() {
        let (_d, cat, t) = tree();
        let ascending: Vec<u32> = (0..1000).collect();
        let shuffled = fixed_shuffle(1000);
        assert_ne!(ascending, shuffled, "the shuffle is the identity; nothing is being measured");

        let asc_root = build(&t, &cat, &ascending);
        let shuf_root = build(&t, &cat, &shuffled);

        let asc_leaves = ordered_leaf_cids(&t, asc_root).unwrap();
        let shuf_leaves = ordered_leaf_cids(&t, shuf_root).unwrap();

        // Vacuity guard. A single-leaf tree has no boundaries, so a difference could not show.
        assert!(
            asc_leaves.len() > 1 && shuf_leaves.len() > 1,
            "trees are {} and {} leaves; with one leaf there is no partition to differ",
            asc_leaves.len(),
            shuf_leaves.len()
        );

        // The control. Same rows, same order, byte for byte — so anything below is about shape.
        let asc_content = leaf_content_cid(&t, asc_root).unwrap();
        let shuf_content = leaf_content_cid(&t, shuf_root).unwrap();
        assert_eq!(
            asc_content, shuf_content,
            "the two trees do not even hold the same data; the partition measurement would be \
             meaningless"
        );

        let asc_cid = leaf_partition_cid(&t, asc_root).unwrap();
        let shuf_cid = leaf_partition_cid(&t, shuf_root).unwrap();

        // How total the gap is. The partition cids differing could, on its own, be read as nothing
        // worse than "one tree has an extra leaf". This is the number that refutes that reading:
        // how many leaves a content-based diff between these two trees could actually skip.
        let shared = asc_leaves.iter().filter(|c| shuf_leaves.contains(c)).count();

        // Printed, not just asserted: these numbers are the deliverable.
        println!("    ascending insert : {} leaves, partition cid {}", asc_leaves.len(), hex(&asc_cid));
        println!("    shuffled  insert : {} leaves, partition cid {}", shuf_leaves.len(), hex(&shuf_cid));
        println!("    content cid (both): {}", hex(&asc_content));
        println!(
            "    leaves with an equal cid on both sides: {shared} of {} / {} — a content-based \
             diff between two trees holding IDENTICAL data can skip {shared} of them",
            asc_leaves.len(),
            shuf_leaves.len()
        );
        assert!(
            shared * 2 < asc_leaves.len(),
            "{shared} of {} leaves already match across insertion orders; the partitioning is more \
             content-determined than this test assumes and the gap needs restating",
            asc_leaves.len()
        );

        assert_ne!(
            asc_cid, shuf_cid,
            "partition cids already agree — content-defined chunking has landed, so delete this \
             test and un-ignore same_data_in_two_orders_must_converge_to_one_partition_cid"
        );
    }

    /// The equality the fix must produce. Ignored because it fails today, by design — it is the
    /// acceptance criterion for content-defined chunking, written before the work rather than
    /// after, so it cannot be quietly weakened to match whatever gets built.
    ///
    /// Un-ignore this and delete
    /// [`same_data_inserted_in_two_orders_yields_different_partition_cids`] together, in the commit
    /// that replaces the byte-balanced split with a rolling-hash boundary.
    #[test]
    #[ignore = "asserts the post-content-defined-chunking behaviour; fails today, on purpose"]
    fn same_data_in_two_orders_must_converge_to_one_partition_cid() {
        let (_d, cat, t) = tree();
        let asc_root = build(&t, &cat, &(0..1000).collect::<Vec<u32>>());
        let shuf_root = build(&t, &cat, &fixed_shuffle(1000));

        assert_eq!(
            leaf_content_cid(&t, asc_root).unwrap(),
            leaf_content_cid(&t, shuf_root).unwrap(),
            "control: the two trees must hold the same data"
        );
        assert_eq!(
            ordered_leaf_cids(&t, asc_root).unwrap(),
            ordered_leaf_cids(&t, shuf_root).unwrap(),
            "the same data must be cut into the same leaves regardless of insertion order"
        );
        assert_eq!(
            leaf_partition_cid(&t, asc_root).unwrap(),
            leaf_partition_cid(&t, shuf_root).unwrap(),
            "same content must yield one partition cid"
        );
        assert_eq!(
            subtree_cid(&t, asc_root).unwrap(),
            subtree_cid(&t, shuf_root).unwrap(),
            "and one root cid, which is what makes an O(1) subtree skip possible across lineages"
        );
    }

    /// A cid is only useful if it is cheaper than the thing it replaces, and it is only sound to
    /// compare two of them if both were computed over the same kind of object. Guard the second:
    /// asking for a subtree cid of a non-btree page must refuse rather than hash whatever is there.
    #[test]
    fn a_non_btree_page_is_refused_rather_than_hashed() {
        let (_d, cat, t) = tree();
        let e = cat.next_epoch();
        let heap = t.store().alloc_for(BranchId::TRUNK, PageType::Heap, e).unwrap();
        let err = subtree_cid(&t, heap).unwrap_err();
        assert!(
            format!("{err}").contains("not a btree node"),
            "expected a refusal naming the page type, got: {err}"
        );
    }
}
