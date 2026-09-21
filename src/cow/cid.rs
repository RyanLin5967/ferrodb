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
//! # What it found, and where it stands
//!
//! The instrument's first use was to measure whether the tree deserved one. It did not: at the
//! commit that introduced this file, 1000 rows inserted ascending and shuffled produced trees
//! holding byte-identical data with **zero** leaves in common, because a byte-balanced split put
//! the boundaries wherever insertion history happened to leave them. `cow::chunker` then made every
//! boundary a function of the content, and the same fixture now agrees on all 45 leaves. Both
//! numbers, and the reversal between them, are in
//! `same_data_in_two_orders_converges_to_one_partition_cid`.
//!
//! The instrument then found a second gap on the delete path, and that one is now closed too.
//! `CowTree::delete` used to empty a leaf without unlinking it, so 500 rows inserted directly and
//! 500 rows left over from deleting half of 1000 had an identical *live* partition but differed by
//! 20 empty leaves — a false difference, reported by the one comparison that is supposed to be
//! exact across lineages. `CowTree::unlink_up` drops an emptied leaf out of the tree, and
//! `a_delete_must_not_change_the_partition_of_the_surviving_rows` is the assertion, written before
//! that fix rather than after it.
//!
//! So the reach is now: **a cid compares two trees holding the same rows exactly, however those
//! rows were reached.** The known exception is `cow::chunker`'s own — a chunk longer than a page is
//! re-cut at a finer target, and a leaf holding part of an over-long chunk cannot see the rest of
//! it. That is an `e^-8` event at the current `CHUNK_SHIFT` and is argued there, not here.
//!
//! `cow::diff` defines a `NodeIdentity` seam meant to be driven by [`subtree_cid`] through its
//! `MemoIdentity` adapter — this module deliberately does not wire itself in, because memoising
//! policy belongs to the consumer.
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
/// [`same_data_in_two_orders_converges_to_one_partition_cid`](self#tests).
///
/// # It is not a tree identity, and must not be used as one
///
/// This folds [`ordered_leaf_cids`] and **nothing else**, so it is blind to everything above the
/// leaf level: the tree's height, its separator keys, and its internal fanout. Two well-formed
/// trees over the same leaves — one of height 2, one of height 3, both answering `get` identically
/// on every key — were built and measured as sharing a partition cid while their
/// [`subtree_cid`]s differed:
///
/// ```text
/// leaf_partition_cid  A=c43e68dee8bef6fb7eb6974ad94ec6d7  B=c43e68dee8bef6fb7eb6974ad94ec6d7
/// subtree_cid         A=903bcc9ba5e969b501d3a073ca392352  B=ecc0737314252562d204f40cedd731b5
/// ```
///
/// That is by construction and is the right behaviour for the question this function asks — "is the
/// same data cut into the same leaves?" — which is exactly what an insertion-order invariance check
/// wants and what tree height would only add noise to. But it means a caller comparing two trees for
/// **sameness** wants [`subtree_cid`]. Reproduction: branch `D89-falsify`, commit `3c2a132`,
/// `examples/d89_falsify2.rs` attack J.
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

    /// The leaf partition as raw entries, in key order — **no hash anywhere**.
    ///
    /// Every `assert_eq` on two cids leans on the hash's unsound direction (equal cids do not prove
    /// equal inputs; see the module header). Where a test's conclusion is an *equality*, it
    /// establishes it on these bytes first and lets the cid assertion be the narrower claim that
    /// the instrument agrees with the bytes. Without this a hash collision would read as a
    /// property holding.
    fn ordered_leaf_entries(t: &CowTree, root: PageId) -> Vec<LeafEntries> {
        let mut out = Vec::new();
        super::for_each_leaf(t, root, 0, &mut |e: &LeafEntries| out.push(e.clone())).unwrap();
        out
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
    /// comparing two roots can drop every untouched leaf on a 16-byte comparison.
    ///
    /// **The claim is a law, not a count.** The first version of this test asserted that the leaf
    /// *count* was unchanged and that only index 0 moved. `cow::chunker` (landed after this module)
    /// broke that fixture without touching the property it was defending: overwriting key 0 with a
    /// longer value now adds a content boundary, so the tree went 45 leaves -> 46 and the assertion
    /// failed on an arithmetic detail while the thing being claimed still held exactly. Measured
    /// across key positions 0, 1, 137, 500, 998 and 999, the invariant that actually holds is
    /// sharper and indifferent to the count: **exactly one leaf cid changes**, wherever the edit
    /// lands and whether or not a boundary moves with it.
    #[test]
    fn a_subtree_untouched_by_an_edit_keeps_its_cid() {
        let (_d, cat, t) = tree();
        let base = build(&t, &cat, &(0..1000).collect::<Vec<u32>>());
        let before = ordered_leaf_cids(&t, base).unwrap();
        assert!(before.len() > 2, "tree is {} leaves; the claim would be vacuous", before.len());

        // "How many survived" is counted by membership, which is only a sound measure while the
        // leaf cids are distinct. Establish that first rather than assuming it — repeated equal
        // leaves (two empty ones, say) would make the count a multiset error rather than an answer.
        let distinct: std::collections::HashSet<&Cid> = before.iter().collect();
        assert_eq!(
            distinct.len(),
            before.len(),
            "fixture has duplicate leaf cids; the survivor count below would be a multiset error"
        );

        // Every position, not one: an edit in the first leaf and an edit in the last disturb the
        // leaf count differently, and the claim is supposed to be indifferent to that.
        for key in [0u32, 1, 137, 500, 998, 999] {
            let b = child(&cat);
            let e = cat.next_epoch();
            let head = t.insert(base, b, e, &k(key), b"edited").unwrap();
            let after = ordered_leaf_cids(&t, head).unwrap();

            let survived = before.iter().filter(|c| after.contains(c)).count();
            assert_eq!(
                survived,
                before.len() - 1,
                "editing key {key} disturbed {} of {} leaves ({} -> {} leaves); an edit must \
                 disturb exactly one, or the skip is not proportional to the change",
                before.len() - survived,
                before.len(),
                before.len(),
                after.len()
            );
        }
    }

    // ---- THE MEASUREMENT -------------------------------------------------------------------------

    /// **The frontier property, measured — and it now holds.**
    ///
    /// The same 1000 key/value pairs, inserted ascending and in a fixed shuffled order. The two
    /// trees hold byte-identical data (`leaf_content_cid` is the control, and it is what stops this
    /// from passing for the boring reason that the two trees differ), and they are now cut into the
    /// **same leaves** — so every cid agrees, and a diff between two unrelated lineages can skip
    /// every one of them on a 16-byte comparison.
    ///
    /// # This test asserted the opposite when it landed, and the reversal is the point
    ///
    /// At commit `36011fc` this file asserted **inequality**, and was right to. ferrodb then split
    /// a full node at a byte-balanced midpoint of whatever it happened to hold at that moment, so
    /// where a leaf ended was a function of insertion history rather than of content. Measured
    /// then, on this same fixture:
    ///
    /// ```text
    /// ascending insert : 9 leaves, partition cid c861c8777435729cecf24521cabccff5
    /// shuffled  insert : 8 leaves, partition cid d78571b3cea10cbb938cf0dd025d26bf
    /// content cid (both): 41cfb97ddfba11f00c343ec1bb232fe4
    /// leaves with an equal cid on both sides: 0 of 9 / 8
    /// ```
    ///
    /// Zero shared leaves between two trees holding identical rows — two branches that converged on
    /// the same data by different routes had nothing whatsoever to skip. `cow::chunker` closed it by
    /// deriving every boundary from the content (a prolly tree / POS-Tree, as in ForkBase, Noms and
    /// Dolt), and this assertion was inverted in the commit that verified the closure.
    ///
    /// The old test did not have to be hunted down. It carried a guard that fired on its own
    /// obsolescence — *"the partitioning is more content-determined than this test assumes and the
    /// gap needs restating"* — so the landing of the fix turned the suite red and named the reason,
    /// instead of leaving a stale claim quietly green. `cow::tests_chunking` proves the same
    /// convergence over raw entry lists; this proves it at the level of the **instrument a diff
    /// would actually use**.
    #[test]
    fn same_data_in_two_orders_converges_to_one_partition_cid() {
        let (_d, cat, t) = tree();
        let ascending: Vec<u32> = (0..1000).collect();
        let shuffled = fixed_shuffle(1000);
        assert_ne!(ascending, shuffled, "the shuffle is the identity; nothing is being measured");

        let asc_root = build(&t, &cat, &ascending);
        let shuf_root = build(&t, &cat, &shuffled);

        let asc_leaves = ordered_leaf_cids(&t, asc_root).unwrap();
        let shuf_leaves = ordered_leaf_cids(&t, shuf_root).unwrap();

        // Vacuity guard. A single-leaf tree has no boundaries, so agreement would prove nothing.
        assert!(
            asc_leaves.len() > 1 && shuf_leaves.len() > 1,
            "trees are {} and {} leaves; with one leaf there is no partition to agree about",
            asc_leaves.len(),
            shuf_leaves.len()
        );

        // The control. Same rows byte for byte — so everything below is about shape, not data.
        let asc_content = leaf_content_cid(&t, asc_root).unwrap();
        let shuf_content = leaf_content_cid(&t, shuf_root).unwrap();
        assert_eq!(
            asc_content, shuf_content,
            "the two trees do not hold the same data; the partition measurement would be meaningless"
        );

        let asc_cid = leaf_partition_cid(&t, asc_root).unwrap();
        let shuf_cid = leaf_partition_cid(&t, shuf_root).unwrap();
        let shared = asc_leaves.iter().filter(|c| shuf_leaves.contains(c)).count();

        // Printed, not just asserted: these numbers are the deliverable.
        println!("    ascending insert : {} leaves, partition cid {}", asc_leaves.len(), hex(&asc_cid));
        println!("    shuffled  insert : {} leaves, partition cid {}", shuf_leaves.len(), hex(&shuf_cid));
        println!("    content cid (both): {}", hex(&asc_content));
        println!(
            "    leaves with an equal cid on both sides: {shared} of {} / {} (was 0 of 9 / 8 at 36011fc)",
            asc_leaves.len(),
            shuf_leaves.len()
        );

        // Hash-free first. This is the property; the cid assertions below are the narrower claim
        // that the instrument reports it, and on their own they would also be satisfied by a
        // collision.
        assert_eq!(
            ordered_leaf_entries(&t, asc_root),
            ordered_leaf_entries(&t, shuf_root),
            "the same data must be cut into the same leaves regardless of insertion order — this \
             assertion touches no hash, so it is the one that establishes the property"
        );
        assert_eq!(
            asc_leaves, shuf_leaves,
            "the leaf entries agree but their cids do not; the instrument is reading something \
             other than the entries"
        );
        assert_eq!(asc_cid, shuf_cid, "same content must yield one partition cid");
        assert_eq!(
            subtree_cid(&t, asc_root).unwrap(),
            subtree_cid(&t, shuf_root).unwrap(),
            "and one root cid, which is what makes an O(1) subtree skip possible across lineages"
        );
    }

    /// How a row set was reached must not change how it is cut into leaves.
    ///
    /// **This was written as an `#[ignore]`d acceptance criterion before the fix existed**, so it
    /// states what the delete path had to achieve rather than what it happens to do. It failed then
    /// for the right reason — the two leaf-cid sequences agreed for 25 leaves and the churned tree
    /// carried 20 more, every one of them the cid of an empty leaf — and it passes now because
    /// `CowTree::unlink_up` drops an emptied leaf out of the tree.
    ///
    /// Measured either side of that change, on this fixture:
    ///
    /// ```text
    /// before   clean 25 leaves,  0 empty, e8051f36dbe91c9d7a1289f9275573a1
    ///          churned 45 leaves, 20 empty, 371db26401024a1b95371c569e0b4ffa
    /// after    clean 22 leaves,  0 empty, a59b5daa282b5b33f85cc0b4aefb1348
    ///          churned 22 leaves, 0 empty, a59b5daa282b5b33f85cc0b4aefb1348
    /// ```
    #[test]
    fn a_delete_must_not_change_the_partition_of_the_surviving_rows() {
        let (_d, cat, t) = tree();
        let clean = build(&t, &cat, &(0..500).collect::<Vec<u32>>());
        let mut churned = build(&t, &cat, &(0..1000).collect::<Vec<u32>>());
        let e = cat.next_epoch();
        for key in 500..1000u32 {
            churned = t.delete(churned, BranchId::TRUNK, e, &k(key)).unwrap();
        }

        assert_eq!(
            leaf_content_cid(&t, clean).unwrap(),
            leaf_content_cid(&t, churned).unwrap(),
            "control: the two trees must hold the same rows"
        );
        assert_eq!(
            ordered_leaf_cids(&t, clean).unwrap(),
            ordered_leaf_cids(&t, churned).unwrap(),
            "how a row set was reached must not change how it is cut into leaves"
        );
        assert_eq!(
            leaf_partition_cid(&t, clean).unwrap(),
            leaf_partition_cid(&t, churned).unwrap(),
            "same rows must yield one partition cid, whatever the history"
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
