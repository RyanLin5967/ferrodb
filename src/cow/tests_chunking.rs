//! Structural invariance of the leaf level.
//!
//! The property under test is the one a prolly tree / POS-Tree (ForkBase, VLDB'18; Noms; Dolt)
//! has and a textbook B+tree does not: **identical content produces an identical partition**,
//! whatever order the keys arrived in. A byte-balanced split cuts a node where it happened to be
//! half full, so the boundary is a fact about insertion history; a content-defined boundary is a
//! fact about the bytes, so two trees holding the same set agree on it.
//!
//! Why that matters here and not merely aesthetically: ferrodb's whole branch story is that a
//! subtree which did not change *is the same page id* (see [`crate::cow::btree::CowTree::diff`]).
//! Without structural invariance, two branches that converge on the same content still disagree
//! about where their leaves end, so nothing above those leaves can ever be recognised as shared.
//!
//! These tests deliberately assert the **leaf partition**, not page bytes. `PageId`s are handed
//! out in allocation order, so internal cells differ between two trees that are structurally
//! identical; making the pages themselves comparable is a separate change.

use std::sync::Arc;

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};

use tempfile::TempDir;

use crate::branch::types::{BranchId, Epoch, PageId};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::cow::btree::CowTree;
use crate::cow::chunker;
use crate::cow::node::{self, Node};
use crate::cow::page_header::{PageHeader, PageType};
use crate::cow::store::CowStore;
use crate::cow::PageStore;
use crate::storage::disk_manager::DiskManager;

struct Fixture {
    _dir: TempDir,
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
        let tree = CowTree::new(store as Arc<dyn PageStore>);
        Fixture { _dir: dir, tree, clock: AtomicU64::new(1) }
    }

    fn tick(&self) -> Epoch {
        Epoch(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    /// Build a tree by inserting `pairs` in exactly the order given.
    fn build(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> PageId {
        let e = self.tick();
        let mut root = self.tree.create(BranchId::TRUNK, e).unwrap();
        for (k, v) in pairs {
            let e = self.tick();
            root = self.tree.insert(root, BranchId::TRUNK, e, k, v).unwrap();
        }
        root
    }

    /// The leaf level, left to right: leaf `i`'s cells, in order.
    ///
    /// `walk_pages` is a DFS whose output order is an artefact of its stack, so this descends
    /// in key order instead — the partition is a sequence, and comparing it needs that sequence.
    fn leaf_partition(&self, root: PageId) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        self.visit(root, 0, &mut out);
        out
    }

    fn visit(&self, pid: PageId, depth: usize, out: &mut Vec<Vec<(Vec<u8>, Vec<u8>)>>) {
        assert!(depth < 64, "descent guard");
        let store = self.tree.store();
        let h = store.read_page(pid).unwrap();
        let (ty, children, entries) = {
            let f = h.read();
            let ty = PageHeader::read_from(&f.data).unwrap().page_type;
            let n = Node::new(&f.data);
            match ty {
                PageType::BTreeLeaf => (ty, Vec::new(), n.leaf_entries().unwrap()),
                PageType::BTreeInternal => (ty, n.all_children().unwrap(), Vec::new()),
                other => panic!("page {} is a {:?}", pid, other),
            }
        };
        drop(h);
        match ty {
            PageType::BTreeLeaf => out.push(entries),
            _ => {
                for c in children {
                    self.visit(c, depth + 1, out);
                }
            }
        }
    }
}

/// The key/value set both trees hold. Small, fixed-width keys and short values: the same shape
/// the rest of the cow suite builds its trees from.
fn pairs(n: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            (
                format!("key{:06}", i).into_bytes(),
                format!("value-{}", i).into_bytes(),
            )
        })
        .collect()
}

/// A fixed permutation of `0..n`, produced by a Fisher-Yates shuffle driven by a hard-coded
/// xorshift64 seed. Nothing here reads a clock or an RNG the test does not own, so the
/// permutation is a constant of the test — it is written as code rather than as a literal only
/// because a 1500-element literal is unreadable, not because it varies.
fn shuffled(n: u32, seed: u64) -> Vec<u32> {
    let mut idx: Vec<u32> = (0..n).collect();
    let mut s = seed;
    let mut next = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    for i in (1..idx.len()).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        idx.swap(i, j);
    }
    idx
}

/// Hard-coded xorshift64 seeds. One shuffle can agree with an ascending build by luck; several
/// cannot, and a fixed list keeps the test reproducible rather than merely repeated.
const SEEDS: [u64; 4] = [0x5DEE_CE66_D1F3_A7B9, 0x0123_4567_89AB_CDEF, 0xDEAD_BEEF_CAFE_F00D, 0x9E37_79B9_7F4A_7C15];

/// The set, reordered by the permutation `seed` names.
fn permute(set: &[(Vec<u8>, Vec<u8>)], seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    shuffled(set.len() as u32, seed).iter().map(|&i| set[i as usize].clone()).collect()
}

fn describe(part: &[Vec<(Vec<u8>, Vec<u8>)>]) -> String {
    let sizes: Vec<usize> = part.iter().map(|l| l.len()).collect();
    let total: usize = sizes.iter().sum();
    format!("{} leaves, {} cells, sizes {:?}", sizes.len(), total, sizes)
}

/// **The test this whole change exists for.**
///
/// Tree A takes the pairs in ascending key order. Tree B takes the identical set in a fixed
/// shuffled permutation. Both must end up with the same leaf partition: the same number of
/// leaves, and leaf `i` holding exactly the same cells.
#[test]
fn insertion_order_does_not_change_the_leaf_partition() {
    const N: u32 = 1500;
    let set = pairs(N);

    let fa = Fixture::new();
    let root_a = fa.build(&set);

    let fb = Fixture::new();
    let root_b = fb.build(&permute(&set, SEEDS[0]));

    let a = fa.leaf_partition(root_a);
    let b = fb.leaf_partition(root_b);

    // Both trees must hold the set, or the partition comparison is comparing two wrong answers.
    let flat_a: Vec<_> = a.iter().flatten().cloned().collect();
    let flat_b: Vec<_> = b.iter().flatten().cloned().collect();
    assert_eq!(flat_a, set, "tree A does not hold the set in order");
    assert_eq!(flat_b, set, "tree B does not hold the set in order");
    assert!(a.len() > 8, "fixture too small to be a partition test: {}", describe(&a));

    assert_eq!(
        a.len(),
        b.len(),
        "leaf count depends on insertion order\n  ascending: {}\n  shuffled : {}",
        describe(&a),
        describe(&b)
    );
    for (i, (la, lb)) in a.iter().zip(b.iter()).enumerate() {
        assert_eq!(
            la,
            lb,
            "leaf {} differs between the two insertion orders \
             ({} cells ascending vs {} cells shuffled)\n  ascending: {}\n  shuffled : {}",
            i,
            la.len(),
            lb.len(),
            describe(&a),
            describe(&b)
        );
    }
}

/// The chunker's partition of the whole sorted set: what a tree holding this content is supposed
/// to look like, computed without reference to any tree.
fn canonical(set: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let sizes: Vec<usize> = set.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).collect();
    let hashes: Vec<u32> = set.iter().map(|(k, _)| chunker::key_hash(k)).collect();
    let cuts = chunker::leaf_cuts(&sizes, &hashes, node::NODE_CAPACITY);
    let mut bounds = vec![0usize];
    bounds.extend_from_slice(&cuts);
    bounds.push(set.len());
    bounds.windows(2).map(|w| set[w[0]..w[1]].to_vec()).collect()
}

/// Equality between two builds is necessary but not sufficient: two identically-wrong trees would
/// also satisfy it, and so would a rule that ignored content and cut every 64 rows. This pins the
/// partition to the decision function itself, so what is being proved is invariance **to
/// content** rather than a shared accident of the two builds.
#[test]
fn the_tree_agrees_with_the_chunker_about_where_the_leaves_end() {
    const N: u32 = 1500;
    let set = pairs(N);
    let want = canonical(&set);
    assert!(want.len() > 8, "fixture too small: {}", describe(&want));

    let fa = Fixture::new();
    assert_eq!(fa.leaf_partition(fa.build(&set)), want, "ascending build");

    for seed in SEEDS {
        let fb = Fixture::new();
        let got = fb.leaf_partition(fb.build(&permute(&set, seed)));
        assert_eq!(
            got.len(),
            want.len(),
            "seed {:#x}: {} leaves against the chunker's {}\n  built     : {}\n  chunker   : {}",
            seed,
            got.len(),
            want.len(),
            describe(&got),
            describe(&want)
        );
        assert_eq!(got, want, "seed {:#x} built a different partition", seed);
    }
}

/// Every leaf but the last must end on a content boundary, with none in any leaf's interior.
///
/// This is the invariant stated directly rather than through two builds agreeing. The count of
/// leaves that end somewhere else is the count that took the byte-capacity escape hatch, which is
/// the one path `chunker` cannot keep invariant; it has to be zero for this fixture, or the test
/// above is passing through a weaker guarantee than it claims.
#[test]
fn every_leaf_but_the_last_ends_on_a_content_boundary() {
    let set = pairs(1500);
    let f = Fixture::new();
    let part = f.leaf_partition(f.build(&set));

    let mut capped = 0usize;
    for (i, leaf) in part.iter().enumerate() {
        assert!(!leaf.is_empty(), "leaf {} is empty", i);
        for (j, (k, v)) in leaf.iter().enumerate().take(leaf.len() - 1) {
            assert!(
                !chunker::is_boundary(k, v),
                "leaf {} holds a boundary cell at interior position {}",
                i,
                j
            );
        }
        let (k, v) = leaf.last().unwrap();
        if i + 1 < part.len() && !chunker::is_boundary(k, v) {
            capped += 1;
        }
    }
    assert_eq!(
        capped, 0,
        "{} of {} leaves were cut by the byte cap rather than by content. {}",
        capped,
        part.len(),
        describe(&part)
    );
}

/// Fanout, and what it cost.
///
/// Chunk lengths are geometric, so the mean chunk has to sit well below a page or the tail of the
/// distribution spills over it — and a chunk that does not fit a page is the one case the
/// partition stops being a function of content. `chunker::CHUNK_SHIFT` buys that headroom with
/// fanout, and this is the assertion that keeps the trade where it was put rather than letting it
/// drift: leaves average about `NODE_CAPACITY / CHUNK_SHIFT` bytes, not a number nobody chose.
///
/// Measured against the byte-balanced split this replaces, on the same 1500 rows: 68 rows per
/// leaf before, about 15 after.
#[test]
fn leaves_average_the_target_chunk_size_and_never_exceed_a_page() {
    let set = pairs(1500);
    let f = Fixture::new();
    let part = f.leaf_partition(f.build(&set));

    let total: usize = set.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).sum();
    let mean = total as f64 / part.len() as f64;
    let target = chunker::TARGET_CHUNK_BYTES as f64;
    assert!(
        mean > target * 0.6 && mean < target * 1.4,
        "leaves average {:.0} bytes against a {:.0}-byte target. {}",
        mean,
        target,
        describe(&part)
    );
    for (i, leaf) in part.iter().enumerate() {
        let bytes: usize = leaf.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).sum();
        assert!(
            bytes <= node::NODE_CAPACITY,
            "leaf {} holds {} bytes, over the {}-byte page",
            i,
            bytes,
            node::NODE_CAPACITY
        );
    }
}

/// Rows big enough that only a handful fit a page still chunk by content, and still fit.
///
/// The byte-denominated target exists for this case: a row-counting predicate tuned for 30-byte
/// rows would put sixteen 500-byte rows in a chunk and hand the page 8 KB.
#[test]
fn wide_rows_chunk_by_content_and_still_fit_the_page() {
    let set: Vec<(Vec<u8>, Vec<u8>)> = (0..600u32)
        .map(|i| (format!("key{:06}", i).into_bytes(), vec![(i % 251) as u8; 400]))
        .collect();
    let want = canonical(&set);
    assert!(want.len() > 8, "fixture too small: {} leaves", want.len());

    let fa = Fixture::new();
    assert_eq!(fa.leaf_partition(fa.build(&set)), want, "ascending build of wide rows");
    let fb = Fixture::new();
    assert_eq!(
        fb.leaf_partition(fb.build(&permute(&set, SEEDS[0]))),
        want,
        "shuffled build of wide rows"
    );
    for (i, leaf) in want.iter().enumerate() {
        let bytes: usize = leaf.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).sum();
        assert!(bytes <= node::NODE_CAPACITY, "leaf {} holds {} bytes", i, bytes);
    }
}

/// A rewritten value must not re-chunk the tree.
///
/// This is the end-to-end form of `chunker::rewriting_a_value_does_not_move_a_boundary`, and the
/// reason the chunking hash reads the key alone: a branch that updates a column should shadow the
/// leaves it touched and nothing else. If boundaries moved with values, the leaves either side
/// would be rewritten too and `CowTree::diff` — which prunes only on identical page ids — would
/// stop seeing the rest of the tree as shared.
#[test]
fn rewriting_values_in_place_leaves_the_partition_alone() {
    let set = pairs(1500);
    let f = Fixture::new();
    let mut root = f.build(&set);
    let before = f.leaf_partition(root);

    // Same-length replacements, so only the value bytes change.
    let updated: Vec<(Vec<u8>, Vec<u8>)> = set
        .iter()
        .map(|(k, v)| (k.clone(), vec![b'Z'; v.len()]))
        .collect();
    for (k, v) in &updated {
        let e = f.tick();
        root = f.tree.insert(root, BranchId::TRUNK, e, k, v).unwrap();
    }
    let after = f.leaf_partition(root);

    assert_eq!(
        after.iter().map(|l| l.len()).collect::<Vec<_>>(),
        before.iter().map(|l| l.len()).collect::<Vec<_>>(),
        "rewriting every value moved the leaf boundaries\n  before: {}\n  after : {}",
        describe(&before),
        describe(&after)
    );
    assert_eq!(after, canonical(&updated), "the rewritten tree is not the chunker's partition");
}

/// Both sides of [`node::MAX_KEY_BYTES`], which is one byte tighter than the leaf alone implies.
///
/// A key can fit a leaf entry and still be too large to *separate* one, because a content cut
/// promotes the boundary key plus a zero byte. That narrowed the largest usable key by one when
/// cuts became content-defined, so it is a real behaviour change and both sides are pinned: the
/// key at the limit has to survive a split, and the one past it has to be refused.
///
/// The refusal is asserted by its **outcome**, not its wording: an up-front refusal spends no
/// pages, where refusing from inside `internal_relink` would already have split a leaf. The page
/// count is the only thing that tells those two apart from outside.
#[test]
fn the_key_limit_holds_on_both_sides_and_refuses_before_spending_a_page() {
    let f = Fixture::new();
    let mut root = f.build(&pairs(200));
    let before = f.leaf_partition(root);
    let pages_before = f.tree.store().live_page_count().unwrap();

    // The value is sized so the entry itself is exactly at the leaf limit, which is what makes
    // the internal node's limit the binding one.
    let at_limit = |i: u8| {
        let mut k = vec![b'k'; node::MAX_KEY_BYTES];
        k[0] = i;
        k
    };
    let value = vec![b'v'; node::MAX_ENTRY_BYTES - node::leaf_entry_bytes(&at_limit(0), b"")];
    assert_eq!(
        node::leaf_entry_bytes(&at_limit(0), &value),
        node::MAX_ENTRY_BYTES,
        "fixture: the entry must sit exactly at the leaf limit"
    );

    // Enough of them to force splits: four of these fill a page exactly.
    for i in 0..10u8 {
        let e = f.tick();
        root = f
            .tree
            .insert(root, BranchId::TRUNK, e, &at_limit(i), &value)
            .unwrap_or_else(|e| panic!("a key at the limit was refused: {}", e));
    }
    for i in 0..10u8 {
        assert_eq!(
            f.tree.get(root, &at_limit(i)).unwrap().as_deref(),
            Some(value.as_slice()),
            "key {} at the limit did not survive the splits",
            i
        );
    }

    // One byte over, and nothing may happen at all.
    let mut too_long = vec![b'k'; node::MAX_KEY_BYTES + 1];
    too_long[0] = b'z';
    let short = vec![b'v'; 1];
    assert!(
        node::leaf_entry_bytes(&too_long, &short) <= node::MAX_ENTRY_BYTES,
        "fixture: the over-long key must still fit a leaf, or it trips the other limit"
    );
    let partition_before = f.leaf_partition(root);
    let pages = f.tree.store().live_page_count().unwrap();

    let err = f.tree.insert(root, BranchId::TRUNK, f.tick(), &too_long, &short).unwrap_err();
    assert!(err.to_string().contains("key limit"), "unexpected error: {}", err);
    assert_eq!(
        f.tree.store().live_page_count().unwrap(),
        pages,
        "the refused insert allocated a page, so it was refused after splitting rather than before"
    );
    assert_eq!(f.tree.get(root, &too_long).unwrap(), None, "the refused key reached the tree");
    assert_eq!(f.leaf_partition(root), partition_before, "the refused insert moved a boundary");
    assert!(pages_before <= pages, "fixture: the limit-sized inserts should have grown the tree");
    assert!(before.len() <= partition_before.len());
}
