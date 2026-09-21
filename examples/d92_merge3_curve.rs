//! **D92's primary evidence: what a structural three-way merge costs as the tree grows.**
//!
//! The claim is a complexity class — O(delta x log N) rather than O(N) — and a wall clock proves a
//! class only indirectly, on a box that has ten agents on it. `MergeStats::nodes_read` is an
//! operation count: the number of pages `cow::merge3::descend` actually read. It is identical on an
//! idle machine and a thrashing one, which is the only reason a number measured here is worth
//! banking.
//!
//! # The workload
//!
//! One base tree of N rows. Two branches forked off it — a fork is a metadata record and the
//! child's root **is** the parent's root, so every subtree starts shared. Each side changes 4 keys,
//! spread across the key space and disjoint from the other side's. Then merge.
//!
//! # Pre-registered expectation, written before the first run
//!
//! `merged` is built on ours, so the three subtree rules land like this:
//!
//! - A subtree neither side touched: `ours == theirs` (both still the base page). **Skipped.**
//! - A subtree only *ours* touched: `theirs == base`. **Skipped** — ours already holds it.
//! - A subtree only *theirs* touched: `ours == base`. **Descended**, because the merged tree is
//!   built on ours and theirs' change has to be found and applied, but with no conflict reachable.
//! - A subtree **both** sides touched: no rule fires, full three-way descent to the leaf.
//!
//! Either way the descent follows only the paths somebody wrote. With a B+tree of depth d that is
//! at most `(ours' paths + theirs' paths) x d` node triples, and d grows like log N. **If
//! `nodes_read` tracks the page count, the descent is not skipping and the design is wrong.**
//!
//! # Two workloads, because a counter that never fires proves nothing
//!
//! The first run of this harness used adjacent keys (`i` and `i+1`) for the two sides. They land
//! in the same leaf, so every changed path is contested, `skip_theirs_unchanged` and
//! `descend_ours_unchanged` both read **0 at every N**, and a reader would have had no way to tell
//! a rule that cannot fire from a rule that had nothing to fire on. Both workloads therefore run:
//!
//! - **contested** — the sides change two keys OF THE SAME LEAF, so they share every node down to
//!   the leaf. This is the worst case for skipping and the one that bounds `nodes_read` from above.
//!   ⚠ The pairs are read out of the leaves themselves rather than picked as `i` and `i+1`: since
//!   D89 made leaf boundaries content-defined, a boundary can fall between two adjacent keys and
//!   the arm silently stops being contested. See `Placement::keys`.
//! - **separated** — ours changes keys in the first half of the key space, theirs in the second,
//!   so most changed paths are touched by exactly one side. This is the case rules 2 and 3 exist
//!   for, and it is what makes their zeros in the contested arm readable as "nothing to skip"
//!   rather than "counter is dead".
//!
//! `ids_compared` is the other half and is reported rather than hidden: every child of a descended
//! node is *tested*, and most are retired by that test without a read. It grows like
//! `fanout x depth x changed paths`, which is sublinear in N but is not free, and folding it into
//! `nodes_read` would be the kind of per-block count labelled per-op that this repo has been bitten
//! by before.
//!
//! Run: `cargo run --release --example d92_merge3_curve`

use std::sync::Arc;

use ferrodb::branch::types::{BranchId, Epoch, PageId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::btree::CowTree;
use ferrodb::cow::merge3::{merge3, MergeStats, MerkleId, RootFastPath, ShadowId};
use ferrodb::cow::node::Node;
use ferrodb::cow::page_header::{PageHeader, PageType};
use ferrodb::cow::store::CowStore;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;

const TRUNK: BranchId = BranchId::TRUNK;
const VALUE_BYTES: usize = 100;

struct Harness {
    path: std::path::PathBuf,
    store: Arc<CowStore>,
    tree: CowTree,
    clock: std::cell::Cell<u64>,
}

impl Harness {
    fn new(tag: &str) -> Harness {
        let path = std::env::temp_dir().join(format!("d92-{}-{}.db", std::process::id(), tag));
        let _ = std::fs::remove_file(&path);
        let file =
            std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let store = Arc::new(CowStore::new(pool));
        let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);
        Harness { path, store, tree, clock: std::cell::Cell::new(1) }
    }

    fn tick(&self) -> Epoch {
        let e = self.clock.get();
        self.clock.set(e + 1);
        Epoch(e)
    }

    fn put(&self, root: PageId, br: BranchId, key: &[u8], val: &[u8]) -> PageId {
        let e = self.tick();
        self.tree.insert(root, br, e, key, val).unwrap()
    }

    fn fork(&self, id: u64) -> BranchId {
        let b = BranchId::new(id, 0);
        let e = self.tick();
        self.store.register_branch(b, Some(TRUNK), e).unwrap();
        b
    }

    /// Levels from root to leaf, counted by descending leftmost. 1 = the root is a leaf.
    fn depth(&self, root: PageId) -> usize {
        let mut pid = root;
        let mut d = 1;
        loop {
            let h = self.store.read_page(pid).unwrap();
            let f = h.read();
            if PageHeader::read_from(&f.data).unwrap().page_type == PageType::BTreeLeaf {
                return d;
            }
            pid = Node::new(&f.data).leftmost();
            d += 1;
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("{:016}", i).into_bytes()
}

/// The index a key encodes, for turning a key read back off a page into an `i`.
fn key_index(k: &[u8]) -> usize {
    std::str::from_utf8(k).unwrap().parse().unwrap()
}

/// Every leaf of `root`, in key order, as the list of keys each one holds.
///
/// `all_children` is leftmost-first, so a left-to-right descent visits leaves in key order.
fn leaf_keys(h: &Harness, root: PageId) -> Vec<Vec<Vec<u8>>> {
    fn rec(h: &Harness, pid: PageId, out: &mut Vec<Vec<Vec<u8>>>) {
        let handle = h.store.read_page(pid).unwrap();
        let f = handle.read();
        let ty = PageHeader::read_from(&f.data).unwrap().page_type;
        let node = Node::new(&f.data);
        if ty == PageType::BTreeLeaf {
            out.push((0..node.count()).map(|i| node.key(i).unwrap().to_vec()).collect());
        } else {
            for c in node.all_children().unwrap() {
                rec(h, c, out);
            }
        }
    }
    let mut out = Vec::new();
    rec(h, root, &mut out);
    out
}

/// Which leaf of `root` holds `k`. The premise-check for the contested workload reads this off
/// the tree rather than assuming it.
fn leaf_of(h: &Harness, root: PageId, k: &[u8]) -> PageId {
    let mut pid = root;
    loop {
        let handle = h.store.read_page(pid).unwrap();
        let f = handle.read();
        if PageHeader::read_from(&f.data).unwrap().page_type == PageType::BTreeLeaf {
            return pid;
        }
        let child = Node::new(&f.data).child_slot_for(k).unwrap().1;
        drop(f);
        drop(handle);
        pid = child;
    }
}

struct Arm {
    n: usize,
    pages: usize,
    depth: usize,
    stats: MergeStats,
    conflicts: usize,
}

/// How the two sides' changed keys are placed relative to each other.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// Two entries of ONE leaf: the sides share every node down to the leaf. Worst case for
    /// skipping. Taken from the built tree, not computed — see `Placement::keys`.
    Contested,
    /// Ours in the first half of the key space, theirs in the second. Most changed paths belong to
    /// exactly one side, which is what rules 2 and 3 are for.
    Separated,
}

impl Placement {
    fn name(self) -> &'static str {
        match self {
            Placement::Contested => "contested",
            Placement::Separated => "separated",
        }
    }

    /// The keys each side changes, given the built base tree, N and the per-side delta.
    ///
    /// # ⚠ Why this reads the tree instead of computing `i` and `i+1`
    ///
    /// The contested workload's whole premise is that the two sides' changed keys **share every
    /// node from the root to the leaf**, so no subtree belongs to one side alone and rules 2 and 3
    /// cannot fire. `main` asserts exactly that (`con_theirs == 0`, `con_ours == 0`), and that
    /// assertion is load-bearing: it is what makes the separated arm's nonzero counters readable
    /// as "the counter works" rather than "the counter is stuck".
    ///
    /// This used to pick `i` and `i+1` and assume adjacent keys are co-resident. **D89 made leaf
    /// boundaries content-defined** (`cow::chunker`, "chunk on the key alone"), so a boundary can
    /// fall exactly between `i` and `i+1` — and then those two keys are in *different* leaves, a
    /// subtree does belong to one side alone, and rules 2 and 3 fire in the arm that asserts they
    /// cannot. Measured on this harness at e7588cc: `skip_theirs_unch` read 2, 2, 1, 1, 0 across
    /// the five sizes where the pre-D89 banked curve (`D92-merge3` at 3c4ca6c) had 0 at every one.
    ///
    /// The premise was never "adjacent keys are adjacent in the tree" — that was an accident of a
    /// capacity-based splitter. It is "both sides write into one leaf". So the pairs are taken
    /// **out of the leaves themselves**: each pair is two entries of one leaf, which is co-resident
    /// by construction under any chunker. `arm` then re-reads the premise off the tree and asserts
    /// it, so a future layout change fails with "these two keys are in different leaves" instead of
    /// with a counter assertion four hundred lines away.
    fn keys(self, h: &Harness, base: PageId, n: usize, deltas: usize) -> (Vec<usize>, Vec<usize>) {
        match self {
            Placement::Contested => {
                // Leaves holding at least two entries: only those can host a contested pair.
                let leaves: Vec<Vec<Vec<u8>>> =
                    leaf_keys(h, base).into_iter().filter(|l| l.len() >= 2).collect();
                assert!(
                    leaves.len() >= deltas,
                    "N={n}: only {} leaves hold two or more entries, need {deltas} to place a \
                     contested pair in each. The tree is too small or the chunker is producing \
                     single-entry leaves.",
                    leaves.len()
                );
                // Spread the chosen leaves across the key space so the descent cannot get lucky
                // with locality — the same intent the old arithmetic had.
                let mut ours = Vec::with_capacity(deltas);
                let mut theirs = Vec::with_capacity(deltas);
                for j in 0..deltas {
                    let leaf = &leaves[leaves.len() * (j + 1) / (deltas + 1)];
                    ours.push(key_index(&leaf[0]));
                    theirs.push(key_index(&leaf[1]));
                }
                (ours, theirs)
            }
            Placement::Separated => {
                // Unaffected by the leaf partition: this places the two sides in opposite HALVES
                // of the key space, which no chunker can make co-resident.
                let half = n / 2;
                let ours = (0..deltas).map(|j| half * (j + 1) / (deltas + 1)).collect();
                let theirs = (0..deltas).map(|j| half + half * (j + 1) / (deltas + 1)).collect();
                (ours, theirs)
            }
        }
    }
}

/// One point on the curve: build N rows, fork twice, change `deltas` keys per side, merge.
fn arm(n: usize, deltas: usize, placement: Placement) -> Arm {
    let h = Harness::new(&format!("n{n}"));
    let e = h.tick();
    let mut base = h.tree.create(TRUNK, e).unwrap();
    let val = vec![b'v'; VALUE_BYTES];
    for i in 0..n {
        base = h.put(base, TRUNK, &key(i), &val);
    }
    let pages = h.tree.walk_pages(base).unwrap().len();
    let depth = h.depth(base);

    let ob = h.fork(2);
    let tb = h.fork(3);

    // Spread the changes across the key space so the descent cannot get lucky with locality.
    let (ours_keys, theirs_keys) = placement.keys(&h, base, n, deltas);

    // ---- the workload's premise, READ OFF THE TREE rather than assumed -----------------------
    //
    // `main` asserts that rules 2 and 3 never fire in the contested arm. That is only true if each
    // pair really does share a leaf. Assert it here, where the failure names the actual cause.
    if placement == Placement::Contested {
        for (o, t) in ours_keys.iter().zip(theirs_keys.iter()) {
            let lo = leaf_of(&h, base, &key(*o));
            let lt = leaf_of(&h, base, &key(*t));
            assert_eq!(
                lo, lt,
                "N={n} contested: keys {o} and {t} are in different leaves ({lo} vs {lt}), so a \
                 subtree belongs to one side alone and the contested arm's premise is false"
            );
        }
    }

    let mut ours = base;
    for &i in &ours_keys {
        ours = h.put(ours, ob, &key(i), b"OURS");
    }
    let mut theirs = base;
    for &i in &theirs_keys {
        theirs = h.put(theirs, tb, &key(i), b"THEIRS");
    }

    let into = h.fork(4);
    let e = h.tick();
    let r = merge3(&h.tree, base, ours, theirs, &ShadowId, into, e).unwrap();

    // ---- the harness checks the merge it is measuring -----------------------------------------
    //
    // A curve over a WRONG merge is worse than no curve. These run at every N.
    assert_eq!(
        r.stats.root_fast_path, None,
        "N={n} {}: a root fast path retired the merge, so this arm measured nothing", placement.name()
    );
    assert!(r.conflicts.is_empty(), "N={n} {}: disjoint keys must not conflict: {:?}", placement.name(), r.conflicts);
    for &i in &ours_keys {
        assert_eq!(h.tree.get(r.merged_root, &key(i)).unwrap(), Some(b"OURS".to_vec()));
    }
    for &i in &theirs_keys {
        assert_eq!(h.tree.get(r.merged_root, &key(i)).unwrap(), Some(b"THEIRS".to_vec()));
    }
    // And every other key still reads its original value. Sampled, because reading all 256k keys
    // at every arm would dominate the run; the sample is deterministic and spans the space.
    let touched: Vec<usize> = ours_keys.iter().chain(theirs_keys.iter()).copied().collect();
    let step = (n / 512).max(1);
    for i in (0..n).step_by(step) {
        if touched.contains(&i) {
            continue;
        }
        assert_eq!(
            h.tree.get(r.merged_root, &key(i)).unwrap(),
            Some(val.clone()),
            "N={n}: untouched key {i} did not survive the merge"
        );
    }

    Arm { n, pages, depth, stats: r.stats, conflicts: r.conflicts.len() }
}

/// The detector's fire-check: three trees that share nothing must report **zero** skips.
///
/// Built independently rather than forked, over the same keys with different values, so no page id
/// is shared anywhere. A skip counter that is nonzero here is counting something other than a skip,
/// and every zero it ever reported would be meaningless.
fn detector_fires(n: usize) -> (MergeStats, usize) {
    let h = Harness::new("detector");
    let mut roots = Vec::new();
    for (bi, tag) in [(11u64, b'1'), (12, b'2'), (13, b'3')] {
        let br = h.fork(bi);
        let e = h.tick();
        let mut root = h.tree.create(br, e).unwrap();
        let val = vec![tag; VALUE_BYTES];
        for i in 0..n {
            root = h.put(root, br, &key(i), &val);
        }
        roots.push(root);
    }
    let into = h.fork(14);
    let e = h.tick();
    let r = merge3(&h.tree, roots[0], roots[1], roots[2], &ShadowId, into, e).unwrap();
    assert_eq!(r.stats.root_fast_path, None);
    (r.stats, r.conflicts.len())
}

/// What the stronger identity buys: both sides make the SAME edit. Byte-identical subtrees at
/// different page ids, so `ShadowId` must descend and `MerkleId` retires it at the root.
fn convergent_edit(n: usize) -> (MergeStats, MergeStats, usize) {
    let h = Harness::new("convergent");
    let e = h.tick();
    let mut base = h.tree.create(TRUNK, e).unwrap();
    let val = vec![b'v'; VALUE_BYTES];
    for i in 0..n {
        base = h.put(base, TRUNK, &key(i), &val);
    }
    let ob = h.fork(21);
    let tb = h.fork(22);
    let ours = h.put(base, ob, &key(n / 2), b"same");
    let theirs = h.put(base, tb, &key(n / 2), b"same");
    let into = h.fork(23);

    let e = h.tick();
    let s = merge3(&h.tree, base, ours, theirs, &ShadowId, into, e).unwrap();
    let merkle = MerkleId::new(&h.tree);
    let e = h.tick();
    let m = merge3(&h.tree, base, ours, theirs, &merkle, into, e).unwrap();
    assert_eq!(m.stats.root_fast_path, Some(RootFastPath::SidesAgree));
    assert!(s.conflicts.is_empty() && m.conflicts.is_empty());
    (s.stats, m.stats, merkle.pages_hashed())
}

fn main() {
    println!("ferrodb D92 — structural three-way merge, cost curve");
    println!("build provenance: {}", ferrodb::build_provenance());
    println!(
        "instrument: MergeStats operation counts (pages read, identity comparisons). No wall clock."
    );
    println!("workload: N rows of {VALUE_BYTES}-byte values, two forks, 4 disjoint keys changed per side.");
    println!("identity: ShadowId (the page id) — exact for COW-descended trees and free to compute.");
    println!();

    let deltas = 4;
    // Overridable so the harness can be re-run cheaply while iterating on it. The banked curve is
    // the default set; a run at other sizes must say so where it is recorded.
    let sizes: Vec<usize> = std::env::var("D92_SIZES")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1_000, 4_000, 16_000, 64_000, 256_000]);

    let mut by_placement = Vec::new();
    for placement in [Placement::Contested, Placement::Separated] {
        let arms: Vec<Arm> = sizes.iter().map(|&n| arm(n, deltas, placement)).collect();

        println!("workload: {} — ours and theirs each change {deltas} keys.", placement.name());
        match placement {
            Placement::Contested => println!(
                "  each side writes a different entry of the SAME leaf, so both sides share every \
                 node from the root to the leaf."
            ),
            Placement::Separated => println!(
                "  ours in the first half of the key space, theirs in the second."
            ),
        }
        println!("        N |  pages |  depth | nodes_read | ids_cmp | skips | skip_agree | skip_theirs_unch | descend_ours_unch | leaf_triples | keys_cmp | edits");
        println!("----------|--------|--------|------------|---------|-------|------------|------------------|-------------------|--------------|----------|------");
        for a in &arms {
            let s = &a.stats;
            println!(
                "{:9} | {:6} | {:6} | {:10} | {:7} | {:5} | {:10} | {:16} | {:17} | {:12} | {:8} | {:5}",
                a.n,
                a.pages,
                a.depth,
                s.nodes_read,
                s.ids_compared,
                s.skips(),
                s.skip_sides_agree,
                s.skip_theirs_unchanged,
                s.descend_ours_unchanged,
                s.leaf_triples,
                s.keys_compared,
                s.edits_applied,
            );
            assert_eq!(a.conflicts, 0);
        }
        println!();

        let first = &arms[0];
        let last = &arms[arms.len() - 1];
        let grow = |a: usize, b: usize| if a == 0 { f64::NAN } else { b as f64 / a as f64 };
        println!("  across N = {} -> {} (x{:.0}):", first.n, last.n, grow(first.n, last.n));
        println!("    pages        {:>7} -> {:>7}   x{:.1}", first.pages, last.pages, grow(first.pages, last.pages));
        println!("    depth        {:>7} -> {:>7}", first.depth, last.depth);
        println!(
            "    nodes_read   {:>7} -> {:>7}   x{:.2}   <- the claim",
            first.stats.nodes_read,
            last.stats.nodes_read,
            grow(first.stats.nodes_read, last.stats.nodes_read)
        );
        println!(
            "    ids_compared {:>7} -> {:>7}   x{:.2}",
            first.stats.ids_compared,
            last.stats.ids_compared,
            grow(first.stats.ids_compared, last.stats.ids_compared)
        );
        println!("    nodes_read as a fraction of the tree, and against depth:");
        for a in &arms {
            println!(
                "      N={:>7}  {:>5} / {:>6} pages = {:>7.4}%   nodes_read/depth = {:.1}",
                a.n,
                a.stats.nodes_read,
                a.pages,
                100.0 * a.stats.nodes_read as f64 / a.pages as f64,
                a.stats.nodes_read as f64 / a.depth as f64
            );
        }
        println!(
            "    If this were O(N), nodes_read would have grown by the x{:.0} the page count did.",
            grow(first.pages, last.pages)
        );
        println!(
            "    It grew x{:.2}, tracking depth ({} -> {}), which is what O(delta x log N) predicts.",
            grow(first.stats.nodes_read, last.stats.nodes_read),
            first.depth,
            last.depth
        );
        println!();
        by_placement.push((placement, arms));
    }

    // ---- every skip counter must be shown to fire ----------------------------------------------
    //
    // `skip_theirs_unchanged` and `descend_ours_unchanged` read 0 at every N in the contested arm.
    // That is correct — no path there belongs to one side alone — but a zero from a counter that
    // has never been seen nonzero is indistinguishable from a counter that cannot count. The
    // separated arm is what tells the two apart, and it is asserted rather than eyeballed.
    let contested = &by_placement[0].1;
    let separated = &by_placement[1].1;
    println!("counter fire-check — each rule must be observed firing at least once:");
    let sep_theirs: usize = separated.iter().map(|a| a.stats.skip_theirs_unchanged).sum();
    let sep_ours: usize = separated.iter().map(|a| a.stats.descend_ours_unchanged).sum();
    let con_theirs: usize = contested.iter().map(|a| a.stats.skip_theirs_unchanged).sum();
    let con_ours: usize = contested.iter().map(|a| a.stats.descend_ours_unchanged).sum();
    println!("  rule 1 (ours == theirs, skip)        contested {:>5}   separated {:>5}",
        contested.iter().map(|a| a.stats.skip_sides_agree).sum::<usize>(),
        separated.iter().map(|a| a.stats.skip_sides_agree).sum::<usize>());
    println!("  rule 2 (theirs == base, skip)        contested {con_theirs:>5}   separated {sep_theirs:>5}");
    println!("  rule 3 (ours == base, two-way)       contested {con_ours:>5}   separated {sep_ours:>5}");
    assert!(sep_theirs > 0, "rule 2 never fired even when only ours touched a subtree");
    assert!(sep_ours > 0, "rule 3 never fired even when only theirs touched a subtree");
    assert_eq!(con_theirs, 0, "contested keys share every node; rule 2 cannot fire there");
    assert_eq!(con_ours, 0, "contested keys share every node; rule 3 cannot fire there");
    println!("  -> rules 2 and 3 read 0 in the contested arm because nothing there can trigger");
    println!("     them, not because the counters are dead. The separated arm proves they count.");
    println!();

    // ---- the detector has to be able to fire --------------------------------------------------
    let (d, dconf) = detector_fires(4_000);
    println!("detector fire-check — three INDEPENDENTLY built trees, nothing shared:");
    println!(
        "  skips = {}  (skip_agree {}, skip_theirs_unchanged {}), nodes_read = {}, keys_compared = {}, conflicts = {}",
        d.skips(),
        d.skip_sides_agree,
        d.skip_theirs_unchanged,
        d.nodes_read,
        d.keys_compared,
        dconf
    );
    assert_eq!(d.skips(), 0, "three unrelated trees share no subtree; a skip here is a false skip");
    assert!(d.nodes_read > 0);
    assert_eq!(d.keys_compared, 4_000, "every key must be compared when nothing can be skipped");
    assert_eq!(dconf, 4_000, "all three differ at every key, so every key conflicts");
    println!("  -> ZERO skips, all 4000 keys compared, all 4000 conflict. The counter can read 0,");
    println!("     so the large values above are a measurement and not a stuck register.");
    println!();

    // ---- what the stronger identity buys, and what it costs ------------------------------------
    let (shadow, merkle, hashed) = convergent_edit(16_000);
    println!("identity comparison — both sides make the SAME edit (truth-table row 4):");
    println!(
        "  ShadowId:  nodes_read = {:>4}, ids_compared = {:>5}, root fast path = {:?}",
        shadow.nodes_read, shadow.ids_compared, shadow.root_fast_path
    );
    println!(
        "  MerkleId:  nodes_read = {:>4}, ids_compared = {:>5}, root fast path = {:?}",
        merkle.nodes_read, merkle.ids_compared, merkle.root_fast_path
    );
    println!("  MerkleId's own cost, reported separately and NOT folded into nodes_read above:");
    println!("    pages hashed to compute the three root ids cold = {hashed}");
    println!("  Content identity retires a convergent edit at the root that page identity cannot");
    println!("  see; it pays for that by reading the tree once. That is the trade, stated both ways.");
}
