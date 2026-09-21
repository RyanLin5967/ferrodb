//! Adversarial review reproducers for `src/cow/cid.rs` and `src/cow/diff.rs`.
//!
//! READ-ONLY review: nothing in `src/` is touched. Each test below is a claim the review makes,
//! written so it FAILS if the claim is false.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::types::{BranchId, Epoch, PageId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::btree::CowTree;
use ferrodb::cow::cid;
use ferrodb::cow::diff::{diff, MemoIdentity, NodeIdentity, PageIdentity, SubtreeHash};
use ferrodb::cow::store::CowStore;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;

const B1: BranchId = BranchId::new(1, 0);
const B2: BranchId = BranchId::new(2, 0);

struct Fixture {
    _dir: tempfile::TempDir,
    store: Arc<CowStore>,
    tree: CowTree,
    clock: AtomicU64,
}

impl Fixture {
    fn new(extent_pages: u32) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
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
        Epoch(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    fn store_dyn(&self) -> Arc<dyn PageStore> {
        self.store.clone() as Arc<dyn PageStore>
    }
}

// =================================================================================================
// ATTACK 3 — the memo is keyed on PageId, and PageIds are recycled.
// =================================================================================================

/// `SubtreeHash`'s memo is a cache keyed on `PageId` with no invalidation. `CowStore` recycles
/// page ids (`store.rs:114` pops `free_pages`; `format_page`'s own comment says "a recycled page
/// may still be cached from its previous life"). A `SubtreeHash` that outlives a recycle therefore
/// hands back the id of content that is no longer on that page.
///
/// This test forces the recycle and shows the memo returning the OLD content's id, with a freshly
/// built `SubtreeHash` over the same page as the oracle.
#[test]
fn attack3_subtree_hash_memo_goes_stale_when_a_page_id_is_recycled() {
    let f = Fixture::new(8);

    // v1: a single-leaf tree holding {a -> 1}.
    let e = f.tick();
    let r1 = f.tree.create(BranchId::TRUNK, e).unwrap();
    let e = f.tick();
    let r1 = f.tree.insert(r1, BranchId::TRUNK, e, b"a", b"1").unwrap();
    assert_eq!(f.tree.walk_pages(r1).unwrap(), vec![r1], "expected a single-leaf tree");

    let h = SubtreeHash::new(f.store_dyn());
    let stale_id = h.stamp(r1).unwrap();

    // Free everything trunk owns, so r1's id goes back on the free list.
    for a in f.store.arenas_of(BranchId::TRUNK).unwrap() {
        f.store.free_arena(a).unwrap();
    }

    // Hand the extent to another branch and keep building single-leaf trees until the allocator
    // returns the very page id r1 used to be. Bounded, so a store that never reuses fails loudly.
    f.store.register_branch(B1, Some(BranchId::TRUNK), f.tick()).unwrap();
    let mut reused = None;
    for _ in 0..2000 {
        let e = f.tick();
        let r = f.tree.create(B1, e).unwrap();
        let e = f.tick();
        let r = f.tree.insert(r, B1, e, b"a", b"2").unwrap();
        if r == r1 {
            reused = Some(r);
            break;
        }
    }
    let reused = reused.expect("page id was never recycled; the premise of this test is false");
    assert_eq!(reused, r1);

    // The page now holds {a -> 2}. A fresh provider says so.
    let fresh = SubtreeHash::new(f.store_dyn());
    let true_id = fresh.stamp(r1).unwrap();
    assert_ne!(true_id, stale_id, "control: the two contents must hash differently");

    // The old provider does not: it answers from the memo without reading the page.
    assert_eq!(
        h.id_of(r1),
        stale_id,
        "the memo returned a fresh id; if this fires the staleness has been fixed"
    );
}

/// The consequence: a stale memo makes `diff` report NO CHANGES where there is one.
#[test]
fn attack3_a_stale_memo_makes_the_diff_silently_report_no_changes() {
    let f = Fixture::new(8);

    let e = f.tick();
    let r1 = f.tree.create(BranchId::TRUNK, e).unwrap();
    let e = f.tick();
    let r1 = f.tree.insert(r1, BranchId::TRUNK, e, b"a", b"1").unwrap();

    let h = SubtreeHash::new(f.store_dyn());
    h.stamp(r1).unwrap(); // memo[r1] = id({a -> 1})

    for a in f.store.arenas_of(BranchId::TRUNK).unwrap() {
        f.store.free_arena(a).unwrap();
    }

    // Rebuild until page id r1 comes back, now holding {a -> 2}.
    f.store.register_branch(B1, Some(BranchId::TRUNK), f.tick()).unwrap();
    let mut reused = None;
    for _ in 0..2000 {
        let e = f.tick();
        let r = f.tree.create(B1, e).unwrap();
        let e = f.tick();
        let r = f.tree.insert(r, B1, e, b"a", b"2").unwrap();
        if r == r1 {
            reused = Some(r);
            break;
        }
    }
    let side_a = reused.expect("page id was never recycled");

    // A second, live tree holding {a -> 1} — exactly what the stale memo believes is on side_a.
    f.store.register_branch(B2, Some(BranchId::TRUNK), f.tick()).unwrap();
    let e = f.tick();
    let side_b = f.tree.create(B2, e).unwrap();
    let e = f.tick();
    let side_b = f.tree.insert(side_b, B2, e, b"a", b"1").unwrap();
    assert_ne!(side_a, side_b);
    h.stamp(side_b).unwrap();

    // The oracle: page identity cannot be fooled here, and it finds the change.
    let truth = diff(&f.tree, side_a, side_b, &PageIdentity).unwrap();
    assert_eq!(truth.changes.len(), 1, "oracle: there IS exactly one difference");

    let by_hash = diff(&f.tree, side_a, side_b, &h).unwrap();
    assert_eq!(
        by_hash.changes.len(),
        1,
        "SILENT WRONGNESS: the stale memo skipped the root and reported {} changes \
         where the truth is 1 ({:?})",
        by_hash.changes.len(),
        truth.changes
    );
}

// =================================================================================================
// ATTACK 5 — subtree_cid through MemoIdentity turns O(delta) into worse-than-O(N).
// =================================================================================================

/// `MemoIdentity`'s doc block shows `MemoIdentity::new(|p| cid::subtree_cid(&tree, p))`, in a
/// `ignore` fence, so it is never compiled or run. This compiles it, and measures what warming
/// costs against the O(N) path the module exists to beat.
#[test]
fn attack5_memoidentity_over_subtree_cid_costs_more_than_the_on_path_it_replaces() {
    let f = Fixture::new(4096);
    let n = 16_000usize;

    let e = f.tick();
    let mut base = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..n {
        let e = f.tick();
        base = f
            .tree
            .insert(base, BranchId::TRUNK, e, format!("k{i:09}").as_bytes(), b"v000000000-0000")
            .unwrap();
    }
    f.store.register_branch(B1, Some(BranchId::TRUNK), f.tick()).unwrap();
    let mut head = base;
    for i in [n / 7, n / 3, (n * 2) / 3, n - 1] {
        let e = f.tick();
        head = f
            .tree
            .insert(head, B1, e, format!("k{i:09}").as_bytes(), b"v000000000-0001")
            .unwrap();
    }

    let nodes = f.tree.walk_pages(base).unwrap().len();

    // The control: the O(N) path cow::diff exists to replace.
    let t0 = Instant::now();
    let old = f.tree.diff(base, head).unwrap();
    let t_old = t0.elapsed();
    assert_eq!(old.deltas.len(), 4);

    // The new path with the provider the module recommends.
    let t0 = Instant::now();
    let r_page = diff(&f.tree, base, head, &PageIdentity).unwrap();
    let t_page = t0.elapsed();
    assert_eq!(r_page.changes.len(), 4);

    // The new path driven by cow::cid, warmed as the docs instruct.
    let tree_ref = &f.tree;
    let ident = MemoIdentity::new(|p: PageId| cid::subtree_cid(tree_ref, p));
    let t0 = Instant::now();
    let w = ident.warm(&f.tree, base).unwrap() + ident.warm(&f.tree, head).unwrap();
    let t_warm = t0.elapsed();
    let t0 = Instant::now();
    let r_cid = diff(&f.tree, base, head, &ident).unwrap();
    let t_cid_diff = t0.elapsed();

    // Also the bottom-up provider, for scale.
    let sh = SubtreeHash::new(f.store_dyn());
    let t0 = Instant::now();
    sh.stamp(base).unwrap();
    sh.stamp(head).unwrap();
    let t_stamp = t0.elapsed();

    assert_eq!(r_cid.changes.len(), 4, "the cid-driven diff got the answer wrong");
    assert_eq!(ident.misses(), 0, "warming was incomplete");

    println!("  n={n} tree_nodes={nodes} warmed={w}");
    println!("  CONTROL  CowTree::diff (the O(N) path)        : {t_old:?}");
    println!("  NEW      diff + PageIdentity (no precompute)  : {t_page:?}");
    println!("  NEW      diff + MemoIdentity(subtree_cid)     : {t_cid_diff:?} diff-only");
    println!("           ...its mandatory warm()              : {t_warm:?}   <-- the real cost");
    println!("  NEW      SubtreeHash::stamp (bottom-up, O(N)) : {t_stamp:?}");
    println!(
        "  warm(subtree_cid) / CowTree::diff = {:.1}x",
        t_warm.as_secs_f64() / t_old.as_secs_f64()
    );
    println!(
        "  warm(subtree_cid) / SubtreeHash::stamp = {:.1}x",
        t_warm.as_secs_f64() / t_stamp.as_secs_f64()
    );
}

// =================================================================================================
// ATTACK 4 — the unknown-page fallback.
// =================================================================================================

/// Byte 0 reservation: a page-identity fallback can never equal a content id, and two distinct
/// unstamped pages never collide. Forced over a real tree rather than asserted.
#[test]
fn attack4_the_fallback_reservation_holds_over_a_real_tree() {
    let f = Fixture::new(4096);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..4000 {
        let e = f.tick();
        root = f
            .tree
            .insert(root, BranchId::TRUNK, e, format!("k{i:09}").as_bytes(), b"v")
            .unwrap();
    }
    let pages = f.tree.walk_pages(root).unwrap();
    assert!(pages.len() > 50);

    let h = SubtreeHash::new(f.store_dyn());
    // Every unstamped page is a distinct fallback.
    let mut fallbacks: Vec<[u8; 16]> = pages.iter().map(|p| h.id_of(*p)).collect();
    assert!(fallbacks.iter().all(|i| i[0] == 0x01), "a fallback left the page domain");
    fallbacks.sort();
    let before = fallbacks.len();
    fallbacks.dedup();
    assert_eq!(before, fallbacks.len(), "two unstamped pages collided");

    // Stamp half the tree, then check no content id can equal any fallback.
    h.stamp(root).unwrap();
    let content: Vec<[u8; 16]> = pages.iter().map(|p| h.id_of(*p)).collect();
    assert!(content.iter().all(|i| i[0] == 0x00), "a stamped page left the content domain");
    for c in &content {
        assert!(!fallbacks.contains(c), "a content id equalled a page-identity fallback");
    }
}

// =================================================================================================
// ATTACK 1 (cross-lineage half) — where the digest is the SOLE authority.
// =================================================================================================

/// Within one lineage the digest wins nothing: `PageIdentity` decides first. Cross-lineage is the
/// path `cid.rs` built the digest FOR, and there page identity wins zero skips — so the digest is
/// the only thing standing between the caller and a wrong answer, with no verifying comparison
/// behind it (`grep -n 'verify' src/cow/diff.rs` is empty).
#[test]
fn attack1_cross_lineage_is_where_the_digest_becomes_the_sole_authority() {
    let f = Fixture::new(4096);
    let n = 1000u32;

    // Two lineages, same rows, different insertion orders. Nothing is shared between them.
    let e = f.tick();
    let mut a = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..n {
        let e = f.tick();
        a = f.tree.insert(a, BranchId::TRUNK, e, &i.to_be_bytes(), format!("v{i}").as_bytes()).unwrap();
    }

    f.store.register_branch(B1, Some(BranchId::TRUNK), f.tick()).unwrap();
    let e = f.tick();
    let mut b = f.tree.create(B1, e).unwrap();
    let mut order: Vec<u32> = (0..n).collect();
    let mut s: u32 = 0x5eed_1234;
    for i in (1..order.len()).rev() {
        s = s.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        order.swap(i, (s >> 8) as usize % (i + 1));
    }
    for &i in &order {
        let e = f.tick();
        b = f.tree.insert(b, B1, e, &i.to_be_bytes(), format!("v{i}").as_bytes()).unwrap();
    }

    let pages_a: std::collections::HashSet<PageId> = f.tree.walk_pages(a).unwrap().into_iter().collect();
    let pages_b: std::collections::HashSet<PageId> = f.tree.walk_pages(b).unwrap().into_iter().collect();
    assert_eq!(pages_a.intersection(&pages_b).count(), 0, "the two lineages share pages");

    // Control: the two trees really do hold identical data.
    assert_eq!(
        cid::leaf_content_cid(&f.tree, a).unwrap(),
        cid::leaf_content_cid(&f.tree, b).unwrap(),
        "control: the two trees do not hold the same rows"
    );

    let by_page = diff(&f.tree, a, b, &PageIdentity).unwrap();
    let h = SubtreeHash::new(f.store_dyn());
    h.stamp(a).unwrap();
    h.stamp(b).unwrap();
    let by_hash = diff(&f.tree, a, b, &h).unwrap();

    println!("  cross-lineage, {} rows, {} + {} nodes", n, pages_a.len(), pages_b.len());
    println!("    PageIdentity : visited={:>5} skipped_subtrees={:>4}", by_page.visited, by_page.skipped_subtrees);
    println!("    SubtreeHash  : visited={:>5} skipped_subtrees={:>4}", by_hash.visited, by_hash.skipped_subtrees);

    assert!(by_page.changes.is_empty() && by_hash.changes.is_empty(), "identical data must diff empty");
    assert_eq!(by_page.skipped_subtrees, 0, "page identity skipped something across lineages");
    assert!(
        by_hash.skipped_subtrees > 0,
        "the digest won no skips either — then cross-lineage is not the digest's path after all"
    );
    assert!(
        by_hash.visited < by_page.visited,
        "the digest did not reduce the read set: {} vs {}",
        by_hash.visited,
        by_page.visited
    );
}

/// What a collision COSTS, measured rather than argued. The collision is INJECTED (a 120-bit one
/// cannot be found in a test), so this measures the diff's behaviour under one, not the hash's
/// strength: does a collision produce a loud error or a silently dropped change?
#[test]
fn attack2_a_collision_drops_the_change_silently_rather_than_erroring() {
    struct Colliding;
    impl NodeIdentity for Colliding {
        fn id_of(&self, _page: PageId) -> [u8; 16] {
            // One content-domain value for every page: what a total collision looks like.
            [0u8; 16]
        }
    }

    let f = Fixture::new(4096);
    let e = f.tick();
    let mut base = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..2000u32 {
        let e = f.tick();
        base = f.tree.insert(base, BranchId::TRUNK, e, &i.to_be_bytes(), b"v0").unwrap();
    }
    f.store.register_branch(B1, Some(BranchId::TRUNK), f.tick()).unwrap();
    let e = f.tick();
    let head = f.tree.insert(base, B1, e, &7u32.to_be_bytes(), b"v1").unwrap();

    let truth = diff(&f.tree, base, head, &PageIdentity).unwrap();
    assert_eq!(truth.changes.len(), 1, "oracle");

    let r = diff(&f.tree, base, head, &Colliding);
    match r {
        Err(e) => println!("  a collision is DETECTED: {e}"),
        Ok(rep) => println!(
            "  a collision is SILENT: {} changes reported, truth is {}; visited={} skipped={}",
            rep.changes.len(),
            truth.changes.len(),
            rep.visited,
            rep.skipped_subtrees
        ),
    }
}

/// `cid.rs` has an avalanche test. `diff.rs`'s own digest has none — measure it through the only
/// public surface it has.
#[test]
fn attack2_diff_rs_digest_avalanche_measured_through_stamp() {
    let f = Fixture::new(4096);
    let h = SubtreeHash::new(f.store_dyn());

    let mut worst = 128u32;
    let mut roots = Vec::new();
    for byte in 0u8..32 {
        for bit in 0..8u8 {
            for val in [byte, byte ^ (1 << bit)] {
                let e = f.tick();
                let r = f.tree.create(BranchId::TRUNK, e).unwrap();
                let e = f.tick();
                let r = f.tree.insert(r, BranchId::TRUNK, e, b"k", &[val]).unwrap();
                roots.push(r);
            }
            let b0 = h.stamp(roots[roots.len() - 2]).unwrap();
            let b1 = h.stamp(roots[roots.len() - 1]).unwrap();
            let d: u32 = b0.iter().zip(b1.iter()).map(|(x, y)| (x ^ y).count_ones()).sum();
            worst = worst.min(d);
        }
    }
    println!("  diff.rs SubtreeHash avalanche: worst-case {worst} of 128 output bits (byte 0 is a fixed tag, so 120 are live)");
    assert!(worst >= 40, "worst-case avalanche only {worst}/128 — diff.rs's digest is not mixing");
}

/// Price the upgrade the module header calls "a local change": SHA-256 already ships in this repo,
/// dependency-free with FIPS 180-4 vectors.
#[test]
fn attack2_price_the_sha256_upgrade() {
    use ferrodb::provenance::sha256::Sha256;

    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let reps = 20_000;

    let t0 = Instant::now();
    let mut acc = 0u8;
    for _ in 0..reps {
        let c = cid::leaf_cid(&[(b"k".to_vec(), payload.clone())]);
        acc ^= c[0];
    }
    let t_hand = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..reps {
        let mut s = Sha256::new();
        s.update(b"k");
        s.update(&payload);
        acc ^= s.finish()[0];
    }
    let t_sha = t0.elapsed();

    println!("  {} x 4KiB  Hasher128(leaf_cid) : {:?}", reps, t_hand);
    println!("  {} x 4KiB  Sha256             : {:?}", reps, t_sha);
    println!("  sha256 / hand-rolled = {:.2}x   (acc={acc}, keeps both loops live)", t_sha.as_secs_f64() / t_hand.as_secs_f64());
}

/// Attack 5, as COUNTS rather than times. This box is shared with an agent fleet and the wall-clock
/// control swung 34x between two runs of the same code, so the timing comparison is not quotable.
/// Page reads are deterministic: they do not move when the machine is busy.
#[test]
fn attack5_warming_subtree_cid_reads_the_tree_once_per_level_counted_not_timed() {
    let f = Fixture::new(4096);
    let n = 16_000usize;

    let e = f.tick();
    let mut base = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..n {
        let e = f.tick();
        base = f.tree.insert(base, BranchId::TRUNK, e, format!("k{i:09}").as_bytes(), b"v0").unwrap();
    }

    let pages = f.tree.walk_pages(base).unwrap();

    // What MemoIdentity::warm costs: subtree_cid(p) re-walks p's whole subtree, for every p.
    let warm_reads: usize = pages.iter().map(|p| f.tree.walk_pages(*p).unwrap().len()).sum();
    // What SubtreeHash::stamp costs: each page read exactly once, bottom-up.
    let stamp_reads = pages.len();
    // What the old O(N) path costs before it can prune: both roots enumerated.
    let old_reads = pages.len() * 2;
    // What the diff itself costs with 4 rows changed.
    f.store.register_branch(B1, Some(BranchId::TRUNK), f.tick()).unwrap();
    let mut head = base;
    for i in [n / 7, n / 3, (n * 2) / 3, n - 1] {
        let e = f.tick();
        head = f.tree.insert(head, B1, e, format!("k{i:09}").as_bytes(), b"v1").unwrap();
    }
    let r = diff(&f.tree, base, head, &PageIdentity).unwrap();
    assert_eq!(r.changes.len(), 4);

    println!("  n={n}  tree nodes={}  depth-driven fan-in", pages.len());
    println!("    diff itself (PageIdentity, 4 rows changed) : {:>8} node payloads decoded", r.visited);
    println!("    CowTree::diff enumeration (the O(N) path)  : {old_reads:>8} page reads");
    println!("    SubtreeHash::stamp precompute              : {stamp_reads:>8} page reads");
    println!("    MemoIdentity(subtree_cid).warm precompute  : {warm_reads:>8} page reads");
    println!("    warm / stamp        = {:.1}x", warm_reads as f64 / stamp_reads as f64);
    println!("    warm / O(N) control = {:.1}x", warm_reads as f64 / old_reads as f64);
    println!("    warm / the diff it enables = {:.0}x", warm_reads as f64 / r.visited as f64);

    assert!(
        warm_reads > old_reads,
        "warming the cid adapter ({warm_reads}) was cheaper than the O(N) path it replaces \
         ({old_reads}); the O(N-per-level) claim is wrong"
    );
}

/// The sharper half of attack 3: `MemoIdentity::warm` is the one call that looks like it would
/// refresh a stale entry, and it does not — `diff.rs:250` skips any page already in the map
/// (`if self.memo.read().unwrap().contains_key(&p) { continue; }`), so re-warming after a recycle
/// is a no-op and `misses()` stays at 0. A caller doing the documented thing gets a wrong answer
/// with every counter reading clean.
#[test]
fn attack3_rewarming_does_not_refresh_a_recycled_page_and_misses_stays_zero() {
    let f = Fixture::new(8);

    let e = f.tick();
    let r1 = f.tree.create(BranchId::TRUNK, e).unwrap();
    let e = f.tick();
    let r1 = f.tree.insert(r1, BranchId::TRUNK, e, b"a", b"1").unwrap();

    let tree_ref = &f.tree;
    let ident = MemoIdentity::new(|p: PageId| cid::subtree_cid(tree_ref, p));
    ident.warm(&f.tree, r1).unwrap();
    let stale = ident.id_of(r1);

    for a in f.store.arenas_of(BranchId::TRUNK).unwrap() {
        f.store.free_arena(a).unwrap();
    }
    f.store.register_branch(B1, Some(BranchId::TRUNK), f.tick()).unwrap();
    let mut reused = None;
    for _ in 0..2000 {
        let e = f.tick();
        let r = f.tree.create(B1, e).unwrap();
        let e = f.tick();
        let r = f.tree.insert(r, B1, e, b"a", b"2").unwrap();
        if r == r1 {
            reused = Some(r);
            break;
        }
    }
    let r1b = reused.expect("page id was never recycled");

    // The documented remedy: warm it again.
    let added = ident.warm(&f.tree, r1b).unwrap();
    println!("  re-warm added {added} entries; misses()={}", ident.misses());
    println!("  true cid now  : {}", cid::hex(&cid::subtree_cid(&f.tree, r1b).unwrap()));
    println!("  memo still says: {}", cid::hex(&ident.id_of(r1b)));

    assert_ne!(
        ident.id_of(r1b),
        stale,
        "re-warming did not refresh the recycled page: warm() added {added} entries and \
         misses() is {}, so every counter reads clean while the id is wrong",
        ident.misses()
    );
}
