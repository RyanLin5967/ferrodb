//! THROWAWAY falsification harness for D89 (`src/cow/cid.rs`).
//!
//! Not part of the suite, not a deliverable. It attacks the central measurement of
//! `cow::cid::tests::same_data_inserted_in_two_orders_yields_different_partition_cids`:
//!
//!   1000 identical pairs inserted ascending vs shuffled  ->  equal leaf_content_cid,
//!   different leaf_partition_cid, ZERO leaves with matching cids.
//!
//! Four attacks: A fixture sweep, B control forcing, C physical independence,
//! D collision hunting.

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, Epoch, LeaseDeadline, PageId};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::cid::{
    hex, leaf_cid, leaf_content_cid, leaf_partition_cid, ordered_leaf_cids, subtree_cid, Cid,
    Hasher128,
};
use ferrodb::cow::node::NodeMut;
use ferrodb::cow::page_header::{stamp_checksum, PageType};
use ferrodb::cow::{CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;
use std::collections::HashMap;
use std::sync::Arc;

// ---- fixture -----------------------------------------------------------------------------------

struct Fx {
    _dir: tempfile::TempDir,
    cat: Arc<LogBranchCatalog>,
    t: CowTree,
}

fn fixture(arena_base: u32) -> Fx {
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
    let cat = Arc::new(LogBranchCatalog::in_memory(1));
    let store = Arc::new(
        ArenaPageStore::new(pool, Arc::clone(&cat) as Arc<dyn BranchCatalog>, arena_base).unwrap(),
    );
    let t = CowTree::new(store as Arc<dyn PageStore>);
    Fx { _dir: dir, cat, t }
}

fn k(n: u32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Value of exactly `w` bytes whose content is a function of `n` (so values stay distinct).
/// `w == 0` means "the fixture's own value shape", `format!("v{n}")`.
fn v(n: u32, w: usize) -> Vec<u8> {
    if w == 0 {
        return format!("v{n}").into_bytes();
    }
    let mut out = n.to_be_bytes().to_vec();
    while out.len() < w {
        out.push(b'a' + ((n as u8).wrapping_add(out.len() as u8) % 26));
    }
    out.truncate(w.max(4));
    out
}

fn build_in(fx: &Fx, keys: &[u32], w: usize) -> PageId {
    let e = fx.cat.next_epoch();
    let mut root = fx.t.create(BranchId::TRUNK, e).unwrap();
    for &i in keys {
        root = fx.t.insert(root, BranchId::TRUNK, e, &k(i), &v(i, w)).unwrap();
    }
    root
}

// ---- orders ------------------------------------------------------------------------------------

/// The module's own shuffle, parameterised by seed. seed 0x5eed_1234 reproduces the test exactly.
fn fixed_shuffle_seed(n: u32, seed: u32) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n).collect();
    let mut state: u32 = seed;
    for i in (1..v.len()).rev() {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let j = (state >> 8) as usize % (i + 1);
        v.swap(i, j);
    }
    v
}

/// Ascending with `swaps` adjacent transpositions at pseudo-random positions.
fn near_sorted(n: u32, swaps: usize, seed: u32) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n).collect();
    if v.len() < 2 {
        return v;
    }
    let mut state: u32 = seed;
    for _ in 0..swaps {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let i = (state >> 8) as usize % (v.len() - 1);
        v.swap(i, i + 1);
    }
    v
}

/// Ascending inside blocks of `b`, blocks visited back to front.
fn blocks_reversed(n: u32, b: u32) -> Vec<u32> {
    let mut out = Vec::with_capacity(n as usize);
    let nblocks = n.div_ceil(b);
    for blk in (0..nblocks).rev() {
        for i in blk * b..((blk + 1) * b).min(n) {
            out.push(i);
        }
    }
    out
}

fn evens_then_odds(n: u32) -> Vec<u32> {
    let mut out: Vec<u32> = (0..n).filter(|x| x % 2 == 0).collect();
    out.extend((0..n).filter(|x| x % 2 == 1));
    out
}

// ---- comparison --------------------------------------------------------------------------------

/// The test's own "shared" statistic: positions in `a` whose cid appears anywhere in `b`.
fn shared_as_test_counts(a: &[Cid], b: &[Cid]) -> usize {
    a.iter().filter(|c| b.contains(c)).count()
}

/// What a real content diff would actually skip: length of the longest common subsequence.
fn lcs(a: &[Cid], b: &[Cid]) -> usize {
    let mut prev = vec![0usize; b.len() + 1];
    let mut cur = vec![0usize; b.len() + 1];
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            cur[j] = if a[i - 1] == b[j - 1] {
                prev[j - 1] + 1
            } else {
                prev[j].max(cur[j - 1])
            };
        }
        std::mem::swap(&mut prev, &mut cur);
        cur.iter_mut().for_each(|x| *x = 0);
    }
    prev[b.len()]
}

struct Row {
    n: u32,
    w: usize,
    order: String,
    la: usize,
    lb: usize,
    shared: usize,
    lcs: usize,
    part_eq: bool,
    cont_eq: bool,
}

fn compare(fx: &Fx, n: u32, w: usize, order_name: &str, order: &[u32]) -> Row {
    let asc: Vec<u32> = (0..n).collect();
    let ra = build_in(fx, &asc, w);
    let rb = build_in(fx, order, w);
    let a = ordered_leaf_cids(&fx.t, ra).unwrap();
    let b = ordered_leaf_cids(&fx.t, rb).unwrap();
    Row {
        n,
        w,
        order: order_name.to_string(),
        la: a.len(),
        lb: b.len(),
        shared: shared_as_test_counts(&a, &b),
        lcs: lcs(&a, &b),
        part_eq: leaf_partition_cid(&fx.t, ra).unwrap() == leaf_partition_cid(&fx.t, rb).unwrap(),
        cont_eq: leaf_content_cid(&fx.t, ra).unwrap() == leaf_content_cid(&fx.t, rb).unwrap(),
    }
}

// ---- A: is "0 of 9/8" a fixture artifact? -------------------------------------------------------

fn attack_a() {
    println!("\n================ ATTACK A: fixture sweep ================");
    println!("ascending vs <order>. shared = the test's own statistic. lcs = what a content diff");
    println!("could really skip. part/cont = partition/content cids equal?\n");
    println!(
        "{:>6} {:>5} {:<16} {:>5} {:>5} {:>7} {:>5} {:>6} {:>6}",
        "n", "vlen", "order", "lvsA", "lvsB", "shared", "lcs", "partEq", "contEq"
    );

    let sizes: Vec<u32> = vec![2, 3, 4, 5, 8, 12, 16, 24, 32, 48, 64, 100, 128, 200, 256, 500, 1000, 2000, 5000];
    let widths: Vec<usize> = vec![0, 32, 200, 512, 990];
    let mut rows: Vec<Row> = Vec::new();

    for &w in &widths {
        let fx = fixture(1024);
        for &n in &sizes {
            // Big trees at big values get slow; cap the product.
            if (n as usize) * w.max(6) > 3_000_000 {
                continue;
            }
            let mut orders: Vec<(String, Vec<u32>)> = vec![
                ("shuffle-0x5eed1234".into(), fixed_shuffle_seed(n, 0x5eed_1234)),
                ("shuffle-seed2".into(), fixed_shuffle_seed(n, 0x1234_5678)),
                ("shuffle-seed3".into(), fixed_shuffle_seed(n, 0xdead_beef)),
                ("descending".into(), (0..n).rev().collect()),
                ("evens-then-odds".into(), evens_then_odds(n)),
                ("near-1swap".into(), near_sorted(n, 1, 7)),
                ("near-2swaps".into(), near_sorted(n, 2, 11)),
                ("near-5swaps".into(), near_sorted(n, 5, 13)),
                ("near-20swaps".into(), near_sorted(n, 20, 17)),
                ("blocks-rev-8".into(), blocks_reversed(n, 8)),
                ("blocks-rev-64".into(), blocks_reversed(n, 64)),
            ];
            orders.retain(|(_, o)| o.len() == n as usize);
            for (name, o) in &orders {
                let r = compare(&fx, n, w, name, o);
                println!(
                    "{:>6} {:>5} {:<16} {:>5} {:>5} {:>7} {:>5} {:>6} {:>6}",
                    r.n,
                    if r.w == 0 { 6 } else { r.w },
                    r.order,
                    r.la,
                    r.lb,
                    r.shared,
                    r.lcs,
                    r.part_eq,
                    r.cont_eq
                );
                rows.push(r);
            }
        }
    }

    // Reduce.
    let total = rows.len();
    let multileaf: Vec<&Row> = rows.iter().filter(|r| r.la > 1 && r.lb > 1).collect();
    let any_shared: Vec<&&Row> = multileaf.iter().filter(|r| r.shared > 0).collect();
    let part_equal: Vec<&Row> = rows.iter().filter(|r| r.part_eq).collect();
    let cont_unequal: Vec<&Row> = rows.iter().filter(|r| !r.cont_eq).collect();
    let violates_guard: Vec<&&Row> =
        multileaf.iter().filter(|r| r.shared * 2 >= r.la).collect();

    println!("\n--- A reduce ---");
    println!("configurations run                        : {total}");
    println!("configurations with >1 leaf on both sides : {}", multileaf.len());
    println!("  of those, shared > 0                    : {}", any_shared.len());
    println!("  of those, shared*2 >= leavesA (the test's assert would FAIL) : {}", violates_guard.len());
    println!("configurations where partition cids AGREE : {}", part_equal.len());
    println!("configurations where content cids DIFFER  : {} (must be 0)", cont_unequal.len());

    println!("\nevery multi-leaf configuration with any sharing at all:");
    if any_shared.is_empty() {
        println!("  (none)");
    }
    for r in &any_shared {
        println!(
            "  n={:<5} vlen={:<4} {:<18} leaves {}/{} shared={} lcs={} partEq={}",
            r.n,
            if r.w == 0 { 6 } else { r.w },
            r.order,
            r.la,
            r.lb,
            r.shared,
            r.lcs,
            r.part_eq
        );
    }
    println!("\nevery configuration where the partition cids AGREE:");
    if part_equal.is_empty() {
        println!("  (none)");
    }
    for r in &part_equal {
        println!(
            "  n={:<5} vlen={:<4} {:<18} leaves {}/{}",
            r.n,
            if r.w == 0 { 6 } else { r.w },
            r.order,
            r.la,
            r.lb
        );
    }
}

/// Fix n=1000/vlen=6 (the test's own fixture) and sweep 200 distinct shuffles, to see whether the
/// published "0 of 9/8" is a property of the family or of the one seed.
fn attack_a_seed_sweep() {
    println!("\n---- A2: n=1000, the test's own value shape, 200 distinct shuffle seeds ----");
    let fx = fixture(1024);
    let asc: Vec<u32> = (0..1000).collect();
    let ra = build_in(&fx, &asc, 0);
    let a = ordered_leaf_cids(&fx.t, ra).unwrap();
    let apart = leaf_partition_cid(&fx.t, ra).unwrap();
    let acont = leaf_content_cid(&fx.t, ra).unwrap();

    let mut leafcount: HashMap<usize, usize> = HashMap::new();
    let mut max_shared = 0usize;
    let mut max_lcs = 0usize;
    let mut part_agree = 0usize;
    let mut cont_disagree = 0usize;
    for s in 0..200u32 {
        let seed = 0x5eed_1234u32.wrapping_add(s.wrapping_mul(0x9e37_79b9));
        let order = fixed_shuffle_seed(1000, seed);
        let rb = build_in(&fx, &order, 0);
        let b = ordered_leaf_cids(&fx.t, rb).unwrap();
        *leafcount.entry(b.len()).or_default() += 1;
        max_shared = max_shared.max(shared_as_test_counts(&a, &b));
        max_lcs = max_lcs.max(lcs(&a, &b));
        if leaf_partition_cid(&fx.t, rb).unwrap() == apart {
            part_agree += 1;
        }
        if leaf_content_cid(&fx.t, rb).unwrap() != acont {
            cont_disagree += 1;
        }
    }
    let mut counts: Vec<_> = leafcount.into_iter().collect();
    counts.sort();
    println!("  ascending tree: {} leaves, partition cid {}", a.len(), hex(&apart));
    println!("  shuffled leaf-count distribution over 200 seeds: {counts:?}");
    println!("  max shared leaves over 200 seeds : {max_shared}");
    println!("  max LCS over 200 seeds           : {max_lcs}");
    println!("  seeds whose partition cid equals ascending's : {part_agree} of 200");
    println!("  seeds whose content cid DIFFERS from ascending's : {cont_disagree} of 200 (must be 0)");
}

/// Shuffle vs shuffle, not ascending vs shuffle: two "independent lineages" neither of which is
/// sorted. The test only measured one of the two axes.
fn attack_a_shuffle_vs_shuffle() {
    println!("\n---- A3: shuffle vs shuffle (two unsorted lineages), n=1000 ----");
    let fx = fixture(1024);
    let mut worst = (0usize, 0usize);
    for s in 0..40u32 {
        let o1 = fixed_shuffle_seed(1000, 0x1111_0000u32.wrapping_add(s));
        let o2 = fixed_shuffle_seed(1000, 0x2222_0000u32.wrapping_add(s));
        let r1 = build_in(&fx, &o1, 0);
        let r2 = build_in(&fx, &o2, 0);
        let a = ordered_leaf_cids(&fx.t, r1).unwrap();
        let b = ordered_leaf_cids(&fx.t, r2).unwrap();
        let sh = shared_as_test_counts(&a, &b);
        let l = lcs(&a, &b);
        if sh > worst.0 {
            worst = (sh, l);
        }
        assert_eq!(
            leaf_content_cid(&fx.t, r1).unwrap(),
            leaf_content_cid(&fx.t, r2).unwrap(),
            "control broke in A3"
        );
    }
    println!("  40 shuffle-vs-shuffle pairs: max shared = {}, its lcs = {}", worst.0, worst.1);
}

/// The insertion order the test never tried: the SAME order twice but with the rows arriving as
/// two lineages that then converge. Concretely: does a *single-key difference* in the insert
/// order destroy all sharing, or only local sharing?
fn attack_a_one_key_late() {
    println!("\n---- A4: ascending vs ascending-with-ONE-key-inserted-last, n=1000 ----");
    let fx = fixture(1024);
    let asc: Vec<u32> = (0..1000).collect();
    let ra = build_in(&fx, &asc, 0);
    let a = ordered_leaf_cids(&fx.t, ra).unwrap();
    for moved in [0u32, 1, 137, 500, 998, 999] {
        let mut order: Vec<u32> = (0..1000).filter(|&x| x != moved).collect();
        order.push(moved);
        let rb = build_in(&fx, &order, 0);
        let b = ordered_leaf_cids(&fx.t, rb).unwrap();
        println!(
            "  key {moved} inserted last: leaves {}/{}  shared={}  lcs={}  partEq={}  contEq={}",
            a.len(),
            b.len(),
            shared_as_test_counts(&a, &b),
            lcs(&a, &b),
            leaf_partition_cid(&fx.t, ra).unwrap() == leaf_partition_cid(&fx.t, rb).unwrap(),
            leaf_content_cid(&fx.t, ra).unwrap() == leaf_content_cid(&fx.t, rb).unwrap(),
        );
    }
}

// ---- B: is leaf_content_cid a real control? -----------------------------------------------------

fn attack_b() {
    println!("\n================ ATTACK B: force the control to fire ================");
    let fx = fixture(1024);

    let mut fails = 0usize;
    let mut check = |name: &str, x: Cid, y: Cid, want_equal: bool| {
        let eq = x == y;
        let ok = eq == want_equal;
        if !ok {
            fails += 1;
        }
        println!(
            "  [{}] {:<52} equal={} (wanted {})  {} / {}",
            if ok { "ok  " } else { "FAIL" },
            name,
            eq,
            want_equal,
            hex(&x),
            hex(&y)
        );
    };

    // raw-bytes builder so keys and values can be chosen freely
    let raw = |pairs: &[(&[u8], &[u8])]| -> PageId {
        let e = fx.cat.next_epoch();
        let mut root = fx.t.create(BranchId::TRUNK, e).unwrap();
        for (kk, vv) in pairs {
            root = fx.t.insert(root, BranchId::TRUNK, e, kk, vv).unwrap();
        }
        root
    };

    // B1: one value differs by one byte in a 1000-key tree.
    let base = build_in(&fx, &(0..1000).collect::<Vec<u32>>(), 0);
    let e = fx.cat.next_epoch();
    let b = fx.cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap().branch_id;
    let edited = fx.t.insert(base, b, e, &k(500), b"v500x").unwrap();
    check(
        "B1 1000-key tree, one value +1 byte",
        leaf_content_cid(&fx.t, base).unwrap(),
        leaf_content_cid(&fx.t, edited).unwrap(),
        false,
    );

    // B2: one key present vs absent (999 vs 1000 keys).
    let short = build_in(&fx, &(0..999).collect::<Vec<u32>>(), 0);
    check(
        "B2 1000 keys vs 999 keys",
        leaf_content_cid(&fx.t, base).unwrap(),
        leaf_content_cid(&fx.t, short).unwrap(),
        false,
    );

    // B3: the field-boundary hazard, at TREE level (not just leaf_cid level).
    let t1 = raw(&[(b"ab", b"c")]);
    let t2 = raw(&[(b"a", b"bc")]);
    check(
        "B3 tree{(ab,c)} vs tree{(a,bc)} (same byte stream)",
        leaf_content_cid(&fx.t, t1).unwrap(),
        leaf_content_cid(&fx.t, t2).unwrap(),
        false,
    );

    // B4: two keys whose (key,value) bytes are a rotation of each other.
    let t3 = raw(&[(b"k1", b"AAA"), (b"k2", b"BBB")]);
    let t4 = raw(&[(b"k1", b"BBB"), (b"k2", b"AAA")]);
    check(
        "B4 values swapped between two keys",
        leaf_content_cid(&fx.t, t3).unwrap(),
        leaf_content_cid(&fx.t, t4).unwrap(),
        false,
    );

    // B5: a key with an empty value vs a key with a one-zero-byte value.
    let t5 = raw(&[(b"k", b"")]);
    let t6 = raw(&[(b"k", b"\0")]);
    check(
        "B5 empty value vs one zero byte",
        leaf_content_cid(&fx.t, t5).unwrap(),
        leaf_content_cid(&fx.t, t6).unwrap(),
        false,
    );

    // B6: an EMPTY tree vs a one-entry tree, and empty vs empty.
    let e1 = fx.cat.next_epoch();
    let empty_a = fx.t.create(BranchId::TRUNK, e1).unwrap();
    let empty_b = fx.t.create(BranchId::TRUNK, e1).unwrap();
    check(
        "B6a empty tree vs empty tree (must AGREE)",
        leaf_content_cid(&fx.t, empty_a).unwrap(),
        leaf_content_cid(&fx.t, empty_b).unwrap(),
        true,
    );
    check(
        "B6b empty tree vs one-entry tree",
        leaf_content_cid(&fx.t, empty_a).unwrap(),
        leaf_content_cid(&fx.t, t5).unwrap(),
        false,
    );

    // B7: is the control CONSTANT? Distinct content cids over the sweep above.
    let mut seen: HashMap<Cid, u32> = HashMap::new();
    for n in [1u32, 2, 3, 10, 50, 200, 999, 1000] {
        let r = build_in(&fx, &(0..n).collect::<Vec<u32>>(), 0);
        *seen.entry(leaf_content_cid(&fx.t, r).unwrap()).or_default() += 1;
    }
    println!(
        "  [{}] B7 8 different key sets -> {} distinct content cids (wanted 8)",
        if seen.len() == 8 { "ok  " } else { "FAIL" },
        seen.len()
    );
    if seen.len() != 8 {
        fails += 1;
    }

    // B8: HOW MANY of those trees would the control have to be wrong about? Build 3000 distinct
    // random content sets and count content-cid collisions.
    let mut cids: HashMap<Cid, usize> = HashMap::new();
    let mut state: u32 = 0xabcd_1234;
    for i in 0..3000u32 {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let n = 1 + (state >> 9) % 40;
        let mut keys: Vec<u32> = Vec::new();
        for j in 0..n {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            keys.push((state >> 7) % 10_000 + j * 10_000);
        }
        keys.sort_unstable();
        keys.dedup();
        let r = build_in(&fx, &keys, 0);
        *cids.entry(leaf_content_cid(&fx.t, r).unwrap()).or_default() += 1;
        let _ = i;
    }
    let dup: usize = cids.values().filter(|c| **c > 1).count();
    println!("  [{}] B8 3000 random content sets -> {} distinct content cids, {} colliding buckets",
        if dup == 0 { "ok  " } else { "FAIL" }, cids.len(), dup);

    println!("  B verdict: {} failures", fails);
}

// ---- C: does anything physical leak in? ---------------------------------------------------------

fn attack_c() {
    println!("\n================ ATTACK C: physical independence ================");
    let keys: Vec<u32> = (0..1000).collect();

    // Reference: arena base 1024, TRUNK, first epoch, fresh file.
    let f1 = fixture(1024);
    let r1 = build_in(&f1, &keys, 0);
    let ref_part = leaf_partition_cid(&f1.t, r1).unwrap();
    let ref_cont = leaf_content_cid(&f1.t, r1).unwrap();
    let ref_sub = subtree_cid(&f1.t, r1).unwrap();
    let pages1 = f1.t.walk_pages(r1).unwrap();
    println!(
        "  reference: root={} pages {}..{} ({} pages)  part={} sub={}",
        r1,
        pages1.iter().min().unwrap(),
        pages1.iter().max().unwrap(),
        pages1.len(),
        hex(&ref_part),
        hex(&ref_sub)
    );

    let mut fails = 0usize;
    let mut report = |name: &str, t: &CowTree, root: PageId| {
        let p = leaf_partition_cid(t, root).unwrap();
        let c = leaf_content_cid(t, root).unwrap();
        let s = subtree_cid(t, root).unwrap();
        let pages = t.walk_pages(root).unwrap();
        let disjoint = pages.iter().all(|x| !pages1.contains(x));
        let ok = p == ref_part && c == ref_cont && s == ref_sub;
        if !ok {
            fails += 1;
        }
        println!(
            "  [{}] {:<44} root={:<7} pages {:>6}..{:<6} disjoint-from-ref={}  part={} sub={}",
            if ok { "ok  " } else { "FAIL" },
            name,
            root,
            pages.iter().min().unwrap(),
            pages.iter().max().unwrap(),
            disjoint,
            hex(&p),
            hex(&s)
        );
    };

    // C1: second build in the same store -> different page ids, same arena.
    let r2 = build_in(&f1, &keys, 0);
    report("C1 same store, second build", &f1.t, r2);

    // C2: a completely different file, different arena base -> page ids in a different range.
    let f2 = fixture(65_536);
    let r3 = build_in(&f2, &keys, 0);
    report("C2 different file, arena base 65536", &f2.t, r3);

    // C3: a different branch (fork) and a much later epoch.
    let f3 = fixture(1024);
    for _ in 0..50 {
        f3.cat.next_epoch();
    }
    let br = f3.cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap().branch_id;
    let e = f3.cat.next_epoch();
    let mut root = f3.t.create(br, e).unwrap();
    for &i in &keys {
        root = f3.t.insert(root, br, e, &k(i), &v(i, 0)).unwrap();
    }
    report("C3 forked branch, epoch+50, own arena", &f3.t, root);

    // C4: page ids shifted by allocating 300 unrelated pages first.
    let f4 = fixture(1024);
    let e4 = f4.cat.next_epoch();
    for _ in 0..300 {
        f4.t.store().alloc_for(BranchId::TRUNK, PageType::Heap, e4).unwrap();
    }
    let r5 = build_in(&f4, &keys, 0);
    report("C4 300 heap pages allocated first", &f4.t, r5);

    // C5: every insert preceded by a throwaway insert+delete in a disjoint keyspace, so each leaf
    //     carries freed cells and a different heap_end. Same logical content.
    let f5 = fixture(1024);
    let e5 = f5.cat.next_epoch();
    let mut r6 = f5.t.create(BranchId::TRUNK, e5).unwrap();
    for &i in &keys {
        r6 = f5.t.insert(r6, BranchId::TRUNK, e5, &k(i), &v(i, 0)).unwrap();
    }
    // now churn: insert then delete a key inside each existing leaf's range
    for &i in &keys {
        let junk = k(i).iter().copied().chain([0xffu8]).collect::<Vec<u8>>();
        r6 = f5.t.insert(r6, BranchId::TRUNK, e5, &junk, b"junkjunkjunk").unwrap();
        r6 = f5.t.delete(r6, BranchId::TRUNK, e5, &junk).unwrap();
    }
    let p6 = leaf_partition_cid(&f5.t, r6).unwrap();
    let c6 = leaf_content_cid(&f5.t, r6).unwrap();
    println!(
        "  [{}] C5 1000 insert+delete churn cycles          leaves={} contentEq={} partitionEq={}",
        if c6 == ref_cont { "ok  " } else { "FAIL" },
        ordered_leaf_cids(&f5.t, r6).unwrap().len(),
        c6 == ref_cont,
        p6 == ref_part
    );
    if c6 != ref_cont {
        fails += 1;
    }

    // C6: single-leaf tree, entries inserted in 6 different orders -> identical physical cell
    //     offsets are impossible, identical leaf_cid is demanded.
    let f6 = fixture(1024);
    let small: Vec<u32> = (0..20).collect();
    let mut cids = Vec::new();
    for seed in [1u32, 2, 3, 4, 5, 6] {
        let order = fixed_shuffle_seed(20, seed);
        let r = build_in(&f6, &order, 0);
        let l = ordered_leaf_cids(&f6.t, r).unwrap();
        assert_eq!(l.len(), 1, "C6 fixture split; needs to stay one leaf");
        cids.push(l[0]);
    }
    let r_asc = build_in(&f6, &small, 0);
    let asc_leaf = ordered_leaf_cids(&f6.t, r_asc).unwrap()[0];
    let all_same = cids.iter().all(|c| *c == asc_leaf);
    println!(
        "  [{}] C6 one leaf, 20 entries, 7 insert orders   all leaf_cids equal={}  cid={}",
        if all_same { "ok  " } else { "FAIL" },
        all_same,
        hex(&asc_leaf)
    );
    if !all_same {
        fails += 1;
    }

    println!("  C verdict: {} failures", fails);
}

// ---- D: collisions ------------------------------------------------------------------------------

fn attack_d() {
    println!("\n================ ATTACK D: two different trees, one partition cid ================");

    // D1: STRUCTURAL. leaf_partition_cid is a function of the leaf sequence ONLY. Build two trees
    //     over the SAME leaves but with different internal shape and different separator keys.
    //     No brute force: the collision is by construction.
    let fx = fixture(1024);
    let e = fx.cat.next_epoch();

    // three real leaves, produced by a real tree so they are well-formed
    let donor = build_in(&fx, &(0..600).collect::<Vec<u32>>(), 0);
    let donor_leaf_pages: Vec<PageId> = {
        // walk_pages returns every page; pick the leaves by asking the store for their type
        let mut leaves = Vec::new();
        for p in fx.t.walk_pages(donor).unwrap() {
            let h = fx.t.store().read_page(p).unwrap();
            let f = h.read();
            if ferrodb::cow::page_header::PageHeader::read_from(&f.data).unwrap().page_type
                == PageType::BTreeLeaf
            {
                leaves.push(p);
            }
        }
        leaves.sort_unstable();
        leaves
    };
    println!("  donor tree has {} leaf pages", donor_leaf_pages.len());

    let mk_internal = |leftmost: PageId, entries: &[(Vec<u8>, PageId)]| -> PageId {
        let id = fx.t.store().alloc_for(BranchId::TRUNK, PageType::BTreeInternal, e).unwrap();
        let h = fx.t.store().read_page(id).unwrap();
        let mut f = h.write();
        NodeMut::new(&mut f.data).fill_internal(leftmost, entries).unwrap();
        stamp_checksum(&mut f.data);
        drop(f);
        id
    };

    // take the first three leaves of the donor, in key order
    let l0 = donor_leaf_pages[0];
    let l1 = donor_leaf_pages[1];
    let l2 = donor_leaf_pages[2];

    // shape 1: one internal root with three children, separators "S1"/"S2"
    let flat = mk_internal(l0, &[(b"S1".to_vec(), l1), (b"S2".to_vec(), l2)]);
    // shape 2: two levels, and completely different separator bytes
    let inner = mk_internal(l0, &[(b"ZZZZZZZZ".to_vec(), l1)]);
    let deep = mk_internal(inner, &[(b"Q".to_vec(), l2)]);

    let p_flat = leaf_partition_cid(&fx.t, flat).unwrap();
    let p_deep = leaf_partition_cid(&fx.t, deep).unwrap();
    let s_flat = subtree_cid(&fx.t, flat).unwrap();
    let s_deep = subtree_cid(&fx.t, deep).unwrap();
    let c_flat = leaf_content_cid(&fx.t, flat).unwrap();
    let c_deep = leaf_content_cid(&fx.t, deep).unwrap();
    println!("  shape A (1 internal, 3 children) : part={} sub={}", hex(&p_flat), hex(&s_flat));
    println!("  shape B (2 levels, other seps)   : part={} sub={}", hex(&p_deep), hex(&s_deep));
    println!(
        "  -> partition cids equal: {}   content cids equal: {}   subtree cids equal: {}",
        p_flat == p_deep,
        c_flat == c_deep,
        s_flat == s_deep
    );

    // D2: BIRTHDAY. Does leaf_cid behave like a 128-bit random function on distinct inputs?
    //     2^21 distinct leaves; expect 0 full collisions and ~birthday many on truncations.
    let n: u64 = 1 << 21;
    let mut full: HashMap<Cid, u32> = HashMap::with_capacity(n as usize);
    let mut low32: HashMap<u32, u32> = HashMap::with_capacity(1 << 20);
    let mut low48: HashMap<u64, u32> = HashMap::with_capacity(1 << 21);
    for i in 0..n {
        let c = leaf_cid(&[(i.to_be_bytes().to_vec(), b"x".to_vec())]);
        *full.entry(c).or_default() += 1;
        let a32 = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
        *low32.entry(a32).or_default() += 1;
        let a48 = u64::from_be_bytes([0, 0, c[0], c[1], c[2], c[3], c[4], c[5]]);
        *low48.entry(a48).or_default() += 1;
    }
    let coll = |m: &HashMap<Cid, u32>| -> u64 { m.values().map(|c| (*c as u64) - 1).sum() };
    let coll32: u64 = low32.values().map(|c| (*c as u64) - 1).sum();
    let coll48: u64 = low48.values().map(|c| (*c as u64) - 1).sum();
    println!(
        "\n  D2 birthday over {n} distinct leaves:\n    full 128-bit collisions : {}\n    first-32-bit collisions : {} (random expectation ~{:.0})\n    first-48-bit collisions : {} (random expectation ~{:.1})",
        coll(&full),
        coll32,
        (n as f64) * (n as f64) / 2.0 / 4_294_967_296.0,
        coll48,
        (n as f64) * (n as f64) / 2.0 / 281_474_976_710_656.0,
    );

    // D3: DIRECTED. FNV-1a's low byte is a self-contained 8-bit state machine, so the low bits are
    //     cheap to steer. Does that survive `finish`? Search a 2^22 family for a pair agreeing on
    //     the first 8 output bytes (64 bits) -- if the finalizer were weak this would be far more
    //     likely than 2^-64.
    let mut by_hi: HashMap<u64, u64> = HashMap::with_capacity(1 << 22);
    let mut hit: Option<(u64, u64)> = None;
    for i in 0..(1u64 << 22) {
        let mut h = Hasher128::new(0x6c65_6166_0000_0001);
        h.number(1);
        h.field(&i.to_be_bytes());
        h.field(b"v");
        let c = h.finish();
        let hi = u64::from_be_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]);
        if let Some(prev) = by_hi.insert(hi, i) {
            hit = Some((prev, i));
            break;
        }
    }
    println!(
        "  D3 2^22 chosen 8-byte keys, looking for a 64-bit-prefix collision: {}",
        match hit {
            Some((a, b)) => format!("FOUND {a} / {b}"),
            None => "none (birthday expectation at 2^22 is ~2^-20)".to_string(),
        }
    );

    // D4: can two trees with DIFFERENT content share a partition cid, cheaply? The partition cid
    //     is H(count || cid_0 || .. || cid_{n-1}) with no length prefix between cids. Try the one
    //     structural door that leaves: a tree with k leaves vs a tree with k leaves whose cids are
    //     a rotation/regrouping of the first's.
    let a = leaf_cid(&[(b"a".to_vec(), b"1".to_vec())]);
    let b = leaf_cid(&[(b"b".to_vec(), b"2".to_vec())]);
    let mut h1 = Hasher128::new(0x7061_7274_0000_0001);
    h1.number(2);
    h1.cid(&a);
    h1.cid(&b);
    let mut h2 = Hasher128::new(0x7061_7274_0000_0001);
    h2.number(2);
    h2.cid(&b);
    h2.cid(&a);
    println!("  D4 leaf order swapped in the fold: equal={}", h1.finish() == h2.finish());
    let mut h3 = Hasher128::new(0x7061_7274_0000_0001);
    h3.number(1);
    h3.cid(&a);
    let mut h4 = Hasher128::new(0x7061_7274_0000_0001);
    h4.number(2);
    h4.cid(&a);
    println!("  D4 count 1 vs count 2 over the same first cid: equal={}", h3.finish() == h4.finish());
}

fn main() {
    let which = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    if which == "all" || which == "a" {
        attack_a();
        attack_a_seed_sweep();
        attack_a_shuffle_vs_shuffle();
        attack_a_one_key_late();
    }
    if which == "all" || which == "b" {
        attack_b();
    }
    if which == "all" || which == "c" {
        attack_c();
    }
    if which == "all" || which == "d" {
        attack_d();
    }
    // keep Epoch import honest
    let _: Option<Epoch> = None;
}
