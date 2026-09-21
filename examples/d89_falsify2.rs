//! THROWAWAY part 2 of the D89 falsification harness. See examples/d89_falsify.rs.
//!
//! E: how much disorder does the "0 shared leaves" result actually need?
//! F: the test's exact body, re-run under other orders, plus the four assertions of the
//!    #[ignore]d `same_data_in_two_orders_must_converge_to_one_partition_cid`.
//! G: physical-layout forcing that proves the C-attack fixtures were not vacuous.

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, PageId};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::cid::{
    hex, leaf_content_cid, leaf_partition_cid, ordered_leaf_cids, subtree_cid, Cid,
};
use ferrodb::cow::page_header::{PageHeader, PageType};
use ferrodb::cow::{CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;
use std::sync::Arc;

struct Fx {
    _dir: tempfile::TempDir,
    cat: Arc<LogBranchCatalog>,
    t: CowTree,
}

fn fixture() -> Fx {
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
        ArenaPageStore::new(pool, Arc::clone(&cat) as Arc<dyn BranchCatalog>, 1024).unwrap(),
    );
    let t = CowTree::new(store as Arc<dyn PageStore>);
    Fx { _dir: dir, cat, t }
}

fn k(n: u32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}
fn v(n: u32) -> Vec<u8> {
    format!("v{n}").into_bytes()
}

fn build(fx: &Fx, keys: &[u32]) -> PageId {
    let e = fx.cat.next_epoch();
    let mut root = fx.t.create(BranchId::TRUNK, e).unwrap();
    for &i in keys {
        root = fx.t.insert(root, BranchId::TRUNK, e, &k(i), &v(i)).unwrap();
    }
    root
}

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

/// Ascending with `swaps` adjacent transpositions.
fn near_sorted(n: u32, swaps: usize, seed: u32) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n).collect();
    let mut state: u32 = seed;
    for _ in 0..swaps {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let i = (state >> 8) as usize % (v.len() - 1);
        v.swap(i, i + 1);
    }
    v
}

/// A partial Fisher-Yates: only the first `frac_pct`% of positions get shuffled at all.
fn partial_shuffle(n: u32, frac_pct: u32) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n).collect();
    let mut state: u32 = 0x5eed_1234;
    let limit = ((n as u64 * frac_pct as u64) / 100) as usize;
    for i in (1..v.len()).rev() {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let j = (state >> 8) as usize % (i + 1);
        if i < limit {
            v.swap(i, j);
        }
    }
    v
}

fn shared_as_test_counts(a: &[Cid], b: &[Cid]) -> usize {
    a.iter().filter(|c| b.contains(c)).count()
}

fn lcs(a: &[Cid], b: &[Cid]) -> usize {
    let mut prev = vec![0usize; b.len() + 1];
    let mut cur = vec![0usize; b.len() + 1];
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            cur[j] = if a[i - 1] == b[j - 1] { prev[j - 1] + 1 } else { prev[j].max(cur[j - 1]) };
        }
        std::mem::swap(&mut prev, &mut cur);
        cur.iter_mut().for_each(|x| *x = 0);
    }
    prev[b.len()]
}

// ---- E ------------------------------------------------------------------------------------------

fn attack_e() {
    println!("\n================ ATTACK E: how much disorder does \"0 shared\" need? ================");
    println!("n=1000, values `v{{n}}` -- the test's OWN fixture. Only the insert order varies.\n");
    let fx = fixture();
    let asc: Vec<u32> = (0..1000).collect();
    let ra = build(&fx, &asc);
    let a = ordered_leaf_cids(&fx.t, ra).unwrap();
    let apart = leaf_partition_cid(&fx.t, ra).unwrap();
    let asub = subtree_cid(&fx.t, ra).unwrap();
    let acont = leaf_content_cid(&fx.t, ra).unwrap();
    println!("  ascending: {} leaves  part={}  sub={}", a.len(), hex(&apart), hex(&asub));
    println!(
        "\n{:>28} {:>7} {:>8} {:>6} {:>8} {:>8} {:>8}",
        "order", "leaves", "shared", "lcs", "partEq", "subEq", "contEq"
    );

    let mut orders: Vec<(String, Vec<u32>)> = Vec::new();
    for t in [0usize, 1, 2, 5, 10, 25, 50, 100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600] {
        orders.push((format!("adjacent-transpositions={t}"), near_sorted(1000, t, 0x13579bdf)));
    }
    for p in [1u32, 2, 5, 10, 20, 40, 60, 80, 100] {
        orders.push((format!("tail-shuffled={p}%"), partial_shuffle(1000, p)));
    }
    orders.push(("full shuffle (the test's)".into(), fixed_shuffle(1000)));

    for (name, o) in &orders {
        let rb = build(&fx, o);
        let b = ordered_leaf_cids(&fx.t, rb).unwrap();
        let identical_order = *o == asc;
        println!(
            "{:>28} {:>7} {:>8} {:>6} {:>8} {:>8} {:>8}{}",
            name,
            b.len(),
            shared_as_test_counts(&a, &b),
            lcs(&a, &b),
            leaf_partition_cid(&fx.t, rb).unwrap() == apart,
            subtree_cid(&fx.t, rb).unwrap() == asub,
            leaf_content_cid(&fx.t, rb).unwrap() == acont,
            if identical_order { "   <- order IS ascending (vacuous)" } else { "" }
        );
    }
}

// ---- F ------------------------------------------------------------------------------------------

fn attack_f() {
    println!("\n================ ATTACK F: the #[ignore]d convergence test, re-run per order ================");
    println!("Those four assertions are the stated acceptance criterion for content-defined chunking.");
    println!("Which of them already hold TODAY, with the byte-balanced split still in place?\n");
    let fx = fixture();
    let asc: Vec<u32> = (0..1000).collect();
    let ra = build(&fx, &asc);

    let cases: Vec<(String, Vec<u32>)> = vec![
        ("full shuffle (the test's own)".into(), fixed_shuffle(1000)),
        ("20 adjacent transpositions".into(), near_sorted(1000, 20, 17)),
        ("1 adjacent transposition".into(), near_sorted(1000, 1, 7)),
        ("400 adjacent transpositions".into(), near_sorted(1000, 400, 0x13579bdf)),
        ("evens ascending then odds".into(), {
            let mut o: Vec<u32> = (0..1000).filter(|x| x % 2 == 0).collect();
            o.extend((0..1000).filter(|x| x % 2 == 1));
            o
        }),
        ("descending".into(), (0..1000).rev().collect()),
    ];

    for (name, o) in &cases {
        let rb = build(&fx, o);
        let c1 = leaf_content_cid(&fx.t, ra).unwrap() == leaf_content_cid(&fx.t, rb).unwrap();
        let c2 = ordered_leaf_cids(&fx.t, ra).unwrap() == ordered_leaf_cids(&fx.t, rb).unwrap();
        let c3 = leaf_partition_cid(&fx.t, ra).unwrap() == leaf_partition_cid(&fx.t, rb).unwrap();
        let c4 = subtree_cid(&fx.t, ra).unwrap() == subtree_cid(&fx.t, rb).unwrap();
        let all = c1 && c2 && c3 && c4;
        println!(
            "  {:<32} content={:<5} leafseq={:<5} partition={:<5} subtree={:<5}  -> ignored test would {}",
            name,
            c1,
            c2,
            c3,
            c4,
            if all { "PASS" } else { "fail" }
        );
    }
}

// ---- G ------------------------------------------------------------------------------------------

fn attack_g() {
    println!("\n================ ATTACK G: prove the physical fixtures are not vacuous ================");
    let fx = fixture();

    // G1: one leaf, 20 entries, inserted ascending vs shuffled. Dump the raw page bytes.
    let asc: Vec<u32> = (0..20).collect();
    let shuf: Vec<u32> = {
        let mut v = asc.clone();
        v.reverse();
        v
    };
    let ra = build(&fx, &asc);
    let rb = build(&fx, &shuf);
    let bytes = |p: PageId| -> Vec<u8> {
        let h = fx.t.store().read_page(p).unwrap();
        let f = h.read();
        f.data.to_vec()
    };
    let pa = bytes(ra);
    let pb = bytes(rb);
    let differing = pa.iter().zip(pb.iter()).filter(|(x, y)| x != y).count();
    let la = ordered_leaf_cids(&fx.t, ra).unwrap();
    let lb = ordered_leaf_cids(&fx.t, rb).unwrap();
    println!(
        "  G1 one leaf, 20 entries, ascending vs descending insert:\n     raw page bytes differing: {} of 4096   leaf_cid equal: {}   ({})",
        differing,
        la == lb,
        hex(&la[0])
    );

    // G2: same entries reached by insert-then-delete rather than insert-only.
    let e = fx.cat.next_epoch();
    let mut r = fx.t.create(BranchId::TRUNK, e).unwrap();
    for i in 0..60u32 {
        r = fx.t.insert(r, BranchId::TRUNK, e, &k(i), &v(i)).unwrap();
    }
    for i in 20..60u32 {
        r = fx.t.delete(r, BranchId::TRUNK, e, &k(i)).unwrap();
    }
    let pd = bytes(r);
    let ld = ordered_leaf_cids(&fx.t, r).unwrap();
    let diff2 = pa.iter().zip(pd.iter()).filter(|(x, y)| x != y).count();
    let header_ok = PageHeader::read_from(&pd.clone().try_into().unwrap()).unwrap().page_type
        == PageType::BTreeLeaf;
    println!(
        "  G2 keys 0..20 by insert-only vs insert-0..60-then-delete-20..60:\n     raw page bytes differing: {} of 4096   is a leaf: {}   leaf_cid equal to G1's: {}",
        diff2,
        header_ok,
        ld == la
    );

    // G3: confirm the churn fixture from attack C really left the page physically different.
    let e3 = fx.cat.next_epoch();
    let mut r3 = fx.t.create(BranchId::TRUNK, e3).unwrap();
    for i in 0..20u32 {
        r3 = fx.t.insert(r3, BranchId::TRUNK, e3, &k(i), &v(i)).unwrap();
    }
    for i in 0..20u32 {
        let junk: Vec<u8> = k(i).into_iter().chain([0xffu8]).collect();
        r3 = fx.t.insert(r3, BranchId::TRUNK, e3, &junk, b"junkjunkjunkjunk").unwrap();
        r3 = fx.t.delete(r3, BranchId::TRUNK, e3, &junk).unwrap();
    }
    let p3 = bytes(r3);
    let l3 = ordered_leaf_cids(&fx.t, r3).unwrap();
    let diff3 = pa.iter().zip(p3.iter()).filter(|(x, y)| x != y).count();
    println!(
        "  G3 keys 0..20 clean vs after 20 insert+delete churn cycles:\n     raw page bytes differing: {} of 4096   leaf_cid equal: {}",
        diff3,
        l3 == la
    );
}

// ---- E2: where exactly is the threshold, and what sets it? --------------------------------------

/// Keys 0..n inserted with each key displaced by at most `d` positions from sorted.
fn bounded_displacement(n: u32, d: u32, seed: u32) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n).collect();
    if d == 0 {
        return v;
    }
    let mut state: u32 = seed;
    for i in 0..v.len() {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let span = (d as usize).min(v.len() - 1 - i);
        if span == 0 {
            continue;
        }
        let j = i + 1 + (state >> 8) as usize % span;
        v.swap(i, j);
    }
    v
}

fn attack_e2() {
    println!("\n================ ATTACK E2: locate the threshold ================");
    let fx = fixture();
    let asc: Vec<u32> = (0..1000).collect();
    let ra = build(&fx, &asc);
    let a = ordered_leaf_cids(&fx.t, ra).unwrap();
    let apart = leaf_partition_cid(&fx.t, ra).unwrap();

    // How many entries does the FIRST split see? Build ascending prefixes until the tree splits.
    let mut first_split_at = 0u32;
    for n in 1..400u32 {
        let r = build(&fx, &(0..n).collect::<Vec<u32>>());
        if ordered_leaf_cids(&fx.t, r).unwrap().len() > 1 {
            first_split_at = n;
            break;
        }
    }
    println!(
        "  ascending inserts: the first leaf split happens on insert #{first_split_at} ({}% of 1000)",
        first_split_at as f64 / 10.0
    );

    println!("\n  head-shuffled p%% (shuffle the first p%% of the insert order, rest ascending):");
    println!("{:>8} {:>7} {:>7} {:>8}", "p%", "leaves", "shared", "partEq");
    for p in [15u32, 18, 20, 21, 22, 23, 25, 30, 35, 40] {
        let o = partial_shuffle(1000, p);
        let rb = build(&fx, &o);
        let b = ordered_leaf_cids(&fx.t, rb).unwrap();
        println!(
            "{:>8} {:>7} {:>7} {:>8}",
            p,
            b.len(),
            shared_as_test_counts(&a, &b),
            leaf_partition_cid(&fx.t, rb).unwrap() == apart
        );
    }

    println!("\n  bounded displacement: every key within d positions of sorted, n=1000:");
    println!("{:>8} {:>7} {:>7} {:>6} {:>8}", "d", "leaves", "shared", "lcs", "partEq");
    for d in [1u32, 2, 4, 8, 16, 32, 64, 96, 128, 192, 256, 384, 512, 1000] {
        let o = bounded_displacement(1000, d, 0x2468_ace0);
        let rb = build(&fx, &o);
        let b = ordered_leaf_cids(&fx.t, rb).unwrap();
        println!(
            "{:>8} {:>7} {:>7} {:>6} {:>8}",
            d,
            b.len(),
            shared_as_test_counts(&a, &b),
            lcs(&a, &b),
            leaf_partition_cid(&fx.t, rb).unwrap() == apart
        );
    }
}

// ---- H: the seed / lineage-pairing questions the test never asked --------------------------------

fn attack_h() {
    println!("\n================ ATTACK H: is 0-of-9/8 the seed, or the family? ================");
    let fx = fixture();
    let asc: Vec<u32> = (0..1000).collect();
    let ra = build(&fx, &asc);
    let a = ordered_leaf_cids(&fx.t, ra).unwrap();
    let apart = leaf_partition_cid(&fx.t, ra).unwrap();
    let acont = leaf_content_cid(&fx.t, ra).unwrap();

    let mut counts = std::collections::HashMap::<usize, usize>::new();
    let (mut max_shared, mut max_lcs, mut part_agree, mut cont_disagree) = (0usize, 0usize, 0, 0);
    for s in 0..300u32 {
        let seed = 0x5eed_1234u32.wrapping_add(s.wrapping_mul(0x9e37_79b9));
        let mut o: Vec<u32> = (0..1000).collect();
        let mut state = seed;
        for i in (1..o.len()).rev() {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let j = (state >> 8) as usize % (i + 1);
            o.swap(i, j);
        }
        let rb = build(&fx, &o);
        let b = ordered_leaf_cids(&fx.t, rb).unwrap();
        *counts.entry(b.len()).or_default() += 1;
        max_shared = max_shared.max(shared_as_test_counts(&a, &b));
        max_lcs = max_lcs.max(lcs(&a, &b));
        if leaf_partition_cid(&fx.t, rb).unwrap() == apart {
            part_agree += 1;
        }
        if leaf_content_cid(&fx.t, rb).unwrap() != acont {
            cont_disagree += 1;
        }
    }
    let mut c: Vec<_> = counts.into_iter().collect();
    c.sort();
    println!("  H1 ascending vs 300 full shuffles (n=1000):");
    println!("     shuffled leaf-count distribution: {c:?}");
    println!("     max shared over 300 seeds = {max_shared}, max lcs = {max_lcs}");
    println!("     seeds whose partition cid equals ascending's = {part_agree} of 300");
    println!("     seeds whose CONTENT cid differs (control)    = {cont_disagree} of 300 (must be 0)");

    let mut worst = (0usize, 0usize, 0usize);
    for s in 0..60u32 {
        let mk = |seed: u32| {
            let mut o: Vec<u32> = (0..1000).collect();
            let mut st = seed;
            for i in (1..o.len()).rev() {
                st = st.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                let j = (st >> 8) as usize % (i + 1);
                o.swap(i, j);
            }
            o
        };
        let r1 = build(&fx, &mk(0x1111_0000u32.wrapping_add(s)));
        let r2 = build(&fx, &mk(0x2222_0000u32.wrapping_add(s)));
        let x = ordered_leaf_cids(&fx.t, r1).unwrap();
        let y = ordered_leaf_cids(&fx.t, r2).unwrap();
        let sh = shared_as_test_counts(&x, &y);
        if sh >= worst.0 {
            worst = (sh, lcs(&x, &y), x.len());
        }
        assert_eq!(
            leaf_content_cid(&fx.t, r1).unwrap(),
            leaf_content_cid(&fx.t, r2).unwrap(),
            "control broke in H2"
        );
    }
    println!(
        "  H2 shuffle-vs-shuffle, 60 pairs (neither side sorted): max shared = {}, lcs = {}, of ~{} leaves",
        worst.0, worst.1, worst.2
    );

    println!("  H3 ascending vs ascending-with-ONE-key-moved-to-the-end:");
    for moved in [0u32, 1, 137, 500, 998, 999] {
        let mut o: Vec<u32> = (0..1000).filter(|&x| x != moved).collect();
        o.push(moved);
        let rb = build(&fx, &o);
        let b = ordered_leaf_cids(&fx.t, rb).unwrap();
        println!(
            "     key {:<4} last: leaves {}/{}  shared={}  lcs={}  partEq={}  contEq={}",
            moved,
            a.len(),
            b.len(),
            shared_as_test_counts(&a, &b),
            lcs(&a, &b),
            leaf_partition_cid(&fx.t, rb).unwrap() == apart,
            leaf_content_cid(&fx.t, rb).unwrap() == acont,
        );
    }
}

// ---- I: force the detectors to fire, and re-do the control without the hash --------------------

fn attack_i() {
    println!("\n================ ATTACK I: force the detectors, drop the hash from the control ================");

    // I1: the birthday detector from attack D found 0 full-128-bit collisions. Prove that counter
    //     can count: plant a known duplicate and a known near-duplicate.
    use ferrodb::cow::cid::leaf_cid;
    let mut m: std::collections::HashMap<Cid, u32> = std::collections::HashMap::new();
    for i in 0..1000u64 {
        *m.entry(leaf_cid(&[(i.to_be_bytes().to_vec(), b"x".to_vec())])).or_default() += 1;
    }
    let before: u64 = m.values().map(|c| (*c as u64) - 1).sum();
    // plant: the same input twice
    *m.entry(leaf_cid(&[(7u64.to_be_bytes().to_vec(), b"x".to_vec())])).or_default() += 1;
    let after: u64 = m.values().map(|c| (*c as u64) - 1).sum();
    println!(
        "  I1 collision counter: {before} over 1000 distinct inputs, {after} after planting one duplicate  -> detector {}",
        if before == 0 && after == 1 { "FIRES" } else { "IS BROKEN" }
    );

    // I2: the 64-bit-prefix search in D3 found nothing. Prove the search works by running the same
    //     loop against a 24-bit prefix, where a collision is certain.
    let mut seen: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
    let mut hit = None;
    for i in 0..(1u64 << 16) {
        let c = leaf_cid(&[(i.to_be_bytes().to_vec(), b"v".to_vec())]);
        let p24 = u32::from_be_bytes([0, c[0], c[1], c[2]]);
        if let Some(prev) = seen.insert(p24, i) {
            hit = Some((prev, i));
            break;
        }
    }
    println!(
        "  I2 same search machinery on a 24-bit prefix: {}  -> search {}",
        match hit {
            Some((a, b)) => format!("collision at inputs {a} / {b}"),
            None => "none".into(),
        },
        if hit.is_some() { "FIRES" } else { "IS BROKEN" }
    );

    // I3: the measurement's CONTROL (`leaf_content_cid` equal) is the one assertion that uses the
    //     hash's UNSOUND direction. Redo it with no hash at all: materialise both trees' entries
    //     and compare the byte sequences directly.
    let fx = fixture();
    let asc: Vec<u32> = (0..1000).collect();
    let shuf = fixed_shuffle(1000);
    let ra = build(&fx, &asc);
    let rb = build(&fx, &shuf);
    let rows = |r: PageId| -> Vec<(Vec<u8>, Vec<u8>)> {
        fx.t.range_scan(r, None, None).unwrap().map(|x| x.unwrap()).collect()
    };
    let ea = rows(ra);
    let eb = rows(rb);
    println!(
        "  I3 hash-free control: ascending tree has {} rows, shuffled tree has {} rows, byte-identical sequences: {}",
        ea.len(),
        eb.len(),
        ea == eb
    );
    println!(
        "     (leaf_content_cid said equal: {})",
        leaf_content_cid(&fx.t, ra).unwrap() == leaf_content_cid(&fx.t, rb).unwrap()
    );
    // and prove THAT comparison can fail
    let rc = build(&fx, &(0..999).collect::<Vec<u32>>());
    println!("     same comparison against a 999-row tree: {}  -> control FIRES", ea == rows(rc));

    // I4: the partition measurement itself, without the hash: compare the ordered per-leaf entry
    //     lists directly, so "0 shared leaves" does not depend on leaf_cid at all.
    let leaf_rows = |r: PageId| -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        // walk leaves in key order by re-deriving boundaries from ordered_leaf_cids' own walk:
        // scan the whole tree, then cut it at the same sizes the leaves have.
        let mut out: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
        let mut stack = vec![r];
        let mut order: Vec<PageId> = Vec::new();
        while let Some(p) = stack.pop() {
            let h = fx.t.store().read_page(p).unwrap();
            let f = h.read();
            let ty = PageHeader::read_from(&f.data).unwrap().page_type;
            let node = ferrodb::cow::node::Node::new(&f.data);
            match ty {
                PageType::BTreeLeaf => order.push(p),
                PageType::BTreeInternal => {
                    let mut kids = vec![node.leftmost()];
                    kids.extend(node.internal_entries().unwrap().into_iter().map(|(_, c)| c));
                    for c in kids.into_iter().rev() {
                        stack.push(c);
                    }
                }
                _ => unreachable!(),
            }
        }
        for p in order {
            let h = fx.t.store().read_page(p).unwrap();
            let f = h.read();
            out.push(ferrodb::cow::node::Node::new(&f.data).leaf_entries().unwrap());
        }
        out
    };
    let la = leaf_rows(ra);
    let lb = leaf_rows(rb);
    let shared_raw = la.iter().filter(|x| lb.contains(x)).count();
    println!(
        "  I4 hash-free partition: {} leaves vs {} leaves, leaves with byte-identical entry lists: {}",
        la.len(),
        lb.len(),
        shared_raw
    );
    println!(
        "     leaf sizes ascending: {:?}",
        la.iter().map(|x| x.len()).collect::<Vec<_>>()
    );
    println!(
        "     leaf sizes shuffled : {:?}",
        lb.iter().map(|x| x.len()).collect::<Vec<_>>()
    );
    // force it: compare a tree against itself
    println!(
        "     same comparison of the ascending tree against a second ascending build: {} of {}  -> detector FIRES",
        {
            let rd = build(&fx, &asc);
            let ld = leaf_rows(rd);
            la.iter().filter(|x| ld.contains(x)).count()
        },
        la.len()
    );
}

// ---- J: two WELL-FORMED, functionally identical trees with one partition cid -------------------

fn attack_j() {
    println!("\n================ ATTACK J: same partition cid, two valid trees ================");
    use ferrodb::cow::node::{Node, NodeMut};
    use ferrodb::cow::page_header::stamp_checksum;
    let fx = fixture();
    let e = fx.cat.next_epoch();

    // Donor: a real tree, so every leaf is well-formed and in key order.
    let donor = build(&fx, &(0..600).collect::<Vec<u32>>());
    let mut leaves: Vec<PageId> = Vec::new();
    let mut stack = vec![donor];
    while let Some(p) = stack.pop() {
        let h = fx.t.store().read_page(p).unwrap();
        let f = h.read();
        let ty = PageHeader::read_from(&f.data).unwrap().page_type;
        let n = Node::new(&f.data);
        if ty == PageType::BTreeLeaf {
            leaves.push(p);
        } else {
            let mut kids = vec![n.leftmost()];
            kids.extend(n.internal_entries().unwrap().into_iter().map(|(_, c)| c));
            for c in kids.into_iter().rev() {
                stack.push(c);
            }
        }
    }
    let first_key = |p: PageId| -> Vec<u8> {
        let h = fx.t.store().read_page(p).unwrap();
        let f = h.read();
        Node::new(&f.data).leaf_entries().unwrap()[0].0.clone()
    };
    let (l0, l1, l2) = (leaves[0], leaves[1], leaves[2]);
    let (s1, s2) = (first_key(l1), first_key(l2));

    let mk = |leftmost: PageId, entries: &[(Vec<u8>, PageId)]| -> PageId {
        let id = fx.t.store().alloc_for(BranchId::TRUNK, PageType::BTreeInternal, e).unwrap();
        let h = fx.t.store().read_page(id).unwrap();
        let mut f = h.write();
        NodeMut::new(&mut f.data).fill_internal(leftmost, entries).unwrap();
        stamp_checksum(&mut f.data);
        drop(f);
        id
    };

    // Shape A: one internal root, three leaf children. CORRECT separators.
    let flat = mk(l0, &[(s1.clone(), l1), (s2.clone(), l2)]);
    // Shape B: two levels. Also correct separators, just a different grouping.
    let inner = mk(l0, &[(s1.clone(), l1)]);
    let deep = mk(inner, &[(s2.clone(), l2)]);

    // Both must be genuinely usable trees answering identically.
    let mut probed = 0usize;
    let mut disagreed = 0usize;
    let mut missing = 0usize;
    for p in [l0, l1, l2] {
        let h = fx.t.store().read_page(p).unwrap();
        let f = h.read();
        for (kk, vv) in Node::new(&f.data).leaf_entries().unwrap() {
            probed += 1;
            let a = fx.t.get(flat, &kk).unwrap();
            let b = fx.t.get(deep, &kk).unwrap();
            if a != b {
                disagreed += 1;
            }
            if a.as_deref() != Some(vv.as_slice()) {
                missing += 1;
            }
        }
    }
    println!(
        "  both shapes probed on all {probed} keys: get() disagreements = {disagreed}, wrong/absent values = {missing}"
    );
    println!("  shape A height 2 (root -> 3 leaves), shape B height 3 (root -> inner -> leaves)");
    println!(
        "    leaf_partition_cid  A={}  B={}  equal={}",
        hex(&leaf_partition_cid(&fx.t, flat).unwrap()),
        hex(&leaf_partition_cid(&fx.t, deep).unwrap()),
        leaf_partition_cid(&fx.t, flat).unwrap() == leaf_partition_cid(&fx.t, deep).unwrap()
    );
    println!(
        "    leaf_content_cid    A={}  B={}  equal={}",
        hex(&leaf_content_cid(&fx.t, flat).unwrap()),
        hex(&leaf_content_cid(&fx.t, deep).unwrap()),
        leaf_content_cid(&fx.t, flat).unwrap() == leaf_content_cid(&fx.t, deep).unwrap()
    );
    println!(
        "    subtree_cid         A={}  B={}  equal={}",
        hex(&subtree_cid(&fx.t, flat).unwrap()),
        hex(&subtree_cid(&fx.t, deep).unwrap()),
        subtree_cid(&fx.t, flat).unwrap() == subtree_cid(&fx.t, deep).unwrap()
    );
    println!(
        "    walk_pages          A={} pages  B={} pages",
        fx.t.walk_pages(flat).unwrap().len(),
        fx.t.walk_pages(deep).unwrap().len()
    );
}

// ---- K: the big-value regime, where a leaf holds only a handful of entries ----------------------

fn attack_k() {
    println!("\n================ ATTACK K: 990-byte values (4 entries per leaf max) ================");
    println!("{:>6} {:<22} {:>6} {:>6} {:>7} {:>5} {:>7}", "n", "order", "lvsA", "lvsB", "shared", "lcs", "partEq");
    let fx = fixture();
    let bigv = |n: u32| -> Vec<u8> {
        let mut o = n.to_be_bytes().to_vec();
        while o.len() < 990 {
            o.push(b'a' + ((n as u8).wrapping_add(o.len() as u8) % 26));
        }
        o
    };
    let build_big = |keys: &[u32]| -> PageId {
        let e = fx.cat.next_epoch();
        let mut root = fx.t.create(BranchId::TRUNK, e).unwrap();
        for &i in keys {
            root = fx.t.insert(root, BranchId::TRUNK, e, &k(i), &bigv(i)).unwrap();
        }
        root
    };
    for n in [4u32, 8, 16, 32, 64, 128, 256, 500, 1000, 2000] {
        let asc: Vec<u32> = (0..n).collect();
        let ra = build_big(&asc);
        let a = ordered_leaf_cids(&fx.t, ra).unwrap();
        let apart = leaf_partition_cid(&fx.t, ra).unwrap();
        let mut orders: Vec<(String, Vec<u32>)> = vec![
            ("full shuffle".into(), {
                let mut v: Vec<u32> = (0..n).collect();
                let mut st: u32 = 0x5eed_1234;
                for i in (1..v.len()).rev() {
                    st = st.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    let j = (st >> 8) as usize % (i + 1);
                    v.swap(i, j);
                }
                v
            }),
            ("descending".into(), (0..n).rev().collect()),
            ("evens then odds".into(), {
                let mut o: Vec<u32> = (0..n).filter(|x| x % 2 == 0).collect();
                o.extend((0..n).filter(|x| x % 2 == 1));
                o
            }),
        ];
        orders.retain(|(_, o)| o.len() == n as usize && *o != asc);
        for (name, o) in &orders {
            let rb = build_big(o);
            let b = ordered_leaf_cids(&fx.t, rb).unwrap();
            println!(
                "{:>6} {:<22} {:>6} {:>6} {:>7} {:>5} {:>7}",
                n,
                name,
                a.len(),
                b.len(),
                shared_as_test_counts(&a, &b),
                lcs(&a, &b),
                leaf_partition_cid(&fx.t, rb).unwrap() == apart
            );
        }
    }
}

// ---- L: confirm the positive-sharing findings with NO hash involved ----------------------------

fn attack_l() {
    println!("\n================ ATTACK L: hash-free confirmation of the sharing findings ================");
    use ferrodb::cow::node::Node;
    let fx = fixture();
    let leaf_rows = |r: PageId| -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut order: Vec<PageId> = Vec::new();
        let mut stack = vec![r];
        while let Some(p) = stack.pop() {
            let h = fx.t.store().read_page(p).unwrap();
            let f = h.read();
            let ty = PageHeader::read_from(&f.data).unwrap().page_type;
            let n = Node::new(&f.data);
            if ty == PageType::BTreeLeaf {
                order.push(p);
            } else {
                let mut kids = vec![n.leftmost()];
                kids.extend(n.internal_entries().unwrap().into_iter().map(|(_, c)| c));
                for c in kids.into_iter().rev() {
                    stack.push(c);
                }
            }
        }
        order
            .into_iter()
            .map(|p| {
                let h = fx.t.store().read_page(p).unwrap();
                let f = h.read();
                Node::new(&f.data).leaf_entries().unwrap()
            })
            .collect()
    };
    let rows = |r: PageId| -> Vec<(Vec<u8>, Vec<u8>)> {
        fx.t.range_scan(r, None, None).unwrap().map(|x| x.unwrap()).collect()
    };

    let build_w = |keys: &[u32], w: usize| -> PageId {
        let e = fx.cat.next_epoch();
        let mut root = fx.t.create(BranchId::TRUNK, e).unwrap();
        for &i in keys {
            let mut val = i.to_be_bytes().to_vec();
            if w == 0 {
                val = format!("v{i}").into_bytes();
            } else {
                while val.len() < w {
                    val.push(b'a' + ((i as u8).wrapping_add(val.len() as u8) % 26));
                }
            }
            root = fx.t.insert(root, BranchId::TRUNK, e, &k(i), &val).unwrap();
        }
        root
    };

    let cases: Vec<(&str, u32, usize, Vec<u32>)> = vec![
        ("n=1000 vlen=6  near-20swaps", 1000, 0, near_sorted(1000, 20, 17)),
        ("n=1000 vlen=6  full shuffle", 1000, 0, fixed_shuffle(1000)),
        ("n=1000 vlen=6  evens-then-odds", 1000, 0, {
            let mut o: Vec<u32> = (0..1000).filter(|x| x % 2 == 0).collect();
            o.extend((0..1000).filter(|x| x % 2 == 1));
            o
        }),
        ("n=1000 vlen=6  descending", 1000, 0, (0..1000).rev().collect()),
        ("n=1000 vlen=200 evens-then-odds", 1000, 200, {
            let mut o: Vec<u32> = (0..1000).filter(|x| x % 2 == 0).collect();
            o.extend((0..1000).filter(|x| x % 2 == 1));
            o
        }),
        ("n=2000 vlen=200 evens-then-odds", 2000, 200, {
            let mut o: Vec<u32> = (0..2000).filter(|x| x % 2 == 0).collect();
            o.extend((0..2000).filter(|x| x % 2 == 1));
            o
        }),
    ];
    for (name, n, w, order) in &cases {
        let asc: Vec<u32> = (0..*n).collect();
        let ra = build_w(&asc, *w);
        let rb = build_w(order, *w);
        let la = leaf_rows(ra);
        let lb = leaf_rows(rb);
        let shared = la.iter().filter(|x| lb.contains(x)).count();
        println!(
            "  {:<34} order differs from ascending: {}   rows identical: {}   leaves {}/{}   byte-identical leaves: {}",
            name,
            *order != asc,
            rows(ra) == rows(rb),
            la.len(),
            lb.len(),
            shared
        );
    }
}

fn main() {
    let which = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    if which == "all" || which == "l" {
        attack_l();
    }
    if which == "all" || which == "k" {
        attack_k();
    }
    if which == "all" || which == "j" {
        attack_j();
    }
    if which == "all" || which == "i" {
        attack_i();
    }
    if which == "all" || which == "e2" {
        attack_e2();
    }
    if which == "all" || which == "h" {
        attack_h();
    }
    if which == "all" || which == "e" {
        attack_e();
    }
    if which == "all" || which == "f" {
        attack_f();
    }
    if which == "all" || which == "g" {
        attack_g();
    }
}
