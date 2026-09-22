//! **D126 — what does the atomic `upsert` cost, against the delete-then-insert it replaces?**
//!
//! `set_root` and `renew_lease` are hot-path writes and both route the branch RECORD key through
//! `TableBranchCatalog::upsert`. A replace primitive that closed the absence window but cost more
//! than the two calls it replaced would trade one problem for another, so this prices it.
//!
//! **What it compares**, all on one tree, one process, one file — the only way two numbers can be
//! subtracted or divided (see `bench/d51`, and the same-instrument rule in
//! `table_catalog::serial_section_profile`):
//!
//! * `delete + insert`  — the old shape, two full root-to-leaf descents and two page writes.
//! * `upsert`           — one descent, one page write.
//! * `search`           — one descent, no write. The CONTROL: neither arm changes it, so if this
//!                        moves between rounds the box drifted and the round is not comparable.
//!
//! **Arm order is rotated every round** (a Latin square over three arms), because running A then
//! B then C every round charges any monotone drift — cache warming, thermal, another tenant — to
//! whichever arm goes last. Every round's numbers are printed, not just the summary, so drift is
//! visible rather than averaged away.
//!
//! The value is FIXED WIDTH, which is the shape a serialised core record has: a same-size rewrite
//! cannot overflow the leaf, so this prices the fast path both arms actually take. The splitting
//! path is a correctness question, not a hot-path one, and `tests/d126_atomic_upsert.rs` owns it.
//!
//!   cargo run --release --example d126_upsert_cost

use std::ops::Bound;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::index::BPlusTreeManager;

type Tree = BPlusTreeManager<Vec<u8>, Vec<u8>>;

/// Enough keys for a multi-level tree, so a descent is a real descent. A profile taken on a
/// single-leaf tree measures the best case of every arm and understates all of them equally,
/// which hides exactly the difference this is looking for.
const PREPOP: u32 = 40_000;
const ITERS: usize = 20_000;
const ROUNDS: usize = 6;

fn key(k: u32) -> Vec<u8> {
    k.to_be_bytes().to_vec()
}
fn val(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn timed<F: FnMut()>(iters: usize, mut f: F) -> f64 {
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    t.elapsed().as_secs_f64() * 1e6 / iters as f64
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("d126.db"))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let tree: Tree = Tree::create(bp).expect("create");
    for k in 0..PREPOP {
        tree.insert(key(k), val(k as u64)).expect("prepop");
    }
    // Spread the targets so no arm keeps hammering the one leaf a previous arm just warmed.
    let targets: Vec<Vec<u8>> = (0..3).map(|i| key(PREPOP / 4 + i * PREPOP / 4)).collect();

    // An INTEGER counter taken inside the loops, so a duration cannot be the whole story: if an
    // arm is fast because it did nothing, `hits` says so. A right shape with a wrong magnitude
    // and a wrong shape look identical in ms/op alone.
    let mut hits = 0u64;

    let mut rows: Vec<[f64; 3]> = Vec::new();
    println!("D126 -- atomic upsert vs delete+insert. {PREPOP} keys resident, {ITERS} iters/arm, \
              {ROUNDS} rounds, arm order rotated each round.");
    println!();
    println!("  round  order     del+ins us/op    upsert us/op     search us/op");

    for r in 0..ROUNDS {
        let k = targets[r % targets.len()].clone();
        let mut t = [0.0f64; 3]; // [del+ins, upsert, search]
        let mut order = [0usize, 1, 2];
        order.rotate_left(r % 3);
        let mut seq = 0u64;
        for &arm in &order {
            match arm {
                0 => {
                    t[0] = timed(ITERS, || {
                        seq += 1;
                        match tree.delete(&k) {
                            Ok(()) | Err(FerroError::KeyNotFound) => {}
                            Err(e) => panic!("delete: {e:?}"),
                        }
                        tree.insert(k.clone(), val(seq)).expect("insert");
                    });
                }
                1 => {
                    t[1] = timed(ITERS, || {
                        seq += 1;
                        tree.upsert(k.clone(), val(seq)).expect("upsert");
                    });
                }
                _ => {
                    t[2] = timed(ITERS, || {
                        if let Ok(Some(_)) = tree.search(&k) {
                            hits += 1;
                        }
                    });
                }
            }
        }
        println!(
            "  {r:>5}  {:?}  {:12.4}  {:14.4}  {:14.4}",
            order, t[0], t[1], t[2]
        );
        rows.push(t);
    }

    let del_ins = median(rows.iter().map(|r| r[0]).collect());
    let ups = median(rows.iter().map(|r| r[1]).collect());
    let search = median(rows.iter().map(|r| r[2]).collect());
    println!();
    println!("  median  delete+insert  {del_ins:8.4} us/op");
    println!("  median  upsert         {ups:8.4} us/op");
    println!("  median  search         {search:8.4} us/op   <- CONTROL, unaffected by either arm");
    println!("  upsert / (delete+insert) = {:.3}x", ups / del_ins);
    println!("  upsert - search          = {:.4} us  (the write half of one descent)", ups - search);
    println!("  search hits              = {hits}   (0 here would mean the control measured nothing)");
    let spread = rows.iter().map(|r| r[2]).fold(f64::MIN, f64::max)
        / rows.iter().map(|r| r[2]).fold(f64::MAX, f64::min);
    println!("  CONTROL spread across rounds = {spread:.2}x");
    println!("  If that spread is large, the box drifted during the run and the ratio above is a");
    println!("  ratio of two different machines. Re-run on a quiet box before quoting it.");

    // The key must hold exactly one entry after all of this -- a replace that left duplicates
    // would be cheap and wrong, and us/op cannot tell the difference.
    //
    // ⚠ The scan is UNBOUNDED, and that is not laziness. A `range_scan` seeded at the key itself
    // cannot count duplicates: its lower bound lands via `BPlusTreeLeafPage::binary_search`, and
    // `slice::binary_search` over an array containing duplicates returns *an* index of a match
    // rather than the first, so a scan seeded that way can begin AFTER a duplicate and report 1
    // where there are 2 -- a detector that fails in the direction that looks like success. The
    // same mistake is called out in `tests/d126_atomic_upsert.rs::occurrences`, and this file had
    // it too. One full pass over {PREPOP} keys, once, at the end of the run.
    let mut counts = vec![0usize; targets.len()];
    for e in tree.range_scan(Bound::Unbounded, Bound::Unbounded).unwrap() {
        let (kk, _) = e.unwrap();
        for (i, k) in targets.iter().enumerate() {
            if &kk == k {
                counts[i] += 1;
            }
        }
    }
    for (i, n) in counts.iter().enumerate() {
        assert_eq!(*n, 1, "target {i} ended with {n} entries; a replace must leave exactly one");
    }
    assert!(hits > 0, "the search control never found the key");
}
