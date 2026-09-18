//! PROBE: the branch->root lookup that EVERY read pays.
//!
//! `AgentRuntime::get_row` (runtime.rs:594) calls `root_of` (561) -> `self.branches.get(branch)`,
//! a full B+tree descent in the branch catalog, before the data tree is touched at all.
//!
//! bench/curve_to_1e6.txt line 12-15 explains why a 266 MB catalog does not thrash a 4.2 MB pool:
//! "every insert on the fork path lands at the EDGE of its span". Branch ids come from
//! `next_id.fetch_add(1)` (table_catalog.rs:742) and the RECORD key is `[0x00][id BE]`
//! (tree_keys.rs:75-78), so forks append to the rightmost leaf. TRUE FOR WRITES.
//!
//! A read resolves an ARBITRARY live branch id -- a uniformly located leaf. This probe asks
//! whether the non-thrash result transfers.
//!
//! Arms, same catalog, same code, differing only in id ORDER:
//!   SEQ  -- ascending ids (the fork path's access pattern)
//!   RAND -- uniformly random live ids (the read path's access pattern)
//! Control: a small N whose whole catalog fits the pool. Both arms must agree there, or the
//! instrument is measuring id order rather than residency.

use std::time::Instant;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

const SAMPLES: usize = 20_000;

fn lcg(s: &mut u64) -> u64 { *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); *s >> 11 }

fn run(n: usize) -> (f64, f64, u64) {
    let dir = std::env::temp_dir().join(format!("ferro-catloc-{}-{}", std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    let cat_path = dir.join("b.branchcat");
    let _ = std::fs::remove_file(&cat_path);
    let cat = TableBranchCatalog::open_sidecar(&cat_path, 1).expect("open");

    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        ids.push(cat.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).expect("fork").branch_id);
    }
    let cat_bytes = std::fs::metadata(&cat_path).map(|m| m.len()).unwrap_or(0);

    // SEQ: ascending ids, the fork path's order.
    let mut sink = 0u64;
    let t0 = Instant::now();
    for i in 0..SAMPLES {
        let b = ids[(i * ids.len()) / SAMPLES.max(1) % ids.len()];
        sink ^= cat.get(b).expect("seq get").root_page_id as u64;
    }
    let seq_us = t0.elapsed().as_secs_f64() * 1e6 / SAMPLES as f64;

    // RAND: uniformly random live ids, the read path's order.
    let mut s = 0x243F6A8885A308D3u64;
    let mut picks = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES { picks.push(ids[(lcg(&mut s) as usize) % ids.len()]); }
    let t1 = Instant::now();
    for b in &picks { sink ^= cat.get(*b).expect("rand get").root_page_id as u64; }
    let rand_us = t1.elapsed().as_secs_f64() * 1e6 / SAMPLES as f64;

    assert_ne!(sink, 0xdeadbeef);
    let _ = std::fs::remove_dir_all(&dir);
    (seq_us, rand_us, cat_bytes)
}

#[test]
fn branch_root_resolution_loses_its_locality_when_the_catalog_outgrows_the_pool() {
    eprintln!("pool = 1024 frames x 4096 B = 4.19 MB (buffer_pool.rs:45, compile-time const)");
    eprintln!("     N |  catalog MB | pool x |   SEQ us |  RAND us | RAND/SEQ");
    let mut results = Vec::new();
    for n in [10_000usize, 100_000] {
        let (seq, rand, bytes) = run(n);
        let mb = bytes as f64 / 1e6;
        eprintln!("{:>6} | {:>11.1} | {:>6.1} | {:>8.3} | {:>8.3} | x{:.2}",
                  n, mb, mb * 1e6 / (1024.0 * 4096.0), seq, rand, rand / seq);
        results.push((n, seq, rand, mb));
    }
    // CONTROL: at 10k the catalog is ~2.7 MB, inside the pool -- the two arms must agree.
    let (_, s0, r0, mb0) = results[0];
    assert!(mb0 * 1e6 < 1024.0 * 4096.0, "control N is not resident ({mb0:.1} MB); pick a smaller N");
    eprintln!("\ncontrol (resident) RAND/SEQ = x{:.2}", r0 / s0);
    eprintln!("test    (6x over)  RAND/SEQ = x{:.2}", results[1].2 / results[1].1);
}
