//! D23 — what does the latch cost when nothing is contending?
//!
//! One thread, one tree, no other threads in the process. Every latch acquisition here is
//! uncontended, so this measures the *overhead* of the protocol — the `Mutex` + `HashMap` entry
//! per page touched, and the extra descent a pessimistic retry costs — and not any waiting.
//!
//! Run identically against the pre-latch tree and the latched tree; the pre-latch build gets this
//! same file copied in, because the public API (`create`/`insert`/`search`) did not change.
//!
//! Reported as ops/s over ROUNDS independent trees. The BEST round is the headline: it is the one
//! least polluted by whatever else the machine was doing, and this box runs an agent fleet.

use std::sync::Arc;
use std::time::Instant;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;

/// Kept well inside the pool's 1024 frames so this measures the tree and not eviction/disk.
const N: i32 = 40_000;
const ROUNDS: usize = 5;

fn fresh() -> (tempfile::TempDir, Arc<BPlusTreeManager<Value, RecordId>>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("thr.db"))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let tree = Arc::new(BPlusTreeManager::<Value, RecordId>::create(bp).unwrap());
    (dir, tree)
}

fn rid(k: i32) -> RecordId {
    RecordId::new(k as u32 + 1, (k % 100) as u16)
}

fn main() {
    println!("D23 single-threaded B+tree throughput");
    println!("N={N} inserts + {N} searches per round, {ROUNDS} rounds, one thread");
    println!();

    let mut ins = Vec::new();
    let mut sea = Vec::new();

    for round in 0..ROUNDS {
        let (_dir, tree) = fresh();

        let t0 = Instant::now();
        for i in 0..N {
            tree.insert(Value::Integer(i), rid(i)).expect("insert");
        }
        let ins_s = t0.elapsed().as_secs_f64();

        let t1 = Instant::now();
        let mut found = 0usize;
        for i in 0..N {
            if tree.search(&Value::Integer(i)).expect("search").is_some() {
                found += 1;
            }
        }
        let sea_s = t1.elapsed().as_secs_f64();

        // A throughput number over a tree that lost entries is meaningless, so prove it is whole.
        assert_eq!(found, N as usize, "round {round}: tree lost entries; timing is meaningless");

        let (i_ops, s_ops) = (N as f64 / ins_s, N as f64 / sea_s);
        ins.push(i_ops);
        sea.push(s_ops);
        println!(
            "round {round}: insert {i_ops:>10.0} ops/s ({ins_s:.3}s)   search {s_ops:>10.0} ops/s ({sea_s:.3}s)"
        );
    }

    let best = |v: &Vec<f64>| v.iter().cloned().fold(f64::MIN, f64::max);
    let median = |v: &Vec<f64>| {
        let mut s = v.clone();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        s[s.len() / 2]
    };
    println!();
    println!("insert  best {:>10.0} ops/s   median {:>10.0} ops/s", best(&ins), median(&ins));
    println!("search  best {:>10.0} ops/s   median {:>10.0} ops/s", best(&sea), median(&sea));
}
