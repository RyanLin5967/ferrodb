//! Does the DURABLE branch catalog hold up when N branches fork from one parent?
//!
//! `fork` clones the parent record and appends it WHOLE, and `live_children` lives inside that
//! record — so the bytes written per fork grow with the number of children the parent already has.
//! That predicts O(N^2) total bytes and an O(N^2) reopen. The scaling bench cannot see any of it:
//! it uses `LogBranchCatalog::in_memory`, which never writes.
use std::sync::Arc;
use std::time::Instant;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let dir = std::env::temp_dir().join(format!("ferrodb-durable-fork-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("branches.log");

    let cat = Arc::new(LogBranchCatalog::open(&path, 1).expect("open catalog"));
    let lease = LeaseDeadline(u64::MAX);

    let mut first = 0f64;
    let mut last = 0f64;
    let t0 = Instant::now();
    for i in 0..n {
        let t = Instant::now();
        cat.fork(BranchId::TRUNK, lease).expect("fork");
        let us = t.elapsed().as_secs_f64() * 1e6;
        if i == 0 { first = us; }
        if i + 1 == n { last = us; }
    }
    let total = t0.elapsed().as_secs_f64();
    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    let t = Instant::now();
    let reopened = LogBranchCatalog::open(&path, 1).expect("reopen");
    let reopen = t.elapsed().as_secs_f64();
    let live = reopened.live_count();

    println!("{n}\t{first:.2}\t{last:.2}\t{total:.3}\t{bytes}\t{reopen:.3}\t{live}");
    let _ = std::fs::remove_dir_all(&dir);
}
