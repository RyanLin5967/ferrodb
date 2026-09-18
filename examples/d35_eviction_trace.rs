//! D35 C1 hit-rate gate, tier 1: **replay a fixed trace and print the eviction sequence.**
//!
//! # Why this exists
//!
//! The D35 design entry names hit rate as the check that a throughput benchmark cannot make, and
//! the one that killed the `arc_cache`-sharding option: sharding produces N caches of capacity/N,
//! each adapting to a 1/N sample, and the cost lands on hit RATE where a faults-per-second number
//! cannot see it. Something could have been shipped, measured as faster, and been a regression.
//!
//! Taking the page table off the hit path has an unusually strong answer to that question, and
//! this is what turns the answer into evidence. **ARC is not modified at all** by C1 — the page
//! table carries no policy and no adaptivity, it is an exact map from page id to frame — so the
//! gate is an EQUALITY assertion rather than a tolerance. Any difference at all is a bug.
//!
//! # How to use it
//!
//! Build this example at the BEFORE commit and at the AFTER commit and diff the two outputs. They
//! must be byte-identical:
//!
//! ```text
//! cargo run --release --example d35_eviction_trace > /tmp/before.txt   # at the base commit
//! cargo run --release --example d35_eviction_trace > /tmp/after.txt    # at the C1 commit
//! diff /tmp/before.txt /tmp/after.txt && echo IDENTICAL
//! ```
//!
//! Single-threaded on purpose. Concurrency makes the eviction sequence a function of the
//! scheduler, and then "identical" is unmeetable for reasons that have nothing to do with the
//! change. The question here is whether the POLICY sees the same sequence of requests and answers
//! them the same way, and that is a single-threaded question.
//!
//! # What it would miss, stated rather than glossed
//!
//! It compares what the page table holds. If a change made ARC's internal ghost lists diverge
//! without yet changing which page is evicted, this would not see it until the divergence reached
//! an eviction — which, over 4x the pool in three differently-shaped phases, it will. It also says
//! nothing about the concurrent case; that is the harness's `reads_per_fetch` column in
//! `bufpool_fault_concurrency`, and a structural argument that nothing in C1 touches ARC.

use std::collections::BTreeSet;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

/// Four times the 1024-frame pool, so eviction runs throughout rather than only at the end.
const PAGES: u32 = 4096;
/// The hot set for the frequency-biased phase. Fits the pool many times over, so ARC's `p`
/// parameter has something to adapt towards and away from.
const HOT: u32 = 200;

/// A fixed LCG. The trace must be a function of nothing but this file.
struct Lcg(u32);
impl Lcg {
    fn next(&mut self) -> u32 {
        self.0 = self.0.wrapping_mul(1664525).wrapping_add(1013904223);
        self.0
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("ferro-d35-trace-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("trace.db");
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());

    // Write every page through the disk manager directly, so the pool starts cold and the setup
    // contributes nothing to the eviction sequence.
    let mut ids = Vec::with_capacity(PAGES as usize);
    for _ in 0..PAGES {
        let id = dm.allocate().expect("allocate");
        let mut data = [0u8; PAGE_SIZE];
        data[0..4].copy_from_slice(&id.to_be_bytes());
        dm.write(id, &data).expect("write");
        ids.push(id);
    }

    let bp = Arc::new(BufferPoolManager::new(dm));

    println!("# D35 C1 hit-rate gate, tier 1: deterministic eviction sequence");
    println!("# pages={PAGES} pool_frames=1024 hot_set={HOT}");
    println!("# single-threaded; the sequence must be byte-identical before and after C1");
    println!();

    // The trace, as (phase, page id) pairs. Built up front so it is plainly a fixed input and not
    // a function of anything the pool does.
    let mut trace: Vec<(&str, u32)> = Vec::new();

    // 1. Sequential scan. The recency-hostile shape: every page is touched once, so a pure-LRU
    //    policy evicts everything it just loaded and ARC's frequency half is what saves it.
    for &id in &ids {
        trace.push(("scan", id));
    }

    // 2. Frequency-biased. A hot set that fits, interleaved with cold single-touch pages, which is
    //    the workload ARC's adaptivity exists for.
    let mut rng = Lcg(0x5EED_1234);
    for i in 0..8000 {
        if i % 4 == 3 {
            let cold = HOT + (rng.next() % (PAGES - HOT));
            trace.push(("cold", ids[cold as usize]));
        } else {
            let hot = rng.next() % HOT;
            trace.push(("hot", ids[hot as usize]));
        }
    }

    // 3. Reverse scan, to flush the adapted state through a shape it was not adapted to.
    for &id in ids.iter().rev() {
        trace.push(("rscan", id));
    }

    let mut resident: BTreeSet<u32> = BTreeSet::new();
    let mut evictions = 0usize;
    let mut checksum: u64 = 1469598103934665603; // FNV-1a offset basis

    for (step, (phase, id)) in trace.iter().enumerate() {
        let frame = match bp.fetch_page(*id) {
            Ok(f) => f,
            Err(e) => {
                println!("{step}\t{phase}\t{id}\tFETCH_FAILED\t{e:?}");
                // A failed fetch here is a defect, not a data point. Say so loudly and stop:
                // continuing would produce a sequence that looks like a measurement.
                eprintln!("TRACE ABORTED at step {step}: fetch_page({id}) failed with {e:?}");
                std::process::exit(2);
            }
        };
        // The stamp check is the same one the throughput harness makes: a sequence that is
        // identical but built out of wrong pages is not evidence of anything.
        {
            let f = bp.frames[frame].read().unwrap();
            let stamp = u32::from_be_bytes([f.data[0], f.data[1], f.data[2], f.data[3]]);
            if f.page_id != Some(*id) || stamp != *id {
                eprintln!(
                    "TRACE ABORTED at step {step}: fetch_page({id}) returned frame {frame} \
                     labelled {:?} holding page {stamp}",
                    f.page_id
                );
                std::process::exit(2);
            }
        }
        bp.unpin_page(*id, false);

        let now: BTreeSet<u32> = bp.page_table.read().unwrap().keys().copied().collect();
        for gone in resident.difference(&now) {
            evictions += 1;
            println!("{step}\t{phase}\tEVICT\t{gone}");
            // FNV-1a over the eviction sequence, so the whole sequence has one comparable value
            // as well as being diffable line by line.
            for b in gone.to_be_bytes() {
                checksum ^= b as u64;
                checksum = checksum.wrapping_mul(1099511628211);
            }
        }
        resident = now;
    }

    println!();
    println!("# trace_steps={}", trace.len());
    println!("# evictions={evictions}");
    println!("# eviction_sequence_fnv1a=0x{checksum:016x}");
    let final_resident: Vec<u32> = resident.iter().copied().collect();
    println!("# final_resident_count={}", final_resident.len());
    println!("# final_resident={final_resident:?}");

    // A run that evicted nothing has not exercised the policy, and would report a cheerful
    // "identical" for two implementations that both do nothing.
    if evictions == 0 {
        eprintln!("EMPTY GATE: zero evictions. The trace never filled the pool, so this compares nothing.");
        std::process::exit(2);
    }
    println!("TRACE_EXIT=0");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}
