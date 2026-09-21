//! D95 — **what do `is_ancestor` and `lca` cost as the branch chain DEEPENS?**
//!
//! # The two arms, in one run
//!
//! * **walk** — the parent-pointer walk, which is what `reaper::detach_from_parent` does today
//!   and what any caller would write in the absence of an index.
//! * **index** — [`VersionGraph`], binary lifting over jump pointers.
//!
//! Both run in the same process against the same tree, alternating arm order per depth, because a
//! comparison against a remembered number is not a comparison.
//!
//! # The walk arm is deliberately given the best case
//!
//! It walks **dense `Vec<u32>` arrays** — one array index per step, the same per-step cost the
//! indexed arm pays. So the only thing separating the two curves is the NUMBER of steps, which is
//! the complexity class and nothing else.
//!
//! That flatters the walk considerably. The real walk in ferrodb is
//! `reaper::detach_from_parent`, whose every step is a `has_live_children` call — a
//! `TableBranchCatalog` B+tree descent, plus a scan of a child span. The gap this harness reports
//! is therefore a LOWER BOUND on the gap in the database.
//!
//! # Shape
//!
//! Two chains of depth D hang off one root, so `lca(leafA, leafB)` is the root: the case that
//! forces the walk to climb the whole way and the index to descend every level. `is_ancestor` is
//! asked as `is_ancestor(root, leafA)`, which is likewise the full climb.
//!
//! **PRE-REGISTERED, before the first run:**
//!   * walk LINEAR in D, index FLAT or LOG   -> the structural claim holds; report the slope.
//!   * both linear                           -> binary lifting is not being used; find out why
//!                                              before reporting anything.
//!   * index linear only past some D         -> a cache effect, not a complexity one; re-run with
//!                                              the jump tables warmed and say so.
//!
//! The harness **refuses and exits non-zero** if the two arms ever disagree on an answer: a
//! performance comparison between arms computing different things is not a result.
//!
//! ⚠ Absolutes are this box under whatever else it is running. The SHAPE across D is the result.
//!
//! Usage: `D95_DEPTHS=10,100,1000,10000,100000 cargo run --release --example d95_ancestry_curve`

use std::hint::black_box;
use std::time::{Duration, Instant};

use ferrodb::branch::types::BranchId;
use ferrodb::branch::version_graph::VersionGraph;

/// The naive arm: parent pointers in dense arrays, walked.
struct Walk {
    parent: Vec<u32>,
    depth: Vec<u32>,
}

const NO_PARENT: u32 = u32::MAX;

impl Walk {
    /// Steps taken, and whether `a` is an ancestor of `b`.
    fn is_ancestor(&self, a: u32, b: u32) -> (bool, u64) {
        let mut steps = 0u64;
        if self.depth[a as usize] >= self.depth[b as usize] {
            return (false, steps);
        }
        let mut cur = b;
        loop {
            let p = self.parent[cur as usize];
            steps += 1;
            if p == NO_PARENT {
                return (false, steps);
            }
            if p == a {
                return (true, steps);
            }
            cur = p;
        }
    }

    /// The textbook walk: lift the deeper one, then climb in lockstep. `depth` is O(1) here, as
    /// it is in ferrodb proper (`BranchRecord.depth` is stored at fork), so this arm is not being
    /// penalised for having to count depth.
    fn lca(&self, mut a: u32, mut b: u32) -> (Option<u32>, u64) {
        let mut steps = 0u64;
        while self.depth[a as usize] > self.depth[b as usize] {
            a = self.parent[a as usize];
            steps += 1;
        }
        while self.depth[b as usize] > self.depth[a as usize] {
            b = self.parent[b as usize];
            steps += 1;
        }
        while a != b {
            if self.parent[a as usize] == NO_PARENT || self.parent[b as usize] == NO_PARENT {
                return (None, steps);
            }
            a = self.parent[a as usize];
            b = self.parent[b as usize];
            steps += 2;
        }
        (Some(a), steps)
    }
}

/// Median nanoseconds per operation. Times a BATCH so the clock's resolution never becomes the
/// measurement, and takes the median of several samples so one scheduler hiccup cannot set the
/// number.
fn ns_per_op<F: FnMut() -> u64>(mut op: F) -> f64 {
    const SAMPLES: usize = 7;
    const MIN_BATCH: Duration = Duration::from_millis(20);

    // Warm up: first touch of a 200k-node jump table is a page-fault measurement, not an
    // algorithmic one.
    for _ in 0..1000 {
        black_box(op());
    }

    let mut samples: Vec<f64> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let mut n = 0u64;
        let start = Instant::now();
        loop {
            black_box(op());
            n += 1;
            // Check the clock rarely: at ~20ns/op, an Instant::now() per iteration would be most
            // of what is being timed.
            if n % 64 == 0 && start.elapsed() >= MIN_BATCH {
                break;
            }
        }
        samples.push(start.elapsed().as_nanos() as f64 / n as f64);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[SAMPLES / 2]
}

fn main() {
    let depths: Vec<u64> = std::env::var("D95_DEPTHS")
        .unwrap_or_else(|_| "10,100,1000,10000,100000".to_string())
        .split(',')
        .map(|s| s.trim().parse().expect("D95_DEPTHS must be a comma-separated list of integers"))
        .collect();
    assert!(!depths.is_empty(), "D95_DEPTHS collected nothing");

    let profile = if cfg!(debug_assertions) { "debug" } else { "release" };
    println!("# D95 ancestry curve. ferrodb {}", ferrodb::build_provenance());
    println!("# profile={profile}  arms=walk,index  two chains of depth D off one root");
    if profile == "debug" {
        println!("# ⚠ DEBUG BUILD — absolute numbers are meaningless; re-run with --release.");
    }
    println!(
        "{:>8}  {:>13}  {:>13}  {:>9}  {:>13}  {:>13}  {:>9}  {:>13}  {:>13}  {:>11}  {:>11}",
        "depth",
        "anc_walk_ns",
        "anc_index_ns",
        "anc_x",
        "lca_walk_ns",
        "lca_index_ns",
        "lca_x",
        "forkreap_ns",
        "depth1hash_ns",
        "walk_steps",
        "index_nodes",
    );

    let mut disagreements = 0u64;

    for (di, &d) in depths.iter().enumerate() {
        // ---- build both arms over the same tree -------------------------------------------
        // Node 0 is the root. Chain A is 1..=d, chain B is d+1..=2d.
        let n = (2 * d + 1) as usize;
        let mut walk = Walk { parent: vec![NO_PARENT; n], depth: vec![0; n] };
        let mut g = VersionGraph::new();
        let bid = |i: u64| BranchId::new(i, 0);
        g.insert_root(bid(0)).expect("root");

        for i in 1..=d {
            walk.parent[i as usize] = (i - 1) as u32;
            walk.depth[i as usize] = i as u32;
            g.insert_child(bid(i), bid(i - 1)).expect("chain A");
        }
        for j in 1..=d {
            let id = d + j;
            let par = if j == 1 { 0 } else { id - 1 };
            walk.parent[id as usize] = par as u32;
            walk.depth[id as usize] = j as u32;
            g.insert_child(bid(id), bid(par)).expect("chain B");
        }

        let leaf_a = d;
        let leaf_b = 2 * d;

        // ---- agreement gate, BEFORE timing anything ---------------------------------------
        //
        // A run whose arms disagree is not a slow-vs-fast comparison, it is two different
        // programs. Check first, refuse rather than report.
        let (w_anc, w_anc_steps) = walk.is_ancestor(0, leaf_a as u32);
        let i_anc = g.is_ancestor(bid(0), bid(leaf_a)).expect("index is_ancestor");
        let (w_lca, w_lca_steps) = walk.lca(leaf_a as u32, leaf_b as u32);
        let i_lca = g.lca(bid(leaf_a), bid(leaf_b)).expect("index lca");
        let i_lca_id = i_lca.map(|b| b.id);

        if w_anc != i_anc || w_lca.map(|x| x as u64) != i_lca_id {
            eprintln!(
                "DISAGREEMENT at depth {d}: is_ancestor walk={w_anc} index={i_anc}; \
                 lca walk={w_lca:?} index={i_lca_id:?}"
            );
            disagreements += 1;
            continue;
        }
        assert!(w_anc, "depth {d}: the root must be an ancestor of its own chain's leaf");
        assert_eq!(w_lca, Some(0), "depth {d}: the two chains must meet at the root");

        // ---- timing, arm order alternated per depth ---------------------------------------
        let (anc_walk, anc_index, lca_walk, lca_index);
        if di % 2 == 0 {
            anc_walk = ns_per_op(|| walk.is_ancestor(0, leaf_a as u32).1);
            anc_index =
                ns_per_op(|| g.is_ancestor(bid(0), bid(leaf_a)).map(u64::from).unwrap_or(0));
            lca_walk = ns_per_op(|| walk.lca(leaf_a as u32, leaf_b as u32).1);
            lca_index =
                ns_per_op(|| g.lca(bid(leaf_a), bid(leaf_b)).ok().flatten().map(|b| b.id).unwrap_or(0));
        } else {
            anc_index =
                ns_per_op(|| g.is_ancestor(bid(0), bid(leaf_a)).map(u64::from).unwrap_or(0));
            anc_walk = ns_per_op(|| walk.is_ancestor(0, leaf_a as u32).1);
            lca_index =
                ns_per_op(|| g.lca(bid(leaf_a), bid(leaf_b)).ok().flatten().map(|b| b.id).unwrap_or(0));
            lca_walk = ns_per_op(|| walk.lca(leaf_a as u32, leaf_b as u32).1);
        }

        // ---- what the index costs on the WRITE path ---------------------------------------
        //
        // The obvious objection to any query index is that it was paid for at insert time. A
        // fork here builds a jump table of `log2(depth)` entries, so this must come out
        // logarithmic too — if it were linear the index would simply have moved the wall from
        // ancestry queries onto `fork`, which is the operation this whole project keeps O(1).
        //
        // Insert-then-reap so the graph neither grows without bound across samples nor drifts to
        // a different size between depths, and so the free list is exercised the way a real
        // fork/reap cycle exercises it. The leaf is childless, so the reap really does remove it.
        let mut churn_id = 1_000_000_000u64;
        let fork_reap = ns_per_op(|| {
            churn_id += 1;
            let id = bid(churn_id);
            g.insert_child(id, bid(leaf_a)).expect("churn fork");
            g.reap(id).expect("churn reap") as u64
        });
        assert_eq!(g.len(), n, "depth {d}: churn must leave the graph the size it found it");

        // ---- the mechanism check for the shallow-depth crossover ---------------------------
        //
        // The index LOSES below ~depth 30, and the claimed reason is that the BranchId -> slot
        // hash lookup dominates: `is_ancestor` pays two of them, while the climb itself is only
        // popcount(delta) array indexings. That is a mechanism, so it gets measured rather than
        // asserted. `depth()` is exactly one hash lookup and no climb, so if the claim holds,
        // 2 x this should account for most of `anc_index_ns` at every depth.
        let depth_hash = ns_per_op(|| g.depth(bid(leaf_a)).map(u64::from).unwrap_or(0));

        println!(
            "{:>8}  {:>13.1}  {:>13.1}  {:>9.1}  {:>13.1}  {:>13.1}  {:>9.1}  {:>13.1}  {:>13.1}  {:>11}  {:>11}",
            d,
            anc_walk,
            anc_index,
            anc_walk / anc_index,
            lca_walk,
            lca_index,
            lca_walk / lca_index,
            fork_reap,
            depth_hash,
            w_anc_steps.max(w_lca_steps),
            g.len(),
        );
    }

    if disagreements > 0 {
        eprintln!("# REFUSING: {disagreements} depth(s) had arms that disagreed.");
        std::process::exit(1);
    }
    println!("# rc=0");
}
