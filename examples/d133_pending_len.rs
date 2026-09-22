//! D133 — how long does the arena PENDING-FREE queue get, and how often is it walked?
//!
//! D133 read from structure that `reap → drain_pending_seeded` walks the **entire global**
//! pending-free log inside the pgwire per-statement lock, and said so with the limit stated in the
//! same breath: *"`pending`'s actual length is UNMEASURED and I am not asserting it is large."*
//! This is that measurement.
//!
//! # What is on the statement path, and where this harness enters it
//!
//! `lease_thread.rs:569` chunks expired candidates at `REAP_CHUNK = 1` and calls
//! `reaper.reap_if_still_expired(..)` inside `with_lock` (`lease_thread.rs:572`) — the pgwire
//! per-statement mutex. `reap_if_still_expired` (`reaper.rs:777`) ends in `self.reap(branch)`.
//! So **one lock acquisition == one `Reaper::reap`**, and this harness calls `Reaper::reap` once
//! per branch in a loop. It is the same call, reached through the trait rather than through the
//! timer, so no clock or lease has to be faked.
//!
//! Inside one `reap`, the work whose size is NOT bounded by the chunk:
//!   - `arena.rs:940` parks every page of the reaped branch a live child can still see.
//!   - `reaper.rs:410` `drain_pending_seeded` then loops: `take_pending` is `std::mem::take` of
//!     the WHOLE vec (`arena.rs:800`), walks every entry, and `put_pending`s the survivors back
//!     (`arena.rs:810`) to be re-walked by the next reap, for as long as their child lives.
//!
//! # The two integers, and why neither is reported alone
//!
//! `pending_len()` (`arena.rs:575`) is the queue's LENGTH. `drain_passes()` / `drain_entry_visits()`
//! (`reaper.rs`, added for this measurement beside the existing `sweep_descents`/`sweep_visits`)
//! are how many times it was walked and how many entries those walks touched. A length alone
//! cannot separate "short queue walked often" from "long queue walked once". `entry_visits` is the
//! work; `pending_len` is the state; `ms` is the confirmation, and on this box it is an upper
//! bound and nothing better.
//!
//! # ⛔ `checkpoint_path` GATES THE COST, so every row states which setting produced it
//!
//! `put_pending` closes with `persist_if_configured()` (`arena.rs:799-803`), which is
//! `None => Ok(())`. **Production sets it**: `cli/cli.rs:120` calls
//! `store.checkpoint_to(arena_path)` unconditionally when the server opens. The published 10^6
//! harnesses did not. So `persist=off` is what this project has historically measured and
//! `persist=on` is what the shipped binary runs — both are here, and the header of every row says
//! which. Following D99's second correction, `checkpoint_to` is called **after** padding, so
//! setup's own extent claims do not pay for a persistence the measurement, not the fixture, is
//! about.
//!
//! # Shapes, because "a branching workload" is not one workload
//!
//! A page is parked exactly when a *reaped* branch has a *live* child that can see it
//! (`arena.rs:1585-1600`). So what grows `pending` is not the branch count — it is the count of
//! reaped-but-pinned branches, and the shape of the tree decides whether those are the same
//! number:
//!
//!   `fan`   N parents off trunk, each with P pages written BEFORE it forks one child that is
//!           never reaped. Every parent's reap parks P pages. This is the shape in which pending
//!           CAN grow with N.
//!   `chain` trunk -> b1 -> ... -> bN, reaped deepest-first with the tip left alive — which is the
//!           order `expired_candidates` produces. Only the branch adjacent to the live tip has a
//!           live child, so at most one branch's pages are parked at a time.
//!   `leaf`  N childless leaves. **The forced negative.** Nothing can be parked, so `pending_len`,
//!           `drain_passes` and `drain_entry_visits` must all read ZERO. A counter that is never
//!           zero is not measuring what its name says.
//!
//! Refuses rather than reporting: zero branches, zero rows, a `fan`/`chain` row that parked
//! nothing (the fixture missed the code under test), or a `leaf` row that parked anything.
//!
//! Usage:
//!   d133_pending_len
//!   D133_SHAPES=fan D133_BRANCHES=100,300,1000,3000 D133_PERSIST=on d133_pending_len

use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::DiskManager;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn list(key: &str, default: &str) -> Vec<String> {
    env_or(key, default).split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

struct Rig {
    store: Arc<ArenaPageStore>,
    cat: Arc<dyn BranchCatalog>,
    reaper: TwoTierReaper,
    dir: std::path::PathBuf,
}

fn build(tag: &str, catalog_kind: &str) -> Rig {
    let dir = std::env::temp_dir().join(format!("ferrodb-d133-{}-{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(dir.join("main.db"))
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
    let cat: Arc<dyn BranchCatalog> = if catalog_kind == "table" {
        Arc::new(TableBranchCatalog::open_sidecar(&dir.join("b.branchcat"), 1).unwrap())
    } else {
        Arc::new(LogBranchCatalog::in_memory(1))
    };
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&cat), base).unwrap());
    let reaper = TwoTierReaper::new(Arc::clone(&cat), Arc::clone(&store));
    Rig { store, cat, reaper, dir }
}

/// Write `pages` novel pages to `branch`, the way an agent task does.
///
/// `alloc_for`, not a captured `ArenaId`: under geometric extent growth the first extent is ONE
/// page, so a harness that takes one arena up front and fills it refuses on the second write.
fn write_pages(rig: &Rig, branch: BranchId, pages: u32) {
    let epoch = rig.cat.next_epoch();
    for i in 0..pages {
        let p = rig.store.alloc_for(branch, PageType::BTreeLeaf, epoch).unwrap();
        let handle = rig.store.read_page(p).unwrap();
        let mut frame = handle.write();
        frame.data[PAGE_HEADER_SIZE] = (i & 0xff) as u8;
        stamp_checksum(&mut frame.data);
    }
}

/// Build the fixture and return the branches to reap, **in the order the lease thread would**.
fn pad(rig: &Rig, shape: &str, n: usize, pages: u32) -> Vec<BranchId> {
    match shape {
        // N parents off trunk. Pages are written BEFORE the child forks, which is what makes them
        // visible to it and therefore parkable; a page born after the fork goes straight back.
        "fan" => (0..n)
            .map(|_| {
                let p = rig.cat.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
                write_pages(rig, p.branch_id, pages);
                let _child = rig.cat.fork(p.branch_id, LeaseDeadline(0)).unwrap();
                p.branch_id
            })
            .collect(),
        // trunk -> b1 -> ... -> bN. The tip is never reaped, so it is the live child that pins.
        // Returned deepest-first excluding the tip, which is `expired_candidates`' own order.
        "chain" => {
            let mut prev = BranchId::TRUNK;
            let mut all = Vec::with_capacity(n);
            for _ in 0..n {
                let b = rig.cat.fork(prev, LeaseDeadline(0)).unwrap();
                write_pages(rig, b.branch_id, pages);
                prev = b.branch_id;
                all.push(b.branch_id);
            }
            all.pop(); // the tip stays alive
            all.reverse();
            all
        }
        // The forced negative: childless leaves park nothing, so every counter must stay at zero.
        "leaf" => (0..n)
            .map(|_| {
                let b = rig.cat.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
                write_pages(rig, b.branch_id, pages);
                b.branch_id
            })
            .collect(),
        other => panic!("unknown shape {other}"),
    }
}

fn main() {
    let shapes = list("D133_SHAPES", "fan,chain,leaf");
    let counts: Vec<usize> =
        list("D133_BRANCHES", "100,300,1000,3000").iter().filter_map(|s| s.parse().ok()).collect();
    let persists = list("D133_PERSIST", "off,on");
    let pages: u32 = env_or("D133_PAGES", "4").parse().unwrap();
    let catalog_kind = env_or("D133_CATALOG", "mem");

    if counts.is_empty() || shapes.is_empty() || persists.is_empty() {
        eprintln!("REFUSED: empty sweep — nothing would be measured.");
        std::process::exit(2);
    }

    println!("D133 — pending-free queue length and how often it is walked.");
    println!();
    println!("  catalog={catalog_kind}  pages written per branch before its child forks={pages}");
    println!("  reap loop = `Reaper::reap` once per branch, which is what `lease_thread.rs:572`");
    println!("  calls inside the per-statement lock at REAP_CHUNK=1 (via `reap_if_still_expired`).");
    println!("  `checkpoint_to` is called AFTER padding, so setup does not pay for persistence.");
    println!("  pending_len is sampled inside the timed loop (one O(1) mutex read per reap).");
    println!();
    println!(
        "  {:<6} {:<8} {:>7} {:>9} {:>9} {:>8} {:>14} {:>10} {:>11}",
        "shape",
        "persist",
        "N",
        "pend_end",
        "pend_peak",
        "passes",
        "entry_visits",
        "total_ms",
        "ms/reap"
    );

    let mut rows = 0usize;
    let mut refusals: Vec<String> = Vec::new();

    for shape in &shapes {
        for persist in &persists {
            for &n in &counts {
                if n == 0 {
                    refusals.push("a row with N=0 was requested".to_string());
                    continue;
                }
                let tag = format!("{shape}-{persist}-{n}");
                let rig = build(&tag, &catalog_kind);
                let victims = pad(&rig, shape, n, pages);

                // AFTER padding — D99's second correction. Turning this on earlier makes every
                // setup extent claim rewrite the whole map, which is O(N^2) bytes of fixture.
                let persist_on = persist == "on";
                if persist_on {
                    rig.store.checkpoint_to(rig.dir.join("main.db.arena"));
                }

                let mut peak = 0usize;
                let t0 = Instant::now();
                for b in &victims {
                    rig.reaper.reap(*b).unwrap();
                    let l = rig.store.pending_len();
                    if l > peak {
                        peak = l;
                    }
                }
                let ms = t0.elapsed().as_secs_f64() * 1000.0;

                let pend_end = rig.store.pending_len();
                let passes = rig.reaper.drain_passes();
                let visits = rig.reaper.drain_entry_visits();
                let reaped = victims.len();

                println!(
                    "  {:<6} {:<8} {:>7} {:>9} {:>9} {:>8} {:>14} {:>10.1} {:>11.4}",
                    shape,
                    persist,
                    reaped,
                    pend_end,
                    peak,
                    passes,
                    visits,
                    ms,
                    ms / reaped as f64
                );
                rows += 1;

                // The detector must fire where the mechanism exists, and must NOT fire where it
                // does not. Either failure makes every other row in this table uninterpretable.
                match shape.as_str() {
                    "leaf" => {
                        if peak != 0 || visits != 0 {
                            refusals.push(format!(
                                "leaf N={n} parked {peak} entries and walked {visits}: a childless \
                                 leaf has no live child, so the counters are measuring something \
                                 other than the pinned-page path"
                            ));
                        }
                    }
                    _ => {
                        if peak == 0 {
                            refusals.push(format!(
                                "{shape} N={n} parked NOTHING — the fixture never reached the code \
                                 under test, so its ms and its zeros mean nothing"
                            ));
                        }
                        if visits == 0 {
                            refusals.push(format!(
                                "{shape} N={n} walked zero pending entries while pending_len \
                                 reached {peak}"
                            ));
                        }
                    }
                }
                let _ = std::fs::remove_dir_all(&rig.dir);
            }
        }
    }

    println!();
    if rows == 0 {
        eprintln!("REFUSED: zero rows produced.");
        std::process::exit(2);
    }
    if !refusals.is_empty() {
        eprintln!("REFUSED — {} row(s) did not hold up:", refusals.len());
        for r in &refusals {
            eprintln!("  - {r}");
        }
        std::process::exit(2);
    }
    println!("  {rows} rows, every fan/chain row parked pages and every leaf row parked none.");
}
