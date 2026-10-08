//! D221: what one steady-state lease pass costs, against the number of live arenas.
//!
//!   d221_lease_pass [N,N,...] [fork threads]
//!
//! For each N, cumulatively: fork branches until N of them are live, each owning one EMPTY
//! extent. Then build a fresh reaper over the store and start a `LeaseThread` on it, the way a
//! restart does, and watch it until ONE due pass of the orphan-sweep cadence has run. That is the
//! first pass at least `ORPHAN_SWEEP_INTERVAL_MS` (60 s, wall clock on a standalone node) after
//! the open stamped it (D209). Each checkpoint therefore costs at least a minute, by construction.
//!
//! **The claim is a counter**: `due_visits`, the arenas that one due pass examined. An empty
//! extent of a live owner is never collectable, so every visit here is pure cost and nothing is
//! freed. Before D221 a due pass is the full scan, so `due_visits` = the live arena count. After
//! it, a due pass visits the residue (zero here, since nothing failed) plus a fixed slice.
//! `due_pass_ms` is a wall-clock illustration at 1 ms polling resolution, not a result.
//!
//! The catalog is the shipped `TableBranchCatalog`, because a visit is one of its descents. The
//! workload never writes a page, so the data file stays empty.
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::lease_thread::{LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

/// The cadence's interval, restated because the constant is crate-private. The run checks it
/// rather than trusting it: a due pass observed before this much time has passed is a failure.
const INTERVAL: Duration = Duration::from_secs(60);
/// The lease thread's scan interval here. Short, so the due pass follows the interval closely;
/// every pass that is not due returns at the gate and visits nothing.
const SCAN: Duration = Duration::from_millis(500);
/// The window closes before a SECOND due pass could run, which is 2 x INTERVAL after the stamp.
const WINDOW: Duration = Duration::from_secs(115);

/// Nothing expires in this workload, so no scan takes the lock; `start`'s resume is the only
/// caller.
struct NoGate;

impl RuntimeLock for NoGate {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        body();
    }
}

/// Deletes the run's directory however the run ends.
struct RemoveOnDrop(std::path::PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn main() {
    let ns: Vec<usize> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1000,4000,16000,64000".into())
        .split(',')
        .map(|s| s.trim().parse().expect("N must be an integer"))
        .collect();
    let threads: usize = std::env::args().nth(2).map(|s| s.parse().expect("threads")).unwrap_or(8);
    assert!(!ns.is_empty() && threads > 0, "usage: d221_lease_pass [N,N,...] [fork threads]");

    let dir = std::env::temp_dir().join(format!("ferrodb-d221-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let _cleanup = RemoveOnDrop(dir.clone());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.join("main.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Arc::new(TableBranchCatalog::open_sidecar(&dir.join("b.branchcat"), 1).unwrap());
    let base = bp.disk_manager.high_water().unwrap() + 1024;
    let store = Arc::new(
        ArenaPageStore::new(bp, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap(),
    );
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .unwrap(),
    );

    println!(
        "D221 lease-pass arm: fork threads {threads}, scan interval {SCAN:?}, cadence interval \
         {INTERVAL:?} (restated), window {WINDOW:?}"
    );
    println!(
        "  LEASE_PASS        N     live   open_visits   first_pass_visits   due_visits   \
         due_pass_ms   due_at_s   fork_s"
    );
    let mut failures: Vec<String> = Vec::new();
    let mut have = 0usize;
    for &n in &ns {
        assert!(n >= have, "checkpoints must not decrease: {n} after {have}");
        let t_fork = Instant::now();
        let need = n - have;
        std::thread::scope(|s| {
            for t in 0..threads {
                let share = need / threads + usize::from(t < need % threads);
                let catalog = Arc::clone(&catalog);
                let store = Arc::clone(&store);
                s.spawn(move || {
                    for _ in 0..share {
                        let b = catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
                        store.arena_for(b.branch_id).unwrap();
                    }
                });
            }
        });
        have = n;
        let fork_s = t_fork.elapsed().as_secs_f64();
        let live = store.live_arenas().len() as u64;

        let reaper = Arc::new(TwoTierReaper::new(
            Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
            Arc::clone(&store),
        ));
        let t_start = Instant::now();
        let lease = LeaseThread::start(
            Arc::clone(&reaper),
            Arc::clone(&runtime),
            Arc::new(NoGate) as Arc<dyn RuntimeLock>,
            SCAN,
        )
        .unwrap();
        let open_visits = reaper.open_sweep_visits();
        while lease.stats().finished == 0 && t_start.elapsed() < WINDOW {
            std::thread::sleep(Duration::from_millis(1));
        }
        let after_first = reaper.sweep_visits();
        let first_pass_visits = after_first - open_visits;

        // Poll until a finished pass has moved `sweep_visits`: that pass was the due one.
        let mut due: Option<(u64, Duration, Duration)> = None;
        let mut last = lease.stats();
        let mut last_visits = after_first;
        let mut attempt_at = Instant::now();
        while t_start.elapsed() < WINDOW {
            std::thread::sleep(Duration::from_millis(1));
            let s = lease.stats();
            if s.attempts > last.attempts {
                attempt_at = Instant::now();
            }
            if s.finished > last.finished {
                let v = reaper.sweep_visits();
                if v > last_visits {
                    due = Some((v - last_visits, attempt_at.elapsed(), t_start.elapsed()));
                    break;
                }
                last_visits = v;
            }
            last = s;
        }
        let stats = lease.stop();

        let (due_visits, due_ms, due_at) = match due {
            Some((v, took, at)) => (v, took.as_secs_f64() * 1e3, at.as_secs_f64()),
            None => (0, f64::NAN, f64::NAN),
        };
        println!(
            "  LEASE_PASS {n:>8} {live:>8} {open_visits:>13} {first_pass_visits:>19} \
             {due_visits:>12} {due_ms:>13.1} {due_at:>10.1} {fork_s:>8.1}"
        );

        if open_visits != live {
            failures.push(format!(
                "N={n}: the open swept {open_visits} arenas and the store holds {live}"
            ));
        }
        if first_pass_visits != 0 {
            failures.push(format!(
                "N={n}: the first pass visited {first_pass_visits} arenas; D209 is not on this tree"
            ));
        }
        match due {
            None => failures.push(format!(
                "N={n}: no due pass within {WINDOW:?} (stats {stats:?})"
            )),
            Some((_, _, at)) if at < INTERVAL => failures.push(format!(
                "N={n}: a pass swept {:.1} s after the open, inside the {INTERVAL:?} interval",
                at.as_secs_f64()
            )),
            Some(_) => {}
        }
        if stats.refused_scans != 0 || stats.failed != 0 || stats.reaped != 0 {
            failures.push(format!(
                "N={n}: the lease thread did something other than scan: {stats:?}"
            ));
        }
    }

    println!();
    if failures.is_empty() {
        println!(
            "GUARDS: every guard held. due_visits stands as printed; due_pass_ms is illustrative."
        );
    }
    for f in &failures {
        println!("NOT A RESULT: {f}");
    }
    if !failures.is_empty() {
        std::process::exit(2);
    }
}
