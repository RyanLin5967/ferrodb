//! D144 — let the LEASE produce the lag, and look for the knee at the scan interval.
//!
//! D142 parameterised the lag directly (`fan` = infinite, `fanlag` = one reap, `fanreap` = zero)
//! and said its three arms **bracket** the real lag and do not **locate** it. D144 located it from
//! source. This measures it, and nothing here fakes a clock or calls `reap`: the real
//! [`LeaseThread`] runs on a real interval, real `LeaseDeadline::from_now` deadlines expire on
//! their own, and the lag is produced by the wall-clock gap between forking a parent and forking
//! its child.
//!
//! # The derivation being tested
//!
//! A branch's deadline is fixed AT FORK (`runtime.rs:1477`, `LeaseDeadline::from_now`), and
//! nothing on the agent path extends it. For a parent forked at `t₀` and a child at `t₀ + Δ`,
//! with the same lease `L`:
//!
//!     parent expires   t₀ + L
//!     child expires    t₀ + Δ + L
//!     window in which the parent is EXPIRED and the child is LIVE  =  exactly Δ
//!
//! A parent reaped inside that window has a live child, so it takes the slow path and PARKS its
//! pages. Reaped outside it, both are expired at the same sweep, `expired_candidates`' deepest-
//! first sort (`reaper.rs:749-751`) reaps the child first, and the parent is a childless leaf that
//! parks nothing.
//!
//! ⭐ **`L` does not appear in the window.** It sets only WHEN the window opens. That is why this
//! harness may use a compressed `L` instead of the shipped `DEFAULT_LEASE_MILLIS` (15 min,
//! `runtime.rs:96`, with no environment override), and why the `lease` control arm below varies
//! `L` at a fixed `Δ` and must NOT move. If it moves, this paragraph is wrong.
//!
//! # ⛔ TWO PREDICTIONS, PRE-REGISTERED, AND THEY DISAGREE BELOW THE INTERVAL
//!
//! **P1 — D144 as written: a STEP at `Δ = S`.** Zero parked below, everything parked above.
//!
//! **P2 — a refinement derived from the same three constants.** The parent is reaped at the FIRST
//! sweep after its deadline. That sweep lands at `t₀ + L + u` where the phase `u` is uniform on
//! `[0, S)` — which is why this harness spreads the parent forks across exactly one interval
//! rather than firing them together. The parent parks iff `u < Δ`. So over many pairs:
//!
//!     parked fraction  =  min(Δ / S, 1)
//!
//! a linear **RAMP** over `[0, S]` and then a **PLATEAU**, i.e. a KNEE at `Δ = S` rather than a
//! step. P1 and P2 agree above `S` and disagree below it, so the sweep below `S` is what
//! discriminates them. **P2 is not the safe answer: it says D144's reassuring "zero below" is
//! wrong, and that a fork gap of half an interval already parks half the parents.**
//!
//! Both are printed beside every measured row. Whichever loses, the row says so.
//!
//! # Why the IN-MEMORY catalog here, when D142's cross-check used the shipped one
//!
//! `REAP_CHUNK`'s own doc comment measures a reap on the durable catalog at **~58 ms, fsync-bound**
//! (`lease_thread.rs`). A point here reaps 120 branches; on the shipped catalog one sweep would
//! take ~7 s against an `S` of 3 s, so sweeps would run back-to-back, the cadence would no longer
//! be `S`, and the uniform-phase argument P2 rests on would be false — the harness would be
//! measuring fsync latency wearing the costume of a lag experiment. With `LogBranchCatalog` a
//! sweep completes in milliseconds and the cadence really is `S`. D142's ARM 2 established that
//! the two catalogs agree **exactly** on every integer, which is what makes this substitution safe
//! for a COUNT and would not make it safe for a duration. No duration is claimed here.
//!
//! # What is asserted about the instrument before any row is believed
//!
//! A lease thread that never ran, or that refused because it could not read the cluster's time
//! (`lease_thread.rs:537`, `LeaseDeadline::try_now_millis`), produces `pending == 0` — which is
//! indistinguishable from "nothing parked" and would confirm P1 spuriously. So every row refuses
//! unless `attempts > 0`, `refused == 0`, `failed == 0` and `reaped > 0`.
//!
//! Usage: FERRODB_LEASE_SCAN_MILLIS=3000 d144_lease_lag

use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::lease_thread::{scan_interval_from_env, LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::record::BranchRecord;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::error::FerroError;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// How often `pending_len()` is read. One O(1) mutex read; fast enough that a park and the
/// release that follows it (at least one interval later) cannot cancel between two samples.
const SAMPLE_MS: u64 = 10;

/// ⛔ **Why the parked count is `peak / pages` and NOT a sum of the queue's positive increments.**
///
/// The increment sum was tried first and is WRONG, caught by a smoke run that reported **12**
/// parents parked out of **10** pairs. `drain_pending_seeded` is a read-modify-write:
/// `take_pending` is `std::mem::take` of the whole vec (`arena.rs:800`), so `pending_len()` drops
/// to **zero** for the length of the walk, and `put_pending` then restores the survivors
/// (`arena.rs:810`). A sampler that lands inside that window sees the restore as a FRESH park and
/// counts it again — the queue's own internal transient read as new work.
///
/// `peak` is immune: a transient dip cannot raise a maximum. And peak == total here because every
/// parent expires within one interval of every other and a parked entry outlives at least one
/// sweep, so the parked intervals overlap. That is a property of this fixture, not a general one.

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

struct Rig {
    store: Arc<ArenaPageStore>,
    cat: Arc<dyn BranchCatalog>,
    reaper: Arc<TwoTierReaper>,
    runtime: Arc<AgentRuntime>,
    ctx: Arc<ServerContext>,
    dir: std::path::PathBuf,
}

fn build(tag: &str) -> Rig {
    let dir = std::env::temp_dir().join(format!("ferrodb-d144-{}-{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(dir.join("main.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.join("main.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat: Arc<dyn BranchCatalog> = Arc::new(LogBranchCatalog::in_memory(1));
    let base = bp.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(bp.clone(), Arc::clone(&cat), base).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            Arc::clone(&cat),
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, Arc::clone(&runtime)));
    let reaper = Arc::new(TwoTierReaper::new(Arc::clone(&cat), Arc::clone(&store)));
    Rig { store, cat, reaper, runtime, ctx, dir }
}

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

/// Fork (and fill) a branch **under the pgwire statement lock**, which is what a real fork holds.
///
/// `BEGIN AGENT SESSION` arrives as a statement, and `lease_thread.rs:572` runs its reaps under
/// the same mutex. Forking outside it would let a fork and a sweep overlap in a way production
/// forbids, and would make this harness's contention structure the one thing it is not allowed to
/// get wrong. `with_runtime_lock` is the trait's own entry point; the `Option` dance is what
/// `lease_thread::with_lock` does privately for the same reason.
fn fork_locked(
    rig: &Rig,
    parent: BranchId,
    lease_ms: u64,
    pages: u32,
) -> Result<BranchRecord, FerroError> {
    let mut out: Option<Result<BranchRecord, FerroError>> = None;
    rig.ctx.with_runtime_lock(&mut || {
        out = Some((|| {
            let rec = rig.cat.fork(parent, LeaseDeadline::from_now(lease_ms))?;
            if pages > 0 {
                write_pages(rig, rec.branch_id, pages);
            }
            Ok(rec)
        })());
    });
    out.expect("a RuntimeLock implementation must run its body exactly once")
}

struct Row {
    delta_ms: u64,
    lease_ms: u64,
    peak: usize,
    parked_parents: usize,
    /// Diagnostic only — see the note on [`SAMPLE_MS`]. Above `parked_parents` means a sample
    /// landed inside a drain's take/put window.
    increment_sum_parents: usize,
    realised_delta_ms: (u64, u64),
    stats: ferrodb::branch::lease_thread::LeaseStats,
}

/// One point: fork `pairs` parent/child pairs whose forks are `delta_ms` apart, let the real lease
/// thread reap them, and watch the pending-free queue.
fn run_point(
    delta_ms: u64,
    lease_ms: u64,
    pairs: usize,
    pages: u32,
    scan: Duration,
    refusals: &mut Vec<String>,
) -> Row {
    let rig = build(&format!("d{delta_ms}-l{lease_ms}"));
    let lock: Arc<dyn RuntimeLock> = Arc::clone(&rig.ctx) as Arc<dyn RuntimeLock>;
    let lease = LeaseThread::start(
        Arc::clone(&rig.reaper),
        Arc::clone(&rig.runtime),
        lock,
        scan,
    )
    .expect("lease thread");

    let scan_ms = scan.as_millis() as u64;
    // Parent forks spread across EXACTLY one interval, so the phase of the first sweep after each
    // parent's deadline is uniform on [0, S). Firing them together would sample one phase and
    // turn a ramp into a coin flip.
    let stagger_ms = scan_ms as f64 / pairs as f64;

    // (due_ms, is_child, index)
    let mut events: Vec<(u64, bool, usize)> = Vec::with_capacity(pairs * 2);
    for i in 0..pairs {
        let t = (i as f64 * stagger_ms) as u64;
        events.push((t, false, i));
        events.push((t + delta_ms, true, i));
    }
    events.sort_by_key(|e| e.0);

    let mut parents: Vec<Option<BranchId>> = vec![None; pairs];
    let mut parent_deadline: Vec<u64> = vec![0; pairs];
    let mut realised: Vec<u64> = Vec::with_capacity(pairs);

    // Stop once every parent is reaped and the queue has had a couple of sweeps to show itself.
    // Not once the children drain: the peak is what is being measured.
    let end_ms = scan_ms + delta_ms + lease_ms + 4 * scan_ms;

    let t0 = Instant::now();
    let mut peak = 0usize;
    let mut prev = 0usize;
    let mut increments = 0usize;
    let mut next_event = 0usize;
    loop {
        let now_ms = t0.elapsed().as_millis() as u64;
        while next_event < events.len() && events[next_event].0 <= now_ms {
            let (_, is_child, i) = events[next_event];
            if !is_child {
                let rec = fork_locked(&rig, BranchId::TRUNK, lease_ms, pages).unwrap();
                parent_deadline[i] = rec.lease_deadline.0;
                parents[i] = Some(rec.branch_id);
            } else if let Some(p) = parents[i] {
                // The parent may already be reaped if delta >= lease; that is refused up front.
                match fork_locked(&rig, p, lease_ms, 0) {
                    Ok(c) => realised.push(c.lease_deadline.0.saturating_sub(parent_deadline[i])),
                    Err(e) => refusals.push(format!(
                        "delta={delta_ms} lease={lease_ms}: child fork {i} failed ({e}) — the \
                         parent was gone before its child existed, so this row measures a \
                         different experiment than the one described"
                    )),
                }
            }
            next_event += 1;
        }

        let l = rig.store.pending_len();
        if l > peak {
            peak = l;
        }
        // Kept only as a DIAGNOSTIC upper bound; see the note on SAMPLE_MS for why it is not the
        // reported figure. It exceeding `peak` is the signature of a sample landing mid-drain.
        if l > prev {
            increments += l - prev;
        }
        prev = l;

        if now_ms >= end_ms {
            break;
        }
        // Derived from elapsed time, not incremented: a loop body that overran SAMPLE_MS (a
        // fork plus its page writes does) would otherwise leave `target` permanently in the past
        // and turn this into a busy spin that competes with the lease thread it is watching.
        let tick = now_ms / SAMPLE_MS + 1;
        let target = t0 + Duration::from_millis(tick * SAMPLE_MS);
        let now = Instant::now();
        if target > now {
            std::thread::sleep(target - now);
        }
    }

    let stats = lease.stop();
    let _ = std::fs::remove_dir_all(&rig.dir);

    // A lease thread that never ran, or refused, yields pending == 0 — which reads exactly like
    // "nothing parked" and would confirm P1 for the wrong reason.
    if stats.attempts == 0 {
        refusals.push(format!("delta={delta_ms} lease={lease_ms}: the lease thread never scanned"));
    }
    if stats.refused > 0 {
        refusals.push(format!(
            "delta={delta_ms} lease={lease_ms}: {} scan(s) REFUSED — the node could not read the \
             cluster's time, so a zero here means nothing was asked, not that nothing parked",
            stats.refused
        ));
    }
    if stats.failed > 0 {
        refusals.push(format!(
            "delta={delta_ms} lease={lease_ms}: {} scan(s) FAILED",
            stats.failed
        ));
    }
    if stats.reaped == 0 {
        refusals.push(format!(
            "delta={delta_ms} lease={lease_ms}: the lease thread reaped NOTHING in {end_ms} ms \
             with a {lease_ms} ms lease — the fixture never reached the code under test"
        ));
    }
    if peak > pairs * pages as usize {
        refusals.push(format!(
            "delta={delta_ms} lease={lease_ms}: peak pending {peak} exceeds {pairs} pairs x \
             {pages} pages — more was parked than exists, so the metric is not measuring parks"
        ));
    }
    if realised.len() != pairs {
        refusals.push(format!(
            "delta={delta_ms} lease={lease_ms}: only {} of {pairs} children were forked",
            realised.len()
        ));
    }

    let lo = realised.iter().copied().min().unwrap_or(0);
    let hi = realised.iter().copied().max().unwrap_or(0);
    Row {
        delta_ms,
        lease_ms,
        peak,
        parked_parents: peak / pages as usize,
        increment_sum_parents: increments / pages as usize,
        realised_delta_ms: (lo, hi),
        stats,
    }
}

fn print_header(scan_ms: u64, lease_ms: u64, pairs: usize, pages: u32) {
    println!("D144 — the lag the LEASE produces, swept across the scan interval.");
    println!();
    println!("  scan interval S  {scan_ms} ms   (from scan_interval_from_env(), i.e. \
              FERRODB_LEASE_SCAN_MILLIS or the {} ms default)",
             ferrodb::branch::lease_thread::DEFAULT_SCAN_MILLIS);
    println!("  lease L          {lease_ms} ms  (COMPRESSED from DEFAULT_LEASE_MILLIS = {} ms, \
              which has no env override;", ferrodb::agent_sql::runtime::DEFAULT_LEASE_MILLIS);
    println!("                   L does not enter the window, which is exactly Δ — the `lease` \
              control arm tests that claim)");
    println!("  pairs per point  {pairs}, parent forks spread across exactly one S so the sweep \
              phase is uniform");
    println!("  pages per parent {pages}, written BEFORE the child forks so the child can see them");
    println!("  reaping          the REAL LeaseThread on a real clock. No faked time, no direct \
              reap() call.");
    println!();
    println!("  P1 = D144 as written: a STEP — 0 parked below S, all parked above.");
    println!("  P2 = refinement:      a KNEE — parked fraction = min(Δ/S, 1), so a RAMP below S.");
    println!();
    println!(
        "  {:>8} {:>8} {:>10} {:>9} {:>8} {:>8} {:>16} {:>15} {:>22} {:>7}",
        "Δ ms", "lease ms", "Δ/S", "parked", "P1", "P2", "peak pending", "realised Δ",
        "scans a/s/r/f/reaped", "incsum"
    );
}

fn print_row(r: &Row, scan_ms: u64, pairs: usize) {
    let ratio = r.delta_ms as f64 / scan_ms as f64;
    let p1 = if r.delta_ms > scan_ms { pairs } else { 0 };
    let p2 = (pairs as f64 * ratio.min(1.0)).round() as usize;
    println!(
        "  {:>8} {:>8} {:>10.3} {:>9} {:>8} {:>8} {:>16} {:>15} {:>22} {:>7}",
        r.delta_ms,
        r.lease_ms,
        ratio,
        r.parked_parents,
        p1,
        p2,
        r.peak,
        format!("{}..{}", r.realised_delta_ms.0, r.realised_delta_ms.1),
        format!(
            "{}/{}/{}/{}/{}",
            r.stats.attempts, r.stats.scans, r.stats.refused, r.stats.failed, r.stats.reaped
        ),
        r.increment_sum_parents,
    );
}

fn main() {
    let scan = scan_interval_from_env().expect("scan interval");
    let scan_ms = scan.as_millis() as u64;
    let lease_ms: u64 = env_or("D144_LEASE_MS", "20000").parse().unwrap();
    let pairs: usize = env_or("D144_PAIRS", "60").parse().unwrap();
    let pages: u32 = env_or("D144_PAGES", "4").parse().unwrap();
    let deltas: Vec<u64> = env_or(
        "D144_DELTAS",
        "0,500,1000,1500,2000,2500,3000,4500,6000,9000",
    )
    .split(',')
    .filter_map(|s| s.trim().parse().ok())
    .collect();
    let control_leases: Vec<u64> = env_or("D144_CONTROL_LEASES", "20000,40000")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let control_delta: u64 = env_or("D144_CONTROL_DELTA", &(scan_ms * 2).to_string())
        .parse()
        .unwrap();

    if deltas.is_empty() || pairs == 0 {
        eprintln!("REFUSED: empty sweep.");
        std::process::exit(2);
    }
    if let Some(bad) = deltas.iter().chain(std::iter::once(&control_delta)).find(|d| **d >= lease_ms)
    {
        eprintln!(
            "REFUSED: Δ={bad} ms is not less than the lease {lease_ms} ms. The parent would be \
             reaped before its child is forked, which is a different experiment."
        );
        std::process::exit(2);
    }

    print_header(scan_ms, lease_ms, pairs, pages);
    let mut refusals = Vec::new();
    let mut rows = 0usize;

    for &d in &deltas {
        let r = run_point(d, lease_ms, pairs, pages, scan, &mut refusals);
        print_row(&r, scan_ms, pairs);
        rows += 1;
    }

    println!();
    println!("  CONTROL — L varied at a fixed Δ={control_delta} ms. L is not in the window, so");
    println!("  these rows MUST agree. If they do not, the derivation above is wrong.");
    println!(
        "  {:>8} {:>8} {:>10} {:>9} {:>8} {:>8} {:>16} {:>15} {:>22} {:>7}",
        "Δ ms", "lease ms", "Δ/S", "parked", "P1", "P2", "peak pending", "realised Δ",
        "scans a/s/r/f/reaped", "incsum"
    );
    let mut control_parked = Vec::new();
    for &l in &control_leases {
        let r = run_point(control_delta, l, pairs, pages, scan, &mut refusals);
        print_row(&r, scan_ms, pairs);
        control_parked.push(r.parked_parents);
        rows += 1;
    }

    println!();
    println!("  realised Δ is computed from the DEADLINES the catalog recorded");
    println!("  (child.lease_deadline - parent.lease_deadline), not from the requested value.");

    if rows == 0 {
        eprintln!("REFUSED: zero rows.");
        std::process::exit(2);
    }
    if !refusals.is_empty() {
        eprintln!("REFUSED — {} problem(s):", refusals.len());
        for r in &refusals {
            eprintln!("  - {r}");
        }
        std::process::exit(2);
    }
    // The control is only a control if something checks it. L is not in the window, so varying
    // it at a fixed Δ must not move the parked count; a spread wider than a tenth of the
    // population means the window is a function of something this harness does not model.
    if control_parked.len() >= 2 {
        let lo = *control_parked.iter().min().unwrap();
        let hi = *control_parked.iter().max().unwrap();
        let tol = (pairs as f64 * 0.10).ceil() as usize;
        if hi - lo > tol {
            eprintln!(
                "REFUSED: the LEASE control MOVED — parked counts {control_parked:?} across \
                 leases {control_leases:?} at a fixed Δ={control_delta} ms, a spread of {} > {} \
                 tolerated. L is supposed not to enter the window; it does, so the derivation in \
                 this file's header is wrong and no row above should be quoted.",
                hi - lo,
                tol
            );
            std::process::exit(2);
        }
        println!("  CONTROL HELD: parked {control_parked:?} across leases {control_leases:?} — \
                  varying L did not move the window.");
    }
    println!("  {rows} rows, every lease thread scanned, none refused, none failed.");
}
