//! D219 — `MERGE` latency, and a concurrent reader's latency, against δ with DURABLE provenance.
//!
//! # Why this run exists
//!
//! With the durable provenance store (the CLI's shape), a `MERGE` of δ one-column updates pays two
//! fsyncs per op on the provenance file: the executor's physical `stamp` for each published
//! version, and `record_applied`'s `stamp_row` for each applied op — the second under the
//! runtime's `state` lock. No merge measurement in `bench/` ever ran against the durable store: the
//! one `stamp_row` in a profile is the in-memory store's. So the cost is unmeasured.
//!
//! # Instruments
//!
//! * **Syncs per `MERGE`, by kind** — `ProvenanceStore::sync_counts()` read immediately before and
//!   after the statement. Integers fixed by control flow, so load cannot move them. This is the
//!   primary result; the timings below are read against it.
//! * **Merge latency** — wall time of the `MERGE` statement, catalog acquisition included, with no
//!   reader running (phase A).
//! * **Reader stall** — per merge, the longest a concurrent reader spent inside that merge's window
//!   before its read returned: `max(r_end - max(r_start, m_start))` over reads overlapping it. The
//!   median over merges is reported, which a single scheduler hiccup cannot move.
//!   * phase B, **queued reader** — the shipped shape: pgwire's order exactly (`read_catalog`,
//!     `begin_read`, `try_run_read`, and the exclusive catalog when stood down). A writer holding the
//!     catalog stands every such reader down, so this reader waits for the WHOLE merge statement.
//!   * phase C, **state-lock reader** — a MECHANISM arm, not a shipped shape: an agent-session
//!     `SELECT` through `try_run_read` with no `begin_read` gate, so it blocks only where it takes
//!     `state`. This is the one arm in which `record_applied`'s hold is visible apart from the rest
//!     of the merge. No server runs reads this way today; the arm answers what the hold would cost
//!     one that did.
//! * **f** — the median `sync_data` of a 25-byte append on a file in the same directory, measured
//!   at the start and the end of the run, so a slope can be read in units of fsyncs.
//!
//! A control store arm (in-memory provenance, no file) runs the identical phases first; every
//! durable-arm slope is reported as `(durable - control) / f`, the excess slope in fsyncs per op.
//!
//! # Pre-registered (lane_d219_provenance_sync.md carries the same table and is the record)
//!
//! | quantity, per MERGE of δ one-column updates | before the fix | after |
//! |---|---|---|
//! | `row_authors` syncs | δ | 1 |
//! | `stamps` syncs | δ | δ (untouched by D219) |
//! | `runs`, `forgets` syncs | 0 | 0 |
//! | control arm, every kind | 0 | 0 |
//! | merge latency excess slope, fsyncs per op | ≈ 2 (band 1.4–2.6) | ≈ 1 (0.7–1.3) |
//! | queued-reader stall excess slope | ≈ the merge's (ratio 0.7–1.3) | ≈ the merge's |
//! | state-lock-reader stall excess slope | ≈ 1 (0.6–1.4) | ≈ 0 (−0.2–0.2) |
//!
//! The run REFUSES (exits non-zero) on: a durable-arm δ point whose sync counts match neither
//! column; counts that differ between repetitions of one point; any non-zero count on the control
//! arm; a merge that did not reach the target; a reader phase with no read overlapping a merge; a
//! reader that could not start. It prints everything and THEN refuses if any read returned an
//! error: errored reads are kept out of every stall, because one that fails fast would pull a stall
//! toward zero — the very shape phase C's AFTER prediction has.
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::{run, try_run_read, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::{Parser, Stmt};
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::provenance::SyncCounts;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const LADDER: [usize; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const REPS: usize = 15;
const CALIBRATION_SYNCS: usize = 200;
/// Every merge updates ids `1..=δ`, so the table holds the largest δ and nothing else.
const ROWS: usize = 128;
const MODEL: &str = "claude-opus-5/2026-05";

struct Server {
    ctx: Arc<ServerContext>,
    runtime: Arc<AgentRuntime>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

fn parse_one(sql: &str) -> Result<Stmt, String> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .map_err(|e| e.to_string())?;
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() || stmts.len() != 1 {
        return Err(format!("{sql} did not parse to one statement: {:?}", parser.errors));
    }
    Ok(stmts.remove(0))
}

/// One statement the way the server runs a write: take the catalog, run, drop.
fn exec(s: &Server, sql: &str, sess: &mut Session) -> Result<Outcome, String> {
    let stmt = parse_one(sql)?;
    let mut cat = s.ctx.catalog();
    let out = run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess);
    drop(cat);
    out.map_err(|e| format!("{sql}: {e}"))
}

/// The engine d67 measures — arena storage, a durable branch catalog, a checkpointed free-space
/// map — plus, on the durable arm, the provenance file `cli.rs` installs.
fn build(dir: &std::path::Path, tag: &str, durable: bool) -> Server {
    let d = dir.join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(d.join("main.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(d.join("main.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&d.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat;
    const ARENA_BASE: u32 = 1024;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    store.checkpoint_to(d.join("main.arena"));
    let mut runtime = AgentRuntime::with_storage(
        branches,
        Arc::new(MemEffectLog::new()),
        Arc::clone(&store) as Arc<dyn PageStore>,
    )
    .expect("attach arena storage");
    if durable {
        runtime = runtime
            .with_durable_provenance(d.join("main.provenance"))
            .expect("open durable provenance");
    }
    let runtime = Arc::new(runtime);
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime.clone()));
    let s = Server { ctx, runtime, bp, txn };

    let mut sess = s.ctx.session();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=ROWS {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, 0);"), &mut sess).unwrap();
    }
    s
}

/// `after - before` per kind; every counter is monotone.
fn delta(before: SyncCounts, after: SyncCounts) -> SyncCounts {
    let d = |a: u64, b: u64| b.checked_sub(a).expect("a sync counter went backwards");
    SyncCounts {
        runs: d(before.runs, after.runs),
        stamps: d(before.stamps, after.stamps),
        row_authors: d(before.row_authors, after.row_authors),
        forgets: d(before.forgets, after.forgets),
    }
}

struct MergeSample {
    start: Instant,
    end: Instant,
    syncs: SyncCounts,
}

/// Fork, update ids `1..=delta` once each to a value never used before, and time the `MERGE`.
///
/// ONE run id for every merge, which is load-bearing: `intern` is first-wins per `(agent, run)`
/// and syncs only for a new run, so its sync happens at the first `BEGIN` and never inside a
/// measured `MERGE` window. The counts below would show it as `runs = 1` if it did.
fn one_merge(s: &Server, delta_ops: usize, seq: &mut i64) -> Result<MergeSample, String> {
    let mut sess = s.ctx.session();
    exec(s, &format!("BEGIN AGENT SESSION AS 'd219' RUN 'one-run' MODEL '{MODEL}';"), &mut sess)?;
    for id in 1..=delta_ops {
        *seq += 1;
        exec(s, &format!("UPDATE t SET v = {} WHERE id = {id};", *seq), &mut sess)?;
    }
    let before = s.runtime.provenance().sync_counts();
    let start = Instant::now();
    let out = exec(s, "MERGE;", &mut sess)?;
    let end = Instant::now();
    let after = s.runtime.provenance().sync_counts();
    match out {
        Outcome::Agent(AgentOutput::Merge(report)) if report.applied_to_target => {}
        _ => return Err(format!("a MERGE of {delta_ops} ops did not reach the target")),
    }
    Ok(MergeSample { start, end, syncs: delta(before, after) })
}

#[derive(Clone, Copy, PartialEq)]
enum Reader {
    Alone,
    Queued,
    StateLock,
}

impl Reader {
    fn name(self) -> &'static str {
        match self {
            Reader::Alone => "A merge-alone",
            Reader::Queued => "B queued-reader",
            Reader::StateLock => "C state-lock-reader",
        }
    }
}

/// Read in a tight loop until `stop`, returning every SUCCESSFUL read's `(start, end)` and how
/// many reads returned an error.
///
/// Both kinds parse ONE statement once. The queued reader is pgwire's order exactly; the
/// state-lock reader forks an agent session first (before `ready`, so its intern is outside every
/// merge window) and never takes `begin_read`, so nothing but `state` can hold it.
fn read_loop(
    s: Arc<Server>,
    kind: Reader,
    ready: Arc<Barrier>,
    stop: Arc<AtomicBool>,
) -> Result<(Vec<(Instant, Instant)>, usize), String> {
    let slot = Arc::new(AtomicBool::new(false));
    let mut sess = s.ctx.session();
    // Everything that can fail before the barrier is RETURNED after it, never panicked: a reader
    // that died before `ready.wait()` would leave the merging thread parked on the barrier until
    // the step's timeout, and the output would say rc=124 instead of why.
    let setup = parse_one("SELECT v FROM t WHERE id = 7;").and_then(|stmt| {
        match kind {
            Reader::Queued => s.ctx.register_reader(Arc::clone(&slot)),
            Reader::StateLock => {
                exec(&s, &format!("BEGIN AGENT SESSION AS 'd219-reader' RUN 'reader' MODEL '{MODEL}';"), &mut sess)?;
            }
            Reader::Alone => return Err("phase A starts no reader".to_string()),
        }
        Ok(stmt)
    });
    ready.wait();
    let stmt = setup?;
    let mut cache: Option<(u64, Arc<Catalog>)> = None;
    let mut reads = Vec::new();
    let mut errors = 0usize;
    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();
        let result = {
            let shared = s.ctx.read_catalog(&mut cache);
            let attempted = if kind == Reader::Queued {
                match s.ctx.begin_read(&slot) {
                    Some(_pass) => try_run_read(&stmt, shared, s.bp.clone(), s.txn.clone(), &mut sess),
                    None => None,
                }
            } else {
                try_run_read(&stmt, shared, s.bp.clone(), s.txn.clone(), &mut sess)
            };
            match attempted {
                Some(r) => r.map(|_| ()),
                None => {
                    let mut cat = s.ctx.catalog();
                    run(stmt.clone(), &mut cat, s.bp.clone(), s.txn.clone(), &mut sess).map(|_| ())
                }
            }
        };
        // Only a read that returned rows is a read. An error that returns fast would pull a stall
        // toward zero, which is exactly the shape phase C's AFTER prediction has, so an errored
        // read is counted and kept out of every stall; `main` refuses to certify a run with any.
        let t1 = Instant::now();
        match result {
            Ok(()) => reads.push((t0, t1)),
            Err(_) => errors += 1,
        }
    }
    Ok((reads, errors))
}

/// Per merge, the longest a reader spent inside that merge's window before its read returned.
fn stalls(merges: &[MergeSample], reads: &[(Instant, Instant)]) -> Vec<Duration> {
    merges
        .iter()
        .filter_map(|m| {
            reads
                .iter()
                .filter(|(r0, r1)| *r1 > m.start && *r0 < m.end)
                .map(|(r0, r1)| r1.duration_since((*r0).max(m.start)))
                .max()
        })
        .collect()
}

fn median(mut v: Vec<Duration>) -> Duration {
    assert!(!v.is_empty(), "a median of nothing");
    v.sort();
    v[v.len() / 2]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

/// Least-squares slope of `ys` (ms) against the ladder.
fn slope(ys: &[f64]) -> f64 {
    let xs: Vec<f64> = LADDER.iter().map(|d| *d as f64).collect();
    let n = xs.len() as f64;
    let (mx, my) = (xs.iter().sum::<f64>() / n, ys.iter().sum::<f64>() / n);
    let num: f64 = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum();
    let den: f64 = xs.iter().map(|x| (x - mx) * (x - mx)).sum();
    num / den
}

/// What one phase returns: its merges, the per-merge reader stalls (none in phase A), and how many
/// reads returned an error.
type PhaseResult = (Vec<MergeSample>, Option<Vec<Duration>>, usize);

/// One phase at one δ: `REPS` merges, with the given reader running beside them.
fn phase(s: &Arc<Server>, delta_ops: usize, kind: Reader, seq: &mut i64) -> Result<PhaseResult, String> {
    let stop = Arc::new(AtomicBool::new(false));
    let reader = if kind == Reader::Alone {
        None
    } else {
        let ready = Arc::new(Barrier::new(2));
        let h = {
            let (s, ready, stop) = (Arc::clone(s), Arc::clone(&ready), Arc::clone(&stop));
            std::thread::spawn(move || read_loop(s, kind, ready, stop))
        };
        ready.wait();
        Some(h)
    };
    let mut merges = Vec::with_capacity(REPS);
    let mut failed = None;
    for _ in 0..REPS {
        match one_merge(s, delta_ops, seq) {
            Ok(m) => merges.push(m),
            Err(e) => {
                failed = Some(e);
                break;
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    let reads = reader.map(|h| h.join().expect("reader thread panicked"));
    if let Some(e) = failed {
        return Err(e);
    }
    let (stalled, read_errors) = match reads {
        None => (None, 0),
        Some(Err(e)) => return Err(format!("{} at d={delta_ops}: the reader never started: {e}", kind.name())),
        Some(Ok((reads, errors))) => {
            let st = stalls(&merges, &reads);
            if st.len() != merges.len() {
                return Err(format!(
                    "{} at d={delta_ops}: {} of {} merges had no overlapping read ({} reads, {errors} errors)",
                    kind.name(),
                    merges.len() - st.len(),
                    merges.len(),
                    reads.len()
                ));
            }
            (Some(st), errors)
        }
    };
    Ok((merges, stalled, read_errors))
}

/// The median fsync of a small append on a file in `dir`: the unit every slope is read in.
fn calibrate(dir: &std::path::Path) -> Duration {
    let path = dir.join("calibration.log");
    let mut f = OpenOptions::new().create(true).append(true).open(&path).unwrap();
    let mut v = Vec::with_capacity(CALIBRATION_SYNCS);
    for i in 0..CALIBRATION_SYNCS {
        f.write_all(&[i as u8; 25]).unwrap();
        let t0 = Instant::now();
        f.sync_data().unwrap();
        v.push(t0.elapsed());
    }
    let _ = std::fs::remove_file(&path);
    median(v)
}

struct Point {
    merge_ms: f64,
    queued_ms: f64,
    state_ms: f64,
}

/// Every phase at every δ on one store, phase order rotated per δ so drift does not land on one
/// phase. Returns the per-δ medians and the number of reads that errored, or the reason the arm
/// refused.
fn arm(dir: &std::path::Path, durable: bool) -> Result<(Vec<Point>, usize), String> {
    let label = if durable { "durable" } else { "control" };
    let s = Arc::new(build(dir, label, durable));
    let mut seq = 0i64;
    let orders = [
        [Reader::Alone, Reader::Queued, Reader::StateLock],
        [Reader::Queued, Reader::StateLock, Reader::Alone],
        [Reader::StateLock, Reader::Alone, Reader::Queued],
    ];
    let mut points = Vec::new();
    let mut arm_errors = 0usize;
    for (i, &d) in LADDER.iter().enumerate() {
        let (mut merge_ms, mut queued_ms, mut state_ms) = (f64::NAN, f64::NAN, f64::NAN);
        let mut counts: Option<SyncCounts> = None;
        let mut point_errors = 0usize;
        for &kind in &orders[i % 3] {
            let (merges, stalled, read_errors) = phase(&s, d, kind, &mut seq)?;
            point_errors += read_errors;
            for m in &merges {
                if let Some(c) = counts {
                    if c != m.syncs {
                        return Err(format!(
                            "{label} d={d}: sync counts differ between merges: {c:?} vs {:?}",
                            m.syncs
                        ));
                    }
                } else {
                    counts = Some(m.syncs);
                }
            }
            match (kind, stalled) {
                (Reader::Alone, _) => {
                    merge_ms = ms(median(merges.iter().map(|m| m.end.duration_since(m.start)).collect()))
                }
                (Reader::Queued, Some(st)) => queued_ms = ms(median(st)),
                (Reader::StateLock, Some(st)) => state_ms = ms(median(st)),
                _ => unreachable!("a reader phase returned no stalls"),
            }
        }
        let c = counts.expect("a δ point ran no merges");
        let model = if !durable {
            if c.total() != 0 {
                return Err(format!("control d={d}: the in-memory store synced: {c:?}"));
            }
            "control (all zero)"
        } else if c.runs != 0 || c.forgets != 0 || c.stamps != d as u64 {
            return Err(format!("durable d={d}: counts match neither model: {c:?}"));
        } else if c.row_authors == d as u64 && d > 1 {
            "BEFORE model (row_authors = d)"
        } else if c.row_authors == 1 && d > 1 {
            "AFTER model (row_authors = 1)"
        } else if c.row_authors == 1 {
            "d=1: both models predict 1"
        } else {
            return Err(format!("durable d={d}: row_authors={} matches neither d nor 1", c.row_authors));
        };
        println!(
            "  {label:<8} d={d:<4} merge {merge_ms:>9.3} ms | queued-reader stall {queued_ms:>9.3} ms | \
             state-lock-reader stall {state_ms:>9.3} ms | syncs/merge runs={} stamps={} row_authors={} \
             forgets={} | {model} | errored reads {point_errors}",
            c.runs, c.stamps, c.row_authors, c.forgets
        );
        points.push(Point { merge_ms, queued_ms, state_ms });
        arm_errors += point_errors;
    }
    Ok((points, arm_errors))
}

fn main() {
    let dir = std::env::temp_dir().join(format!("ferrodb-d219-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    println!("D219 merge/reader curve: ladder {LADDER:?}, {REPS} merges per phase per point");

    let f0 = calibrate(&dir);
    println!("f (median sync_data of a 25-byte append, {CALIBRATION_SYNCS} samples) at start: {:.4} ms", ms(f0));

    let control = arm(&dir, false);
    let durable = control.as_ref().ok().map(|_| arm(&dir, true));

    let f1 = calibrate(&dir);
    println!("f at end: {:.4} ms", ms(f1));
    let _ = std::fs::remove_dir_all(&dir);

    let ((control, control_errors), (durable, durable_errors)) = match (control, durable) {
        (Ok(c), Some(Ok(d))) => (c, d),
        (Err(e), _) | (_, Some(Err(e))) => {
            println!("REFUSED: {e}");
            std::process::exit(2);
        }
        (Ok(_), None) => unreachable!("the durable arm runs whenever the control arm succeeded"),
    };

    // Slopes in ms per op, and the durable arm's EXCESS over the control in fsyncs per op, read
    // against the mean of the two calibrations.
    let f = (ms(f0) + ms(f1)) / 2.0;
    let columns: [(&str, fn(&Point) -> f64); 3] = [
        ("merge latency", |p| p.merge_ms),
        ("queued-reader stall", |p| p.queued_ms),
        ("state-lock-reader stall", |p| p.state_ms),
    ];
    for (name, pick) in columns {
        let sc = slope(&control.iter().map(pick).collect::<Vec<f64>>());
        let sd = slope(&durable.iter().map(pick).collect::<Vec<f64>>());
        println!(
            "slope {name:<24} control {sc:>8.4} ms/op | durable {sd:>8.4} ms/op | excess {:>6.2} fsyncs/op (f = {f:.4} ms)",
            (sd - sc) / f
        );
    }
    // Printed above, and still not certified: an errored read is excluded from every stall, but a
    // phase that errored measured something other than the read it names.
    if control_errors + durable_errors > 0 {
        println!(
            "REFUSED: {control_errors} control and {durable_errors} durable reads returned an error; \
             the stall numbers above exclude them and are NOT certified"
        );
        std::process::exit(2);
    }
}
