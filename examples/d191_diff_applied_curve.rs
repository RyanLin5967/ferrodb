//! D191 — **does `DIFF` pay for every merge anyone has ever published?** Answered with an INTEGER.
//!
//! # The question
//!
//! `AgentRuntime::diff` — the function `DIFF` reaches (`dispatch.rs`, `BoundAgentStmt::Diff` →
//! `runtime.diff`) — computes each changed row's `concurrent` flag with
//! `state.applied.iter().any(..)`, once PER CHANGED ROW, over `State::applied`: a log that is global
//! across branches and never pruned. D86's `applied_by_cell` index is keyed `(tbl, row, col)` and
//! cannot serve the `(tbl, row)` question. Read from source the shape is O(delta × |applied|).
//! Nobody had measured it: `bench/d103_production_diff_curve.txt` measures a function `DIFF` does
//! not call (D193), on the table-size axis, and `bench/d86_merge_degrades_with_merge_count.txt`
//! measures MERGE.
//!
//! # The axis
//!
//! `|applied|` grows by publishing K OTHER branches' merges while the diffed branch's delta is held
//! at 4. The primary instrument is `DIFF_APPLIED_VISITED`, counted inside the `.any` closure — a
//! control-flow count, not a `len()` — so it sees the short-circuit. Pre-registration:
//! `artie-research/frontier/lane_d191_diff.md` §4, committed before this file was built.
//!
//! # The arms
//!
//! | arm | session | what it is for |
//! |---|---|---|
//! | A | `B`, forked BEFORE every merge, ids 1..4 | the claim: visited = 4 · \|applied\| |
//! | P | a FRESH session per checkpoint, forked AFTER every merge, ids 1..4 | history that PREDATES the fork cannot be concurrent — is it still visited? |
//! | M | `C`, forked before every merge, ids 1,2,3,19; merge #30 publishes id 19 | **the fire-check.** `.any` must stop at the id-19 entry, so M ≠ A. If M = A the counter is computing rows × len and the run is VOID |
//! | Δ | fresh sessions with delta 1..16 at the last checkpoint | product or sum? |
//!
//! `|applied|` is taken from the MERGE REPORTS (`MergeReport::rows[*].applied`, the same vec
//! `record_applied` pushes), never from the counter under test.
//!
//! # ⚠ What is NOT claimed
//!
//! A count linear in |applied| is not by itself a latency problem: D114 measured a scan that was
//! exactly `delta × ops` and moved merge latency by nothing. Wall time is printed as the SECONDARY
//! column for exactly that reason, and the verdict on cost is read from it, not from the count.
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::changeset::{ChangeOutcome, MergeReport};
use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{diff_scan_counters, AgentRuntime};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::ids::RowId;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const NROWS: i64 = 1000;
/// Other branches merged before each checkpoint. 7 sizes; 32x across the non-zero ones.
const CHECKPOINTS: [usize; 7] = [0, 50, 100, 200, 400, 800, 1600];
/// The diffed branches' delta. PINNED — that is what makes this an |applied| axis.
const DELTA: i64 = 4;
/// Rows each OTHER branch updates before merging.
const OTHER_ROWS: usize = 4;
/// Other branches draw their rows from `POOL_LO..=NROWS`, so they never touch ids 1..20.
const POOL_LO: i64 = 21;
/// ARM M's extra row, and the one merge that publishes it.
const MATCH_ID: i64 = 19;
const MATCH_MERGE: usize = 30;
const REPS: usize = 7;
const DELTAS: [i64; 5] = [1, 2, 4, 8, 16];

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

/// Run one statement the way the server does. Copied from `examples/d176_merge_rowcount.rs`.
fn exec(s: &Server, sql: &str, sess: &mut Session) -> Result<Outcome, String> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .map_err(|e| e.to_string())?;
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() {
        return Err(format!("parse failed for {sql}: {:?}", parser.errors));
    }
    let stmt = stmts.remove(0);
    let mut cat = s.ctx.catalog();
    let out = run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess);
    drop(cat);
    out.map_err(|e| e.to_string())
}

/// The D176 server, with its three load-bearing configuration corrections carried over verbatim:
/// `with_storage` (not `with_catalog`), `s.ctx.session()` (never `Session::new()`), and
/// `checkpoint_to`. See `examples/d176_merge_rowcount.rs::build_sized_indexed` for what each one
/// cost when it was missing.
fn build(dir: &std::path::Path) -> Server {
    let d = dir.join("srv");
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
    let branches: Arc<dyn BranchCatalog> = cat.clone();
    const ARENA_BASE: u32 = 1024;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    store.checkpoint_to(d.join("main.arena"));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    let s = Server { ctx, bp, txn };
    let mut sess = s.ctx.session();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=NROWS {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess).unwrap();
    }
    s
}

/// Open an agent session and stage one single-column UPDATE per id.
fn open_with(s: &Server, name: &str, ids: &[i64], v: i64) -> Session {
    let mut sess = s.ctx.session();
    exec(s, &format!("BEGIN AGENT SESSION AS '{name}';"), &mut sess)
        .unwrap_or_else(|e| panic!("BEGIN {name}: {e}"));
    for id in ids {
        exec(s, &format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut sess)
            .unwrap_or_else(|e| panic!("UPDATE id={id} on {name}: {e}"));
    }
    sess
}

/// One `DIFF;`, with the three counters scoped to that one statement by read-twice-and-subtract.
/// Exact because this harness is single-threaded.
struct Obs {
    visited: u64,
    rows: u64,
    frame: u64,
    nanos: u128,
    out_rows: usize,
    /// First-column values of the rows DIFF reported as `PendingConcurrent`.
    concurrent_ids: Vec<i64>,
    /// (first-column value, RowId) for every row DIFF reported.
    ids: Vec<(i64, RowId)>,
}

fn first_int(img: &Option<Vec<Value>>) -> i64 {
    match img.as_ref().and_then(|v| v.first()) {
        Some(Value::Integer(i)) => *i as i64,
        Some(Value::BigInt(i)) => *i,
        _ => -1,
    }
}

fn diff_once(s: &Server, sess: &mut Session) -> Obs {
    let (a0, r0, f0) = diff_scan_counters();
    let t0 = Instant::now();
    let out = exec(s, "DIFF;", sess).expect("DIFF failed");
    let nanos = t0.elapsed().as_nanos();
    let (a1, r1, f1) = diff_scan_counters();
    let cs = match out {
        Outcome::Agent(AgentOutput::Diff(cs)) => cs,
        _ => panic!("DIFF did not return a changeset — the instrument window is not a DIFF"),
    };
    let mut concurrent_ids = Vec::new();
    let mut ids = Vec::new();
    for r in &cs.rows {
        let id = first_int(&r.after);
        ids.push((id, r.row));
        if matches!(r.outcome, ChangeOutcome::PendingConcurrent) {
            concurrent_ids.push(id);
        }
    }
    Obs {
        visited: a1 - a0,
        rows: r1 - r0,
        frame: f1 - f0,
        nanos,
        out_rows: cs.rows.len(),
        concurrent_ids,
        ids,
    }
}

/// REPS DIFFs, reported as min/max for every integer (min == max is how determinism is SHOWN) and
/// the median for wall time.
struct Cell {
    vmin: u64,
    vmax: u64,
    rmin: u64,
    rmax: u64,
    fmin: u64,
    fmax: u64,
    med_ns: u128,
    out_rows: usize,
    concurrent: Vec<i64>,
    /// Whether every rep's concurrent list equals rep 0's, which is the one `concurrent` holds.
    /// PREREG amendment 1: observation only.
    conc_reps_agree: bool,
}

fn cell(s: &Server, sess: &mut Session) -> Cell {
    let obs: Vec<Obs> = (0..REPS).map(|_| diff_once(s, sess)).collect();
    let mut ns: Vec<u128> = obs.iter().map(|o| o.nanos).collect();
    ns.sort_unstable();
    let out_rows = obs[0].out_rows;
    assert!(obs.iter().all(|o| o.out_rows == out_rows), "DIFF row count varied across reps");
    Cell {
        vmin: obs.iter().map(|o| o.visited).min().unwrap(),
        vmax: obs.iter().map(|o| o.visited).max().unwrap(),
        rmin: obs.iter().map(|o| o.rows).min().unwrap(),
        rmax: obs.iter().map(|o| o.rows).max().unwrap(),
        fmin: obs.iter().map(|o| o.frame).min().unwrap(),
        fmax: obs.iter().map(|o| o.frame).max().unwrap(),
        med_ns: ns[ns.len() / 2],
        out_rows,
        concurrent: obs[0].concurrent_ids.clone(),
        conc_reps_agree: obs.iter().all(|o| o.concurrent_ids == obs[0].concurrent_ids),
    }
}

fn verdict(ok: bool) -> &'static str {
    if ok {
        "ok"
    } else {
        "MISMATCH"
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("d191_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    println!("D191 — AgentRuntime::diff's scan of State::applied, as a control-flow COUNT.");
    println!("NROWS={NROWS} DELTA={DELTA} OTHER_ROWS={OTHER_ROWS} POOL={POOL_LO}..={NROWS} MATCH_ID={MATCH_ID} MATCH_MERGE={MATCH_MERGE} REPS={REPS}");
    println!("visited = DIFF_APPLIED_VISITED, rows = DIFF_ROWS, frame = DIFF_FRAME_VISITED, each per ONE `DIFF;`.");
    println!("|applied| is summed from MergeReport::rows[*].applied of every published merge, NOT from the counter.");
    println!();

    let s = build(&dir);
    // Both forked BEFORE any merge, so every later entry has seq > their fork_seq.
    let mut b = open_with(&s, "B", &[1, 2, 3, 4], 1);
    let mut c = open_with(&s, "C", &[1, 2, 3, MATCH_ID], 1);

    // C's RowId for id 19, read from C's own DIFF so no assumption about how ids become RowIds.
    let c0 = diff_once(&s, &mut c);
    let match_row: RowId = c0
        .ids
        .iter()
        .find(|(id, _)| *id == MATCH_ID)
        .map(|(_, r)| *r)
        .expect("C's DIFF does not contain id 19 — the fixture did not stage it");
    println!("C's RowId for id {MATCH_ID}: {:?}", match_row);

    let mut applied_total: u64 = 0;
    let mut ops_per_merge: Vec<u64> = Vec::new();
    // Position in `State::applied` of the id-19 entry, from merge #30's report.
    let mut match_pos: Option<u64> = None;
    let mut cursor: usize = 0;
    let pool = (NROWS - POOL_LO + 1) as usize;
    let mut k_done = 0usize;
    let mut merge_ns: Vec<u128> = Vec::new();

    println!("== ARMS A / P / M, delta pinned at {DELTA}, |applied| grown by K other branches' merges ==");
    println!(
        "{:>5} {:>8} | {:>7} {:>7} {:>9} {:>4} {:>5} {:>10} {:>8} | {:>7} {:>7} {:>9} {:>10} {:>8} | {:>7} {:>7} {:>9} {:>6} {:>8} {:>5}",
        "K", "applied",
        "A_vmin", "A_vmax", "A_expect", "A_r", "A_frm", "A_med_ns", "A",
        "P_vmin", "P_vmax", "P_expect", "P_med_ns", "P",
        "M_vmin", "M_vmax", "M_expect", "M_conc", "M", "rows"
    );
    let mut rows_a: Vec<(usize, u64, u64, u128)> = Vec::new();
    // PREREG amendment 1 (lane_d191_diff.md): one observation line per K, printed after the table
    // so every line above keeps its format.
    let mut outcome_lines: Vec<String> = Vec::new();
    let mut all_ok = true;
    for &k in CHECKPOINTS.iter() {
        while k_done < k {
            let m = k_done + 1;
            let mut ids: Vec<i64> = (0..OTHER_ROWS)
                .map(|_| {
                    let id = POOL_LO + (cursor % pool) as i64;
                    cursor += 1;
                    id
                })
                .collect();
            if m == MATCH_MERGE {
                ids[0] = MATCH_ID;
            }
            let mut sess = open_with(&s, &format!("o{m}"), &ids, (m % 1000) as i64 + 2);
            let t0 = Instant::now();
            let rep: MergeReport = match exec(&s, "MERGE;", &mut sess) {
                Ok(Outcome::Agent(AgentOutput::Merge(r))) => r,
                Ok(_) => panic!("merge {m} returned a non-merge outcome"),
                Err(e) => panic!("merge {m} failed: {e}"),
            };
            merge_ns.push(t0.elapsed().as_nanos());
            if !rep.applied_to_target {
                panic!("merge {m} did not reach the target; |applied| would be wrong");
            }
            if m == MATCH_MERGE {
                let mut off = applied_total;
                for r in &rep.rows {
                    if let Some(i) = r.applied.iter().position(|op| op.row == match_row) {
                        match_pos = Some(off + i as u64);
                        break;
                    }
                    off += r.applied.len() as u64;
                }
                assert!(match_pos.is_some(), "merge {m} published no op on C's id-19 RowId");
            }
            let n: u64 = rep.rows.iter().map(|r| r.applied.len() as u64).sum();
            ops_per_merge.push(n);
            applied_total += n;
            k_done += 1;
        }

        let a = cell(&s, &mut b);
        let mut p_sess = open_with(&s, &format!("P{k}"), &[1, 2, 3, 4], 3);
        let p = cell(&s, &mut p_sess);
        drop(p_sess);
        let mcell = cell(&s, &mut c);

        let a_exp = DELTA as u64 * applied_total;
        let m_exp = match match_pos {
            Some(pos) => (DELTA as u64 - 1) * applied_total + pos + 1,
            None => DELTA as u64 * applied_total,
        };
        let a_ok = a.vmin == a_exp && a.vmax == a_exp && a.rmin == DELTA as u64 && a.rmax == DELTA as u64 && a.concurrent.is_empty() && a.out_rows == DELTA as usize;
        let p_ok = p.vmin == a_exp && p.vmax == a_exp && p.rmin == DELTA as u64 && p.rmax == DELTA as u64 && p.concurrent.is_empty();
        let m_conc_exp: Vec<i64> = if match_pos.is_some() { vec![MATCH_ID] } else { vec![] };
        let m_ok = mcell.vmin == m_exp && mcell.vmax == m_exp && mcell.concurrent == m_conc_exp && mcell.rmin == DELTA as u64;
        all_ok &= a_ok && p_ok && m_ok;
        println!(
            "{:>5} {:>8} | {:>7} {:>7} {:>9} {:>4} {:>5} {:>10} {:>8} | {:>7} {:>7} {:>9} {:>10} {:>8} | {:>7} {:>7} {:>9} {:>6} {:>8} {:>5}",
            k, applied_total,
            a.vmin, a.vmax, a_exp, a.rmax, format!("{}-{}", a.fmin, a.fmax), a.med_ns, verdict(a_ok),
            p.vmin, p.vmax, a_exp, p.med_ns, verdict(p_ok),
            mcell.vmin, mcell.vmax, m_exp, format!("{:?}", mcell.concurrent), verdict(m_ok),
            format!("{}/{}/{}", a.out_rows, p.out_rows, mcell.out_rows)
        );
        rows_a.push((k, applied_total, a.vmax, a.med_ns));
        // PREREG amendment 1: observation only. The ok flags above already CHECK DIFF_ROWS for P
        // and M (M by its min only) and the concurrent lists for A and P, but never PRINT them, so
        // a MISMATCH flag could not say which part failed. `min-max` is the format of the `A_frm` column, and a
        // list is `{:?}`, like `M_conc`.
        outcome_lines.push(format!(
            "outcomes K={} A_rows={}-{} A_conc={:?} P_rows={}-{} P_conc={:?} M_rows={}-{} M_conc={:?} conc_reps_agree={}/{}/{}",
            k,
            a.rmin, a.rmax, a.concurrent,
            p.rmin, p.rmax, p.concurrent,
            mcell.rmin, mcell.rmax, mcell.concurrent,
            a.conc_reps_agree, p.conc_reps_agree, mcell.conc_reps_agree
        ));
    }
    println!();
    println!("== OUTCOMES per K (PREREG amendment 1, observation only): DIFF_ROWS min-max and PendingConcurrent ids, arms A / P / M ==");
    for line in &outcome_lines {
        println!("{line}");
    }
    println!();
    println!("ops per merge (from reports): min={} max={}", ops_per_merge.iter().min().unwrap(), ops_per_merge.iter().max().unwrap());
    println!("id-{MATCH_ID} entry position in State::applied (from merge #{MATCH_MERGE}'s report): {:?}", match_pos);
    println!();

    // ---------------- DELTA axis at the final |applied| ----------------
    println!("== ARM Δ — delta varies at K={} (|applied|={applied_total}), fresh sessions forked after every merge ==", CHECKPOINTS[CHECKPOINTS.len() - 1]);
    println!("{:>5} {:>9} {:>9} {:>10} {:>5} {:>9} {:>10} {:>8}", "delta", "vmin", "vmax", "expect", "rows", "frame", "med_ns", "");
    for d in DELTAS {
        let ids: Vec<i64> = (1..=d).collect();
        let mut sess = open_with(&s, &format!("D{d}"), &ids, 5);
        let cl = cell(&s, &mut sess);
        let exp = d as u64 * applied_total;
        let ok = cl.vmin == exp && cl.vmax == exp && cl.rmax == d as u64 && cl.concurrent.is_empty();
        all_ok &= ok;
        println!("{:>5} {:>9} {:>9} {:>10} {:>5} {:>9} {:>10} {:>8}", d, cl.vmin, cl.vmax, exp, cl.rmax, format!("{}-{}", cl.fmin, cl.fmax), cl.med_ns, verdict(ok));
    }
    println!();

    // ---------------- Wall time, SECONDARY ----------------
    // Least-squares slope of ARM A's median DIFF wall time against visited entries.
    let n = rows_a.len() as f64;
    let xs: Vec<f64> = rows_a.iter().map(|r| r.2 as f64).collect();
    let ys: Vec<f64> = rows_a.iter().map(|r| r.3 as f64).collect();
    let mx = xs.iter().sum::<f64>() / n;
    let my = ys.iter().sum::<f64>() / n;
    let sxy: f64 = xs.iter().zip(&ys).map(|(x, y)| (x - mx) * (y - my)).sum();
    let sxx: f64 = xs.iter().map(|x| (x - mx) * (x - mx)).sum();
    let slope = sxy / sxx;
    let icept = my - slope * mx;
    println!("== WALL TIME (secondary) — ARM A median DIFF ns vs visited entries ==");
    println!("least-squares: {:.3} ns per visited entry, intercept {:.0} ns", slope, icept);
    let first = &rows_a[0];
    let last = &rows_a[rows_a.len() - 1];
    println!("K={} median {} ns  ->  K={} median {} ns  ({:.2}x)", first.0, first.3, last.0, last.3, last.3 as f64 / first.3.max(1) as f64);
    let mut ms = merge_ns.clone();
    ms.sort_unstable();
    let dec = ms.len() / 10;
    let f40: u128 = merge_ns[..dec].iter().sum::<u128>() / dec.max(1) as u128;
    let l40: u128 = merge_ns[merge_ns.len() - dec..].iter().sum::<u128>() / dec.max(1) as u128;
    println!("context: other-branch MERGE mean, first decile {} ns, last decile {} ns (median {} ns)", f40, l40, ms[ms.len() / 2]);
    println!();
    println!("PRE-REGISTERED CHECKS (P1-P4, P6-P8): {}", if all_ok { "ALL MATCH" } else { "AT LEAST ONE MISMATCH — read the rows" });
    let _ = std::fs::remove_dir_all(&dir);
    if !all_ok {
        std::process::exit(2);
    }
}
