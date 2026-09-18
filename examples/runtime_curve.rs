//! S15: does the RUNTIME and the SQL path survive 10⁶ branches?
//!
//! `bench/curve_to_1e6.txt` measured the CATALOG to 10⁶ — space linear, reopen O(1), fork
//! throughput flat. It measured `TableBranchCatalog::fork` and nothing above it. Nobody had run a
//! statement, an agent session or a system view against a 10⁶-branch database, and the objective is
//! the whole system, not the catalog.
//!
//! Two phases, selected by `argv[1]`, because they answer two different questions and one of them
//! can plausibly exhaust memory:
//!
//!   runtime_curve query    [checkpoints] [threads]
//!       Reach N by forking the RAW catalog (the proven-fast path), then at each N measure the
//!       operations a USER touches: `BEGIN AGENT SESSION`, an in-session INSERT, an in-session
//!       SELECT, `SELECT ... AS OF BRANCH`, and each of the five `ferro_*` system views.
//!       The question: does a query survive a database that already holds N branches?
//!
//!   runtime_curve session  [checkpoints]
//!       Reach N by calling `BEGIN AGENT SESSION` N times — through the runtime, not the catalog —
//!       holding every session open. The question: does the RUNTIME survive N agent sessions?
//!       Reported per checkpoint: sessions/sec for that segment, and RSS.
//!
//! **The instrument is calibrated by a control that is KNOWN to move.** `live_count` is already
//! measured linear (0.664 ms at 10⁴ -> 59.234 ms at 10⁶, bench/curve_to_1e6.txt). It is timed in
//! the `query` phase's table for exactly one reason: if this harness reproduces that curve, the
//! harness is wired to a real durable catalog and CAN see an O(N) cost. A flat row next to a flat
//! control proves nothing. A flat row next to a control that rose 90x is a result.
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// This run's scratch directory, removed when the harness drops.
///
/// **It used to be a bare `PathBuf` with a `remove_dir_all` at the end of each phase**, which
/// cleans up on the SUCCESS path and only there. A run killed by `timeout` (SIGTERM), killed by the
/// agent fleet, panicked, or stopped by `ENOSPC` never reaches that last line, and leaves its whole
/// catalog behind — about 270 MB for a 10^6 run. Enough of those took this machine to 100% disk
/// TWICE in one session, at which point unrelated suites start failing with
/// `wal error: No space left on device (os error 28)` and read as a regression in the WAL layer
/// rather than as a full disk. A benchmark that can do that to the next person's test run is a
/// defect in the benchmark.
struct ScratchDir(std::path::PathBuf);

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Remove the scratch directories of previous runs whose process is **gone**.
///
/// `Drop` closes the panic and early-return cases but NOT the one that actually happens here:
/// `timeout` sends `SIGTERM`, and Rust's default disposition terminates the process without
/// unwinding, so no destructor runs. Nothing inside a killed process can clean up after it, so the
/// next run does it instead — which also covers `SIGKILL`, where not even a signal handler would.
///
/// Liveness is decided by `kill(pid, 0)`, not by mtime. An mtime sweep cannot tell a long run from
/// an abandoned one and would delete the catalog out from under a live 10^6 benchmark. Pid reuse
/// can only make a dead run look alive, which skips a cleanup and is the safe direction.
fn sweep_stale_scratch() {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else { return };
    for e in entries.flatten() {
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(pid) = name.strip_prefix("ferrodb-rtcurve-") else { continue };
        let Ok(pid) = pid.parse::<i32>() else { continue };
        if pid == std::process::id() as i32 {
            continue;
        }
        // 0 means the process exists; -1 means it does not (or is not ours to signal).
        if unsafe { kill(pid, 0) } != 0 {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

fn peak_rss_bytes() -> u64 {
    // getrusage(RUSAGE_SELF).ru_maxrss; bytes on macOS, kilobytes on Linux.
    #[repr(C)]
    #[derive(Default)]
    struct RUsage {
        ru_utime: [i64; 2],
        ru_stime: [i64; 2],
        ru_maxrss: i64,
        rest: [i64; 14],
    }
    unsafe extern "C" {
        fn getrusage(who: i32, usage: *mut RUsage) -> i32;
    }
    let mut u = RUsage::default();
    if unsafe { getrusage(0, &mut u) } != 0 {
        return 0;
    }
    if cfg!(target_os = "macos") { u.ru_maxrss as u64 } else { u.ru_maxrss as u64 * 1024 }
}

/// Everything a statement needs, wired exactly as `tests/integration_quarantine.rs` wires it, so
/// this measures the same path an integration test and the CLI take rather than a private one.
struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    cat: Arc<TableBranchCatalog>,
    _dir: ScratchDir,
}

impl Db {
    fn new() -> Self {
        // Before taking any space, give back what previous killed runs left behind.
        sweep_stale_scratch();
        let dir = std::env::temp_dir().join(format!("ferrodb-rtcurve-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.join("main.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.join("main.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        // The DURABLE branch catalog, on a real file. An in-memory stand-in here is exactly the
        // mistake this run exists to avoid: it would make every number below a property of a
        // HashMap rather than of the branch engine.
        let cat_path = dir.join("branches.branchcat");
        let _ = std::fs::remove_file(&cat_path);
        let cat = Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).expect("open catalog"));
        let runtime = Arc::new(AgentRuntime::with_catalog(cat.clone() as Arc<dyn BranchCatalog>));
        Db { catalog, bp, txn, runtime, cat, _dir: ScratchDir(dir) }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    /// Rows a statement produced, whichever shape the executor answered in.
    fn rows(&mut self, sql: &str, s: &mut Session) -> usize {
        match self.ok(sql, s) {
            Outcome::Rows(r) => r.len(),
            Outcome::Table(t) => t.rows.len(),
            _ => 0,
        }
    }
}

/// Mean milliseconds per rep, and the rows the last rep produced.
fn time_ms(reps: usize, mut f: impl FnMut() -> usize) -> (f64, usize) {
    let mut rows = 0;
    let t = Instant::now();
    for _ in 0..reps {
        rows = f();
    }
    (t.elapsed().as_secs_f64() * 1000.0 / reps as f64, rows)
}

fn checkpoints_from(arg: Option<String>) -> Vec<usize> {
    arg.unwrap_or_else(|| "10000,100000,1000000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

fn main() {
    let phase = std::env::args().nth(1).unwrap_or_else(|| "query".into());
    match phase.as_str() {
        "query" => query_phase(),
        "session" => session_phase(),
        "fork" => fork_phase(),
        "firecheck" => firecheck_phase(),
        other => {
            eprintln!("unknown phase {other:?}; expected `query` or `session`");
            std::process::exit(2);
        }
    }
}

// =================================================================================================
// phase 1 — the user-facing paths, against a database that already holds N branches
// =================================================================================================

fn query_phase() {
    let checkpoints = checkpoints_from(std::env::args().nth(2));
    let threads: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(64);

    let mut db = Db::new();
    let mut boot = db.session();
    db.ok("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);", &mut boot);
    for i in 1..=20 {
        db.ok(&format!("INSERT INTO inv VALUES ({i}, {});", i * 10), &mut boot);
    }
    drop(boot);

    println!("S15: the RUNTIME and SQL path at N branches. Durable catalog, full scanner->parser->executor.");
    println!("Every timing is mean-of-reps through `execution::executor::run`, the path the CLI and");
    println!("pgserver both take. `live_count` is the CALIBRATION CONTROL: it is already known linear");
    println!("(bench/curve_to_1e6.txt), so a flat row is only meaningful while this column rises.");
    println!();
    println!(
        "{:>9} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>10}",
        "N",
        "BEGIN ms",
        "INSERT ms",
        "SELECT ms",
        "ASOF ms",
        "br_1row ms",
        "br_all ms",
        "runs ms",
        "activity ms",
        "quaran ms",
        "live_cnt ms",
        "RSS MB",
    );

    let lease = LeaseDeadline(u64::MAX);
    let mut done = 0usize;
    // Keeps its own counter so a run id is never reused: `intern` is first-wins on
    // `(agent, run)`, and a repeated pair would measure a HashMap hit instead of a new run.
    let mut seq = 0usize;

    for &target in &checkpoints {
        if target <= done {
            continue;
        }
        let seg = target - done;
        let per = seg / threads.max(1);
        let actually = per * threads;
        let cat = Arc::clone(&db.cat);
        std::thread::scope(|s| {
            for _ in 0..threads {
                let cat = Arc::clone(&cat);
                s.spawn(move || {
                    for _ in 0..per {
                        cat.fork(BranchId::TRUNK, lease).expect("fork");
                    }
                });
            }
        });
        done += actually;

        // Reps scale down as the row gets expensive, so a 10⁶ run finishes. The cheap operations
        // keep enough reps that a sub-millisecond number is not one clock tick.
        let cheap = if done >= 500_000 { 200 } else { 500 };
        let view_reps = if done >= 50_000 { 1 } else { 3 };
        // Each expensive measurement is reported to stderr the moment it lands. A 10⁶ system-view
        // read materialises a row per branch; if that exhausts memory, the numbers already taken
        // must survive the process that dies taking the next one.
        macro_rules! note {
            ($($a:tt)*) => {{ eprintln!($($a)*); use std::io::Write; let _ = std::io::stderr().flush(); }};
        }

        // --- BEGIN AGENT SESSION, through the runtime, on a database holding `done` branches.
        let (begin_ms, _) = time_ms(cheap, || {
            seq += 1;
            let mut s = db.session();
            db.ok(&format!("BEGIN AGENT SESSION AS 'a' RUN 'r_{seq}';"), &mut s);
            let b = s.agent.as_ref().unwrap().branch_name.clone();
            // Abandoned so the workspace map does not grow under the other measurements — the
            // cost of HOLDING sessions is phase 2's question, not this one's.
            db.ok(&format!("ABANDON BRANCH {b};"), &mut s);
            0
        });

        // One live session, which the next three measurements run inside.
        seq += 1;
        let mut live = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'a' RUN 'r_{seq}';"), &mut live);
        let live_branch = live.agent.as_ref().unwrap().branch_name.clone();

        // --- an INSERT on a branch: staged in the branch's private buffer.
        let mut k = 1_000_000;
        let (insert_ms, _) = time_ms(cheap, || {
            k += 1;
            db.ok(&format!("INSERT INTO inv VALUES ({k}, 1);"), &mut live);
            0
        });

        // --- a SELECT on a branch: runtime.select, over the branch's visible state.
        let (select_ms, sel_rows) = time_ms(cheap, || {
            db.rows("SELECT qty FROM inv WHERE id = 7;", &mut live)
        });
        assert_eq!(sel_rows, 1, "the in-session SELECT stopped returning its row");

        // --- SELECT ... AS OF BRANCH: another branch's uncommitted state, exit criterion 3.
        let mut reader = db.session();
        let asof = format!("SELECT qty FROM inv AS OF BRANCH {live_branch} WHERE id = 7;");
        let (asof_ms, asof_rows) = time_ms(cheap, || db.rows(&asof, &mut reader));
        assert_eq!(asof_rows, 1, "AS OF BRANCH stopped returning its row");

        // --- the system views. `br_1row` is the query a human types to look ONE branch up; it is
        // separated from `br_all` precisely because they should not cost the same and do.
        note!("  N={done}: begin {begin_ms:.4} insert {insert_ms:.4} select {select_ms:.4} asof {asof_ms:.4} ms");
        let one = "SELECT generation, state FROM ferro_branches WHERE branch_id = 1;".to_string();
        let (br1_ms, br1_rows) = time_ms(view_reps, || db.rows(&one, &mut reader));
        assert_eq!(br1_rows, 1, "ferro_branches lost the trunk row");
        note!("  N={done}: ferro_branches WHERE branch_id=1 -> {br1_rows} row in {br1_ms:.2} ms");

        let (runs_ms, runs_rows) =
            time_ms(view_reps, || db.rows("SELECT * FROM ferro_runs;", &mut reader));
        note!("  N={done}: ferro_runs -> {runs_rows} rows in {runs_ms:.2} ms");
        let (act_ms, _) =
            time_ms(view_reps, || db.rows("SELECT * FROM ferro_run_activity;", &mut reader));
        let (quar_ms, _) =
            time_ms(view_reps, || db.rows("SELECT * FROM ferro_quarantine;", &mut reader));
        note!("  N={done}: activity {act_ms:.4} quarantine {quar_ms:.4} ms");

        // The calibration control. Known linear; if this does not rise, the harness is blind.
        let t = Instant::now();
        let live_n = db.cat.live_count().unwrap_or(0);
        let live_ms = t.elapsed().as_secs_f64() * 1000.0;
        assert!(live_n >= done, "live_count {live_n} < branches forked {done}");
        note!("  N={done}: live_count (CONTROL, known linear) {live_ms:.3} ms");

        // Last, because it materialises every branch as a row and is the candidate wall.
        let (brall_ms, brall_rows) =
            time_ms(view_reps, || db.rows("SELECT * FROM ferro_branches;", &mut reader));
        assert!(brall_rows >= done, "ferro_branches returned {brall_rows} of {done} branches");
        note!("  N={done}: ferro_branches (all) -> {brall_rows} rows in {brall_ms:.2} ms, peak RSS {:.1} MB", peak_rss_bytes() as f64 / 1e6);

        println!(
            "{:>9} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>12.2} {:>12.2} {:>12.2} {:>12.4} {:>12.4} {:>12.3} {:>10.1}",
            done,
            begin_ms,
            insert_ms,
            select_ms,
            asof_ms,
            br1_ms,
            brall_ms,
            runs_ms,
            act_ms,
            quar_ms,
            live_ms,
            peak_rss_bytes() as f64 / 1e6,
        );

        db.ok(&format!("ABANDON BRANCH {live_branch};"), &mut live);
    }

    println!();
    println!("BEGIN/INSERT/SELECT/ASOF are per-STATEMENT costs. br_* / runs / activity / quaran are");
    println!("one system-view read each. br_1row and br_all are the SAME query shape with and without");
    println!("a WHERE that selects exactly one row.");
}

// =================================================================================================
// phase 4 — forcing the FLAT columns to fire
// =================================================================================================

/// Phase 1 reports INSERT, SELECT and `AS OF BRANCH` as flat from 10⁴ to 10⁶ branches. A timer that
/// is simply broken reports flat too, and the two are indistinguishable from the flat run alone —
/// a benchmark in this project once used an in-memory catalog and was blind to an O(N²) cost
/// entirely. `live_count` calibrates the harness as a whole (it rose 120x), but it does not
/// calibrate *these three columns*, which are separate timers over separate code.
///
/// So: hold the branch count FIXED and turn a different knob these paths are known to be sensitive
/// to — the TABLE. `runtime.visible_rows` (`src/agent_sql/runtime.rs:1029`) calls `scan_table` and
/// then walks the workspace, so all three must rise with table rows. If they do, these timers are
/// live, and their flatness against branch count is a measurement rather than a silence.
fn firecheck_phase() {
    let sizes: Vec<usize> = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "20,200,2000,20000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let branches: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    let mut db = Db::new();
    let mut boot = db.session();
    db.ok("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);", &mut boot);
    drop(boot);

    // A fixed, non-trivial branch count, so the knob under test is the only thing moving.
    let lease = LeaseDeadline(u64::MAX);
    let cat = Arc::clone(&db.cat);
    std::thread::scope(|s| {
        for _ in 0..16 {
            let cat = Arc::clone(&cat);
            s.spawn(move || {
                for _ in 0..(branches / 16) {
                    cat.fork(BranchId::TRUNK, lease).expect("fork");
                }
            });
        }
    });

    println!("S15 phase 4 — FIRE-CHECK. The branch count is FIXED at {branches}; the TABLE grows.");
    println!("These are the same three timers phase 1 reports as flat against branch count. If they");
    println!("do not move here, phase 1's flat rows are a broken instrument and mean nothing.");
    println!();
    println!("{:>9} {:>12} {:>12} {:>12} {:>10}", "rows", "INSERT ms", "SELECT ms", "ASOF ms", "sel rows");

    let mut planted = 0usize;
    let mut seq = 0usize;
    for &size in &sizes {
        let mut boot = db.session();
        while planted < size {
            planted += 1;
            db.ok(&format!("INSERT INTO inv VALUES ({planted}, {});", planted % 97), &mut boot);
        }
        drop(boot);

        seq += 1;
        let mut live = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'a' RUN 'fc_{seq}';"), &mut live);
        let name = live.agent.as_ref().unwrap().branch_name.clone();

        let mut k = 5_000_000;
        let (insert_ms, _) = time_ms(200, || {
            k += 1;
            db.ok(&format!("INSERT INTO inv VALUES ({k}, 1);"), &mut live);
            0
        });
        let (select_ms, sel_rows) =
            time_ms(200, || db.rows("SELECT qty FROM inv WHERE id = 7;", &mut live));
        let mut reader = db.session();
        let asof = format!("SELECT qty FROM inv AS OF BRANCH {name} WHERE id = 7;");
        let (asof_ms, _) = time_ms(200, || db.rows(&asof, &mut reader));

        println!(
            "{:>9} {:>12.4} {:>12.4} {:>12.4} {:>10}",
            planted, insert_ms, select_ms, asof_ms, sel_rows
        );
        use std::io::Write;
        let _ = std::io::stdout().flush();
        db.ok(&format!("ABANDON BRANCH {name};"), &mut live);
    }
    println!();
    println!("Rising here + flat in phase 1 = the SQL data path is sensitive to TABLE size and not to");
    println!("BRANCH count, which is the claim. Flat here would mean the timers see nothing at all.");
}

// =================================================================================================
// phase 3 — WHERE the per-statement `BEGIN AGENT SESSION` cost lives
// =================================================================================================

/// `BEGIN AGENT SESSION` is the one per-statement number in phase 1 that did not come out flat:
/// ~13 ms at 10⁴ and 10⁵, ~28 ms at 10⁶. Sublinear, so not a walk — but a rise on a per-statement
/// path has to be attributed rather than shrugged at, and phase 1 could not attribute it because
/// it timed the whole stack as one number.
///
/// Three nested layers on ONE catalog at ONE N, single-threaded so no group commit hides the
/// fsync, interleaved so a device that drifts drifts through all three equally:
///   raw    `TableBranchCatalog::fork`        — the catalog and the device
///   rt     `AgentRuntime::begin_session_as`  — the above, plus interning and the Workspace
///   stmt   `BEGIN AGENT SESSION AS ...`      — the above, plus scanner, parser, binder, dispatch
/// Each layer's own cost is the difference from the one below it.
fn fork_phase() {
    let checkpoints = checkpoints_from(std::env::args().nth(2));
    let threads: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(64);
    let reps: usize = std::env::args().nth(4).and_then(|s| s.parse().ok()).unwrap_or(120);

    let mut db = Db::new();
    let mut boot = db.session();
    db.ok("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);", &mut boot);
    drop(boot);

    println!("S15 phase 3: where the per-statement `BEGIN AGENT SESSION` cost lives, at N branches.");
    println!("Single-threaded, {reps} reps each, the three layers INTERLEAVED so device drift hits all three.");
    println!("Sessions are abandoned as they are taken, so `State::workspaces` stays at ~1 and cannot");
    println!("be what is being measured. `syncs` is catalog fsyncs issued across the whole rep block.");
    println!();
    println!(
        "{:>9} {:>12} {:>12} {:>12} {:>12} {:>12} {:>10}",
        "N", "raw ms", "rt ms", "stmt ms", "rt-raw ms", "stmt-rt ms", "syncs/op",
    );

    let lease = LeaseDeadline(u64::MAX);
    let mut done = 0usize;
    let mut seq = 0usize;

    for &target in &checkpoints {
        if target <= done {
            continue;
        }
        let per = (target - done) / threads.max(1);
        let actually = per * threads;
        let cat = Arc::clone(&db.cat);
        std::thread::scope(|s| {
            for _ in 0..threads {
                let cat = Arc::clone(&cat);
                s.spawn(move || {
                    for _ in 0..per {
                        cat.fork(BranchId::TRUNK, lease).expect("fork");
                    }
                });
            }
        });
        done += actually;

        let syncs_before = db.cat.syncs_issued();
        let (mut raw, mut rt, mut stmt) = (0f64, 0f64, 0f64);
        for _ in 0..reps {
            let t = Instant::now();
            db.cat.fork(BranchId::TRUNK, lease).expect("fork");
            raw += t.elapsed().as_secs_f64();

            seq += 1;
            let t = Instant::now();
            let s = db.runtime.begin_session("a", Some(&format!("rt_{seq}")), BranchId::TRUNK)
                .expect("begin_session");
            rt += t.elapsed().as_secs_f64();
            db.runtime.abandon(s.branch).expect("abandon");

            seq += 1;
            let mut sess = db.session();
            let sql = format!("BEGIN AGENT SESSION AS 'a' RUN 'st_{seq}';");
            let t = Instant::now();
            db.ok(&sql, &mut sess);
            stmt += t.elapsed().as_secs_f64();
            let b = sess.agent.as_ref().unwrap().branch_name.clone();
            db.ok(&format!("ABANDON BRANCH {b};"), &mut sess);
        }
        let syncs = db.cat.syncs_issued() - syncs_before;

        let (raw, rt, stmt) = (
            raw * 1000.0 / reps as f64,
            rt * 1000.0 / reps as f64,
            stmt * 1000.0 / reps as f64,
        );
        println!(
            "{:>9} {:>12.3} {:>12.3} {:>12.3} {:>12.3} {:>12.3} {:>10.2}",
            done,
            raw,
            rt,
            stmt,
            rt - raw,
            stmt - rt,
            syncs as f64 / (reps * 3) as f64,
        );
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    println!();
    println!("`rt - raw` is what the RUNTIME adds over the catalog: interning the run, building the");
    println!("Workspace, the name and the capture. `stmt - rt` is scanner + parser + binder + dispatch.");
    println!("If `raw` carries the whole rise, the per-statement cost is the catalog's and the device's,");
    println!("and nothing above the catalog scales with branch count.");
}

// =================================================================================================
// phase 2 — N agent sessions held open through the runtime
// =================================================================================================

fn session_phase() {
    let checkpoints = checkpoints_from(std::env::args().nth(2));
    let threads: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(64);

    let mut db = Db::new();
    let mut boot = db.session();
    db.ok("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);", &mut boot);
    db.ok("INSERT INTO inv VALUES (1, 10);", &mut boot);
    drop(boot);

    println!("S15 phase 2: N agent sessions opened through the RUNTIME, all held open.");
    println!();
    println!("**Which function, exactly.** The bulk loop calls `AgentRuntime::begin_session_as` — the");
    println!("body `BEGIN AGENT SESSION` reaches through `run_agent_stmt`, NOT the catalog's `fork`.");
    println!("It is called directly rather than through the scanner because the statement path needs");
    println!("`&mut Catalog` and so cannot be driven from {threads} threads, and a SEQUENTIAL loop is a");
    println!("measurement of this device's fsync (~13 ms/fork, so 10^6 would be ~4 hours) rather than");
    println!("of the runtime. The `stmt ms` column re-measures the WHOLE statement through the parser");
    println!("at each checkpoint, so the gap the shortcut opens is reported rather than assumed.");
    println!();
    println!("Every one of these interns a run and installs a Workspace, a name and a capture in");
    println!("`AgentRuntime::State`, none of which is dropped while the session is open.");
    println!();
    println!(
        "{:>9} {:>14} {:>12} {:>10} {:>12} {:>12} {:>12} {:>12}",
        "N", "seg sess/sec", "stmt ms", "RSS MB", "B/session", "resolve us", "forget ms", "activity ms",
    );

    let mut done = 0usize;
    let base_rss = peak_rss_bytes();
    // Held so the runtime's per-session state cannot be dropped behind the measurement's back.
    let mut held: Vec<ferrodb::agent_sql::session::AgentSession> = Vec::new();

    for &target in &checkpoints {
        if target <= done {
            continue;
        }
        let seg = target - done;
        let per = seg / threads.max(1);
        let actually = per * threads;
        let base = done;

        let t0 = Instant::now();
        let mut opened: Vec<Vec<ferrodb::agent_sql::session::AgentSession>> =
            std::thread::scope(|s| {
                let mut hs = Vec::new();
                for t in 0..threads {
                    let rt = Arc::clone(&db.runtime);
                    hs.push(s.spawn(move || {
                        let mut mine = Vec::with_capacity(per);
                        for i in 0..per {
                            // A DISTINCT run id per task, which is the realistic shape: one agent
                            // task is one run. A repeated `(agent, run)` would measure a HashMap
                            // hit in the intern table instead of a new entity.
                            let run = format!("r_{}", base + t * per + i);
                            mine.push(
                                rt.begin_session(
                                    "a",
                                    Some(&run),
                                    ferrodb::branch::types::BranchId::TRUNK,
                                )
                                .expect("begin session"),
                            );
                        }
                        mine
                    }));
                }
                hs.into_iter().map(|h| h.join().unwrap()).collect()
            });
        let secs = t0.elapsed().as_secs_f64();
        for v in opened.drain(..) {
            held.extend(v);
        }
        done += actually;

        // The WHOLE statement, through scanner -> parser -> binder -> executor, once, so the
        // shortcut above is checked against the thing it stands in for.
        let mut s = db.session();
        let t = Instant::now();
        db.ok(&format!("BEGIN AGENT SESSION AS 'a' RUN 'stmt_{done}';"), &mut s);
        let stmt_ms = t.elapsed().as_secs_f64() * 1000.0;
        let name = s.agent.as_ref().unwrap().branch_name.clone();

        // Name resolution: `AS OF BRANCH b_k` goes through `State::names`, a BTreeMap that now
        // holds one entry per open session.
        let sql = format!("SELECT qty FROM inv AS OF BRANCH {name} WHERE id = 1;");
        let mut reader = db.session();
        let (resolve_ms, rows) = time_ms(200, || db.rows(&sql, &mut reader));
        assert_eq!(rows, 1, "AS OF BRANCH against a held session stopped answering");

        // `ferro_run_activity` walks `State::workspaces`, which is exactly what just grew.
        let (act_ms, act_rows) =
            time_ms(1, || db.rows("SELECT * FROM ferro_run_activity;", &mut reader));
        assert!(act_rows >= done, "ferro_run_activity reported {act_rows} of {done} sessions");

        // The lease thread's sweep. It runs on a timer against exactly this map.
        let t = Instant::now();
        let forgotten = db.runtime.forget_reaped_branches();
        let forget_ms = t.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(forgotten, 0, "a live session was forgotten");

        let rss = peak_rss_bytes();
        println!(
            "{:>9} {:>14.1} {:>12.3} {:>10.1} {:>12.0} {:>12.1} {:>12.2} {:>12.2}",
            done,
            actually as f64 / secs,
            stmt_ms,
            rss as f64 / 1e6,
            (rss.saturating_sub(base_rss)) as f64 / done as f64,
            resolve_ms * 1000.0,
            forget_ms,
            act_ms,
        );
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    println!();
    println!("B/session is MARGINAL RSS over the whole run divided by N, so it carries the catalog's");
    println!("own growth too (266 B/branch, bench/curve_to_1e6.txt). Subtract that for the runtime's share.");
}
