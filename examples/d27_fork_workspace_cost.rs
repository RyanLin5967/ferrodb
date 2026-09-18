//! D27 — what one fork costs when the parent is a live agent task holding staged rows.
//!
//! `AgentRuntime::begin_session_as` deep-clones five maps out of the parent's `Workspace`
//! (`rows`, `base_rows`, `tables`, `schema_edits`, `base_shapes`). Those hold the parent's
//! **uncommitted staged working set** — the rows it has touched and their fork-point images — not
//! the whole table. So one fork is O(W) in the parent's working set W, and N children of one live
//! parent are O(N·W).
//!
//! # What this measures, and what it does not
//!
//! **Measures:** ferrodb on this machine, right now. Wall clock via `std::time::Instant` around
//! the `begin_session` calls only, and peak resident set via `getrusage(RUSAGE_SELF).ru_maxrss`.
//! Setup — seeding trunk and staging the parent's W rows — is timed separately and is NOT part of
//! the fork number.
//!
//! **Does NOT measure:** anything about merge, about the page store, or about any other system.
//! The runtime here is `AgentRuntime::new()`, the map-backed form with no page store, because the
//! five maps are the subject and a tree underneath them would add a cost this is not asking about.
//!
//! # Why one configuration per process
//!
//! `ru_maxrss` is a high-water mark and never falls. Sweeping W and N inside one process would
//! report the largest configuration's peak for every row after it. So this program runs exactly
//! one (W, N) and exits; the sweep is a shell loop, and each row is its own process.
//!
//!     cargo run --release --example d27_fork_workspace_cost -- <W> <N>

use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::types::BranchId;
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

/// `getrusage(RUSAGE_SELF).ru_maxrss` — bytes on macOS, kilobytes on Linux.
///
/// Same shape as `examples/branch_curve.rs`, deliberately: one already-used instrument rather
/// than a second one that might disagree with it.
/// `Option`, not `0`: the old signature returned `0` both when `getrusage` FAILED and, once this
/// harness was built on Windows, when the platform had no `getrusage` at all. A zero is
/// indistinguishable from a real measurement of a tiny process. Callers render `None` as `NaN`.
///
/// Platform split gated the way `storage::disk_manager::pwrite` already gates its -- the repo's
/// existing pattern. Linking `getrusage` on windows-latest fails with `LNK2019`, which broke CI
/// for every example in this directory.
fn peak_rss_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
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
            return None;
        }
        Some(if cfg!(target_os = "macos") { u.ru_maxrss as u64 } else { u.ru_maxrss as u64 * 1024 })
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// `peak_rss_bytes()` in megabytes, or `NaN` where the platform cannot answer.
fn peak_rss_mb() -> f64 {
    peak_rss_bytes().map_or(f64::NAN, |b| b as f64 / 1e6)
}

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("d27.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d27.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
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
        match self.exec(sql, s) {
            Ok(o) => o,
            Err(e) => panic!("{sql} failed: {e}"),
        }
    }
}

fn main() {
    // **Parsed strictly, with no defaults.** This refused nothing and fell back to (1000, 64) in
    // its first version, and a zsh sweep that passed `"1000 16"` as ONE argument therefore
    // produced nine identical rows that read exactly like a real measurement showing no
    // dependence on W or N. A benchmark that cannot parse its own arguments must refuse, not
    // measure some other configuration and label it with the one it was asked for.
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        eprintln!(
            "d27: expected exactly 2 arguments (W N), got {}: {:?}\n\
             usage: d27_fork_workspace_cost <staged-rows-on-parent> <children-forked>",
            args.len(),
            args
        );
        std::process::exit(2);
    }
    let parse = |label: &str, s: &String| -> usize {
        match s.parse::<usize>() {
            // A run that collected nothing has not passed. Zero is refused, not reported as 0.000.
            Ok(0) => {
                eprintln!("d27: {label} must be > 0, got 0; nothing to measure");
                std::process::exit(2);
            }
            Ok(v) => v,
            Err(e) => {
                eprintln!("d27: {label} is not a positive integer: {s:?} ({e})");
                std::process::exit(2);
            }
        }
    };
    let w = parse("W", &args[0]);
    let n = parse("N", &args[1]);
    if !cfg!(not(debug_assertions)) {
        eprintln!("d27: debug build — these numbers are meaningless. Use --release.");
        std::process::exit(2);
    }

    let setup = Instant::now();
    let mut db = Db::new();
    let mut trunk = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER, note VARCHAR(32));", &mut trunk);
    for i in 0..w {
        db.ok(
            &format!("INSERT INTO inventory VALUES ({i}, {i}, 'widget-{i}');"),
            &mut trunk,
        );
    }
    let seed_s = setup.elapsed().as_secs_f64();

    // The parent is a LIVE agent task with W rows staged and uncommitted. One statement, so the
    // working set is W and the setup is not quadratic in statement count.
    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut parent);
    // **The write path, timed on its own.** A structure that made forks cheap by making writes
    // expensive would have moved the cost rather than removed it, and the fork columns alone
    // cannot see that. This is W rows into a workspace map nothing else holds.
    let stage = Instant::now();
    db.ok("UPDATE inventory SET qty = qty + 1;", &mut parent);
    let stage_s = stage.elapsed().as_secs_f64();
    let parent_branch: BranchId = parent.agent.as_ref().unwrap().branch;
    let staged = db
        .runtime
        .run_activity()
        .into_iter()
        .find(|a| a.branch == parent_branch)
        .expect("parent workspace is live")
        .staged_rows;
    assert_eq!(staged as usize, w, "parent should hold exactly W staged rows, holds {staged}");

    let rss_before = peak_rss_bytes();

    // ---- the measurement: N children off that one parent -------------------------------------
    let mut kids = Vec::with_capacity(n);
    let t = Instant::now();
    for i in 0..n {
        kids.push(
            db.runtime
                .begin_session("sub", Some(&format!("r_{i}")), parent_branch)
                .expect("fork"),
        );
    }
    let fork_s = t.elapsed().as_secs_f64();

    let rss_after = peak_rss_bytes();
    // Never let the children be optimised away, and prove each one really carries the parent's
    // working set: a fork that copied nothing would make this whole measurement a measurement of
    // an empty loop.
    let activity = db.runtime.run_activity();
    let mut total_inherited = 0usize;
    for k in &kids {
        total_inherited += activity
            .iter()
            .find(|a| a.branch == k.branch)
            .expect("child workspace is live")
            .staged_rows as usize;
    }
    assert_eq!(
        total_inherited,
        n * w,
        "every child must see the parent's whole working set; saw {total_inherited} not {}",
        n * w
    );

    // **The other half of the write path: a write into a map that IS shared.** The last child's
    // workspace shares every node with its parent and its N-1 siblings, so this statement forces
    // a path copy for each of its W rows -- the worst case for the new structure, and the one a
    // fork-only measurement would miss entirely.
    let mut kid = db.session();
    kid.agent = Some(kids.last().expect("at least one child").clone());
    let t = Instant::now();
    let touched = db.ok("UPDATE inventory SET qty = qty + 5;", &mut kid);
    let child_write_s = t.elapsed().as_secs_f64();
    drop(touched);

    println!(
        "{w}\t{n}\t{}\t{seed_s:.3}\t{stage_s:.6}\t{fork_s:.6}\t{:.3}\t{child_write_s:.6}\t{:.1}\t{:.1}",
        n * w,
        (fork_s * 1e6) / n as f64,
        peak_rss_mb(),
        match (rss_after, rss_before) {
            (Some(a), Some(b)) => a.saturating_sub(b) as f64 / 1e6,
            _ => f64::NAN,
        },
    );
}
