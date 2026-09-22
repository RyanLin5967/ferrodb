//! D56 — is a point read O(table) or O(log N)? Measure the SHAPE, not a single ratio.
//!
//! D55 ended by withdrawing a complexity claim it could not support: the pushdown measured ×1.44
//! where O(table)→O(log N) on 5,000 rows should have been an order of magnitude, and `EXPLAIN`
//! then named why — without `ANALYZE` the cost model assumes `DEFAULT_TABLE_ROWS = 1000` and a
//! sequential scan wins (`bench/d55_explain_before_after_analyze.txt`). D56 gives the planner the
//! one fact it already had: column 0 is the primary key, so `id = k` matches at most one row.
//!
//! **A single before/after ratio cannot tell a complexity-class change from a constant**, and this
//! machine is never quiet, so one ratio is also the number noise damages most. The instrument here
//! is therefore a CURVE over table size, inside one arm:
//!
//! * O(table) ⇒ throughput falls roughly 1/N as the table grows.
//! * O(log N) ⇒ throughput is ~flat.
//!
//! A slope is robust against a machine that drifts: a load spike moves every point of an arm, and
//! the shape survives. Run the same binary against the fix and against its parent to compare.
//!
//! Arms (both measured, because D55's retracted "33×" compared a 5,000-row agent read against a
//! 200-row plain read — the two paths must sit at ONE row count to be compared at all):
//!
//! * `agent` — a `BEGIN AGENT SESSION` branch with 10 staged rows, reading a row it did NOT stage.
//! * `plain` — the same SELECT with no agent session.
//!
//! Env: `D56_ROWS` (default 5000), `D56_ANALYZE=1` (the ceiling arm: the plan the engine already
//! trusted), `D56_ARM=agent|plain|both`, `D56_LABEL`, and `D56_STAGED` (default 10) — the rows the
//! agent branch stages before reading. D57 sweeps THIS axis at a fixed table size: `visible_rows_where`
//! walks every staged row under the State mutex per read, so O(W) shows as ~1/W across it.
//!
//! Refuses rather than reporting: zero iterations, or a statement that did not return exactly one
//! row — a read that found nothing would otherwise look like a very fast read.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::cow::PageStore;
use ferrodb::tel::MemEffectLog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, try_run_read, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const WARMUP: Duration = Duration::from_millis(300);
const MEASURE: Duration = Duration::from_millis(1000);
const ROUNDS: usize = 3;

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

/// One statement through the server's own dispatch — the shared read path first, the exclusive
/// lock only for what `try_run_read` refuses. Mirrors `pgwire::extended`, as D55's harness does.
fn exec(
    s: &Server,
    sql: &str,
    sess: &mut Session,
    cache: &mut Option<(u64, Arc<Catalog>)>,
    slot: &AtomicBool,
) -> usize {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    assert!(parser.errors.is_empty(), "parse failed for {sql}: {:?}", parser.errors);
    let stmt = stmts.remove(0);

    let outcome = {
        let shared = s.ctx.read_catalog(cache);
        let attempted = match s.ctx.begin_read(slot) {
            Some(_pass) => try_run_read(&stmt, shared, s.bp.clone(), s.txn.clone(), sess),
            None => None,
        };
        match attempted {
            Some(read) => read,
            None => {
                let mut cat = s.ctx.catalog();
                let o = run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess);
                drop(cat);
                o
            }
        }
    };
    match outcome {
        Ok(Outcome::Rows(r)) => r.len(),
        Ok(Outcome::Table(t)) => t.rows.len(),
        Ok(_) => 0,
        Err(e) => panic!("{sql} failed: {e}"),
    }
}

static BUILD_SEQ: AtomicU64 = AtomicU64::new(0);

/// Fresh directory per build — a reused one leaves the previous point's branch-catalog sidecar
/// beside a truncated `main.db`, which fails with "page 1 is not a branch-catalog header".
fn build(dir: &std::path::Path, rows: i64, analyze: bool) -> Server {
    let seq = BUILD_SEQ.fetch_add(1, Ordering::Relaxed);
    let d = dir.join(format!("s{seq}"));
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
    // D101 — the SHIPPED wiring: `with_storage` over a real `ArenaPageStore`, not `with_catalog`.
    // `with_catalog` delegates to `with_parts`, which sets `storage: None, reaper: None`, so the
    // branch STORAGE engine — arena, CoW pages, reaper — was absent entirely and agent writes went
    // to an in-memory effect log. `src/cli/cli.rs` builds `with_storage`/`reopen_with_storage`.
    // The arena floor must sit ABOVE where the ordinary table grows to, or the build runs out of
    // pages below the reserved region; sized off the row count for the reason D90 records.
    let arena_base: u32 = ((rows / 40) as u32 + 4096).next_power_of_two();
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), arena_base).unwrap());
    // D101 — ARM PERSISTENCE, because production does and this harness did not.
    // `ArenaPageStore` persists its free-space map through `persist_if_configured`, which is a
    // NO-OP until a checkpoint path is set. `src/cli/cli.rs:120` and `examples/pgserver.rs:101`
    // both set one, so a run without it measures a configuration nobody ships — and it measures
    // it in the flattering direction, since the persistence work is simply skipped.
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

    let mut cache = None;
    let slot = AtomicBool::new(false);
    // D101 — `s.ctx.session()`, NEVER `Session::new()`. `Session::new` builds its OWN
    // `AgentRuntime::new()` (`storage: None`, private in-memory branch catalog, private
    // effect log), so every agent statement below would run on a stub and the arena/durable
    // catalog built above would be constructed and never touched. `agent_sql::designated`
    // now refuses this rather than measuring it.
    let mut sess = s.ctx.session();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess, &mut cache, &slot);
    for i in 1..=rows {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess, &mut cache, &slot);
    }
    if analyze {
        exec(&s, "ANALYZE t;", &mut sess, &mut cache, &slot);
    }
    s
}

/// stmt/s for one point, as the MEDIAN of `ROUNDS` timed windows. Refuses on a window that did no
/// work, or on a statement that did not return exactly one row.
fn measure(s: &Server, agent: bool, rows: i64, staged: usize, demote: bool) -> (f64, Vec<f64>) {
    let mut sess = s.ctx.session();
    let mut cache: Option<(u64, Arc<Catalog>)> = None;
    let slot = Arc::new(AtomicBool::new(false));
    s.ctx.register_reader(Arc::clone(&slot));

    if agent {
        exec(s, "BEGIN AGENT SESSION AS 'a0' RUN 'r0';", &mut sess, &mut cache, &slot);
        assert!(
            (staged as i64) < rows,
            "REFUSING: staged={staged} would stage the key under test (rows={rows}); the read must hit the base table"
        );
        for i in 0..staged {
            let id = (i as i64) % rows + 1;
            exec(
                s,
                &format!("UPDATE t SET v = {} WHERE id = {id};", 900000 + i),
                &mut sess,
                &mut cache,
                &slot,
            );
            }

        // **D167 — the DEMOTED arm.** One PK-moving UPDATE makes `Workspace::unprobeable_rows`
        // non-zero, and `visible_rows_where` then WALKS instead of probing for every read on this
        // branch (`runtime.rs:2172` gates on `== 0`).
        //
        // Why a PK move and not a variant mismatch: both doors reach the same counter, but
        // `git grep -nE "VALUES \([0-9]+\.[0-9]"` finds the variant door only inside d57's own
        // tests, while the PK move is the plausible one — trunk REFUSES it (`value_fits`,
        // `tuple.rs:47`, reached only through `Tuple::serialize`) so an agent must reach for a
        // branch, and the branch path (`paged_rows::encode_row`) takes `&[Value]` and never sees
        // the schema. Trunk refuses; branch accepts silently.
        //
        // The moved key is deliberately OUTSIDE the table and is NOT the key under test: the read
        // must still come from the base table, so the two arms differ in ONE thing only — whether
        // the probe is enabled. That is the control.
        if demote {
            let victim = 1i64;
            assert!(victim != rows, "the demoting UPDATE must not move the key under test");
            exec(
                s,
                &format!("UPDATE t SET id = {} WHERE id = {victim};", rows + 1_000_000),
                &mut sess,
                &mut cache,
                &slot,
            );
        }
    }

    // Read a row this branch did NOT stage: the answer comes from the base table.
    let key = rows;
    let sql = format!("SELECT v FROM t WHERE id = {key};");

    let t0 = Instant::now();
    while t0.elapsed() < WARMUP {
        exec(s, &sql, &mut sess, &mut cache, &slot);
    }

    let mut per_round = Vec::new();
    for _ in 0..ROUNDS {
        let start = Instant::now();
        let (mut n, mut r) = (0u64, 0u64);
        while start.elapsed() < MEASURE {
            r += exec(s, &sql, &mut sess, &mut cache, &slot) as u64;
            n += 1;
        }
        let secs = start.elapsed().as_secs_f64();
        assert!(n > 0, "REFUSING: a timed window did no work");
        assert_eq!(r, n, "REFUSING: {n} statements returned {r} rows; each must return exactly 1");
        per_round.push(n as f64 / secs);
    }
    let mut sorted = per_round.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (sorted[sorted.len() / 2], per_round)
}

fn main() {
    let rows: i64 = std::env::var("D56_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let analyze = std::env::var("D56_ANALYZE").is_ok();
    let arm = std::env::var("D56_ARM").unwrap_or_else(|_| "both".into());
    let staged: usize = std::env::var("D56_STAGED").ok().and_then(|v| v.parse().ok()).unwrap_or(10);
    // D167: when set, one PK-moving UPDATE demotes the branch so every read WALKS the overlay.
    let demote = std::env::var("D56_DEMOTE").map(|v| v == "1").unwrap_or(false);
    let label = std::env::var("D56_LABEL").unwrap_or_else(|_| "unlabelled".into());
    let dir = std::env::temp_dir().join(format!("ferrodb-d56-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    for (name, is_agent) in [("agent", true), ("plain", false)] {
        if arm != "both" && arm != name {
            continue;
        }
        let s = build(&dir, rows, analyze);
        let (median, per_round) = measure(&s, is_agent, rows, staged, demote);
        let detail: Vec<String> = per_round.iter().map(|v| format!("{v:.0}")).collect();
        println!(
            "{label}  arm={name}  rows={rows}  staged={staged}  demote={demote}  analyze={analyze}  median={median:.0} stmt/s  rounds=[{}]",
            detail.join(", ")
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
