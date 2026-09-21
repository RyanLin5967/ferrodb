//! D101 — the guard that makes "built the real engine, then measured an in-memory stub"
//! unrepresentable at execution time.
//!
//! **This file is the proof that the guard fires, and the proof that it does not fire on a correct
//! harness.** A guard that has never been seen to refuse is not a guard, and one that has only
//! been seen to refuse has not been shown to permit. Both arms below build the SAME server —
//! a real catalog, a real WAL, a real `TableBranchCatalog` sidecar and a storage-backed
//! `AgentRuntime` over an `ArenaPageStore` — and differ in exactly one line: how the session is
//! constructed. That is the whole point. The difference between a benchmark that measures the
//! engine and one that measures a stub was one constructor call, and it was invisible from the
//! outside.
//!
//! ⚠ Its own test binary on purpose. The designation registry is process-wide (it has to be: the
//! real server designates on the main thread and runs statements on connection threads), so a
//! `ServerContext` built here would otherwise designate a runtime for every unrelated test sharing
//! the process. The complementary case — a process that designates NOTHING must not be refused —
//! is in `tests/d101_no_server_context.rs` for the same reason, from the other side.

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

/// The wiring `examples/d90_delta_vs_chunk.rs` and `src/cli/cli.rs` use: a storage-backed runtime
/// over a real arena, handed to a `ServerContext`.
fn build(dir: &Path) -> Server {
    std::fs::create_dir_all(dir).unwrap();
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
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&dir.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), 4096).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            store as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    Server { ctx, bp, txn }
}

fn exec(s: &Server, sql: &str, sess: &mut Session) -> Result<(), String> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "{sql}: {:?}", p.errors);
    let stmt = stmts.remove(0);
    let mut cat = s.ctx.catalog();
    let out = run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess);
    drop(cat);
    out.map(|_| ()).map_err(|e| e.to_string())
}

/// ⛔ THE FIRE CHECK. The exact shape of the defect, made to happen on purpose.
///
/// A `ServerContext` over a real arena runtime, and then `Session::new()` — which is what
/// `d55_agent_read_scaling`, `d56_plan_curve`, `d67_merge_contention` and `d68_merge_is_o_table`
/// all did. Before this guard existed the `BEGIN AGENT SESSION` below **succeeded**, against a
/// private in-memory runtime built milliseconds earlier, and every number the harness then printed
/// described that stub.
#[test]
fn an_agent_statement_on_an_undesignated_runtime_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let s = build(dir.path());

    // Ordinary DDL and DML are NOT refused: they never reach the agent layer, so which runtime the
    // session holds is irrelevant to them. Asserting this pins the guard's SCOPE — a guard that
    // refused every statement would satisfy the refusal assertion below while being useless, and
    // this is the assertion that tells the two apart.
    let mut stub = Session::new();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut stub)
        .expect("ordinary DDL does not touch the agent runtime and must not be refused");
    exec(&s, "INSERT INTO t VALUES (1, 10);", &mut stub)
        .expect("ordinary DML does not touch the agent runtime and must not be refused");

    let err = exec(&s, "BEGIN AGENT SESSION AS 'd101' RUN 'r0';", &mut stub)
        .expect_err("BEGIN AGENT SESSION on a Session::new() beside a ServerContext must REFUSE");
    assert!(
        err.contains("no ServerContext designated"),
        "the refusal must name what is wrong: {err}"
    );
    assert!(
        err.contains("Session::with_runtime"),
        "the refusal must say what to do instead: {err}"
    );

    // Every other door into the agent layer, so the guard cannot be walked around by reaching for
    // a different verb. `AS OF BRANCH` is the one that needs no open session at all, and it is the
    // door `is_agent_stmt` opens for a plain SELECT.
    for sql in ["SELECT * FROM t AS OF BRANCH 1;", "DIFF;", "MERGE;", "ABANDON;", "REVERT MERGE m_1;"] {
        let mut stub = Session::new();
        let err = exec(&s, sql, &mut stub)
            .expect_err(&format!("{sql} reaches the agent layer and must be refused"));
        assert!(
            err.contains("no ServerContext designated"),
            "{sql} was refused for the WRONG reason — the guard must be reached before binding, \
             or a later refusal could mask its absence: {err}"
        );
    }
}

/// The other half, and the half that decides whether the guard is keepable: the SAME server, the
/// same statements, and `Session::with_runtime` instead. Nothing is refused.
#[test]
fn a_correct_harness_is_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    let s = build(dir.path());
    let mut sess = Session::with_runtime(Arc::clone(&s.ctx.runtime));

    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=20 {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess).unwrap();
    }
    exec(&s, "BEGIN AGENT SESSION AS 'd101' RUN 'r0';", &mut sess)
        .expect("the designated runtime must be permitted");
    exec(&s, "UPDATE t SET v = 99 WHERE id = 3;", &mut sess).expect("a staged write");
    exec(&s, "DIFF;", &mut sess).expect("DIFF on the designated runtime");
    exec(&s, "MERGE;", &mut sess).expect("MERGE on the designated runtime");

    // And a second, independent session on the same context — the shape the real server uses for
    // every connection (`pgwire/mod.rs`), built through the constructor that exists so the right
    // thing is also the short thing.
    let mut second = s.ctx.session();
    exec(&s, "BEGIN AGENT SESSION AS 'd101' RUN 'r1';", &mut second)
        .expect("ServerContext::session() must produce a permitted session");
    exec(&s, "ABANDON;", &mut second).expect("ABANDON on the designated runtime");
}
