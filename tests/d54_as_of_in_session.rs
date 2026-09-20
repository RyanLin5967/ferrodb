//! D54 — `AS OF BRANCH` inside an agent session must read the NAMED branch.
//!
//! The shared read path introduced by D54 bypasses `executor::run`, and `run` routes an
//! `AS OF BRANCH` select through `run_agent_stmt`, whose arm reads the **named** branch:
//!
//! ```ignore
//! BoundAgentStmt::SelectAsOf { branch, stmt } => runtime.select(&ctx.read(), branch, &stmt, current)
//! ```
//!
//! `try_run_read`'s in-session arm originally had **no `as_of` guard**, so it caught those
//! statements and read `session.agent.branch` — the session's OWN branch — instead. Silently wrong
//! rows, never an error.
//!
//! ⚠ This goes through `pgwire::extended`, not through a `Db::exec` fixture, **on purpose**. Most
//! of this repo's tests drive `executor::run` directly and never reach `try_run_read` at all,
//! which is exactly why the bug could exist with a green suite. A test that cannot reach the code
//! it is about is not coverage.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::session::Session;
use ferrodb::pgwire::extended::{Connection, Statement};
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn server(dir: &tempfile::TempDir) -> Arc<ServerContext> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("asof.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("asof.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    Arc::new(ServerContext::new(catalog, bp, txn, Arc::new(AgentRuntime::new())))
}

fn conn(ctx: &Arc<ServerContext>) -> Connection {
    let c = Connection::new(Session::with_runtime(Arc::clone(&ctx.runtime)));
    ctx.register_reader(c.read_slot());
    c
}

/// Run one statement the way a client does: parse, then execute.
fn run(ctx: &Arc<ServerContext>, c: &mut Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut stmts = Statement::parse_batch(sql, c, ctx).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
    let s = stmts.remove(0);
    s.execute(c, ctx, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

fn one_int(rows: &[Vec<Value>], sql: &str) -> i32 {
    assert_eq!(rows.len(), 1, "{sql} returned {} rows, expected 1", rows.len());
    match rows[0][0] {
        Value::Integer(i) => i,
        ref other => panic!("{sql} returned {other:?}, expected an integer"),
    }
}

#[test]
fn as_of_branch_inside_a_session_reads_the_named_branch_not_the_session_s_own() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = server(&dir);

    let mut setup = conn(&ctx);
    run(&ctx, &mut setup, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    run(&ctx, &mut setup, "INSERT INTO t VALUES (1, 7);");

    let mut a = conn(&ctx);
    let mut b = conn(&ctx);
    run(&ctx, &mut a, "BEGIN AGENT SESSION AS 'agent-a' RUN 'r1';");
    run(&ctx, &mut b, "BEGIN AGENT SESSION AS 'agent-b' RUN 'r2';");
    let b_name = b.session.agent.as_ref().expect("b is in a session").branch_name.clone();

    // Each branch takes the row to a DIFFERENT value, so reading the wrong one is visible in the
    // answer rather than only in a pointer.
    run(&ctx, &mut a, "UPDATE t SET v = 111 WHERE id = 1;");
    run(&ctx, &mut b, "UPDATE t SET v = 222 WHERE id = 1;");

    // A reads its own branch: 111.
    let own = run(&ctx, &mut a, "SELECT v FROM t WHERE id = 1;");
    assert_eq!(one_int(&own, "own-branch select"), 111);

    // A reads B's branch BY NAME, from inside its own session. This is the statement the shared
    // read path used to answer with A's branch.
    let sql = format!("SELECT v FROM t AS OF BRANCH {b_name} WHERE id = 1;");
    let other = run(&ctx, &mut a, &sql);
    assert_eq!(
        one_int(&other, &sql),
        222,
        "AS OF BRANCH {b_name} returned the SESSION's branch instead of the named one -- this is \
         the D54 shared-read-path bug: try_run_read's in-session arm matched a select whose \
         from.as_of was Some and read session.agent.branch"
    );
}
