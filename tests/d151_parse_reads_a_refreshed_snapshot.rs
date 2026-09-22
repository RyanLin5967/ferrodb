//! D151 — PARSING now reads the per-connection SNAPSHOT, so parse-time correctness depends on the
//! schema epoch for the first time. This test is about that new dependency and nothing else.
//!
//! Before D151, `Statement::parse_one` took the EXCLUSIVE catalog (`ServerContext::catalog()`), so
//! `params::infer_types` and `describe_stmt` could not possibly see a stale schema — they held the
//! live one. D151 replaced that with `ServerContext::read_catalog`, which returns a cached
//! `Arc<Catalog>` and re-snapshots only when `Catalog::epoch` moves. **The whole correctness
//! argument now rests on the epoch moving on every schema change**, and on a connection that
//! cached BEFORE someone else's DDL picking the new one up at its next parse.
//!
//! `tests/d54_as_of_in_session.rs` already drives `parse_batch` and does `CREATE TABLE` then
//! `INSERT` on one connection, so the same-connection case is covered. **The case that is not**,
//! and the one D151 introduces, is a connection whose snapshot was taken and cached BEFORE another
//! connection's DDL. That is what this file pins.
//!
//! # MAKING IT FIRE
//!
//! A test that only asserts the post-DDL statement parses proves nothing: a parse path that never
//! consulted the catalog at all would pass it. So the same assertions run TWICE — once before the
//! DDL, where they must FAIL, and once after, where they must succeed. The pre-DDL failure is the
//! exact shape a stale snapshot would produce, which is what makes the post-DDL success evidence
//! that the snapshot was refreshed rather than evidence that nothing was checked.

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
        .open(dir.path().join("d151.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("d151.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    Arc::new(ServerContext::new(catalog, bp, txn, Arc::new(AgentRuntime::new())))
}

fn conn(ctx: &Arc<ServerContext>) -> Connection {
    let c = Connection::new(Session::with_runtime(Arc::clone(&ctx.runtime)));
    ctx.register_reader(c.read_slot());
    c
}

/// Parse one statement the way a client does, and hand back the `Statement` or the error text.
///
/// Deliberately stops at PARSE. This file is about what the parse path can see, and running the
/// statement would let a correct execution mask a parse that saw the wrong schema.
fn parse(
    ctx: &Arc<ServerContext>,
    c: &mut Connection,
    sql: &str,
) -> Result<Statement, String> {
    let mut stmts = Statement::parse_batch(sql, c, ctx).map_err(|e| e.to_string())?;
    assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
    Ok(stmts.remove(0))
}

/// Parse and run, for the setup connection where both must succeed.
fn run(ctx: &Arc<ServerContext>, c: &mut Connection, sql: &str) -> Vec<Vec<Value>> {
    let s = parse(ctx, c, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    s.execute(c, ctx, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

#[test]
fn a_parse_on_a_cached_connection_sees_ddl_another_connection_did() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = server(&dir);

    let mut setup = conn(&ctx);
    run(&ctx, &mut setup, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    run(&ctx, &mut setup, "INSERT INTO t VALUES (1, 7);");

    // `reader` takes and caches a snapshot HERE, at the epoch that has `t` with two columns and no
    // table called `later`. Everything this test is about happens to this connection afterwards.
    let mut reader = conn(&ctx);
    let rows = run(&ctx, &mut reader, "SELECT v FROM t;");
    assert_eq!(rows.len(), 1, "the seed row must be readable before the DDL");

    // ---- THE FIRE-CHECK. Both statements must FAIL right now, and the failure is the same shape
    // ---- a stale snapshot would produce after the DDL below.
    let before_table = parse(&ctx, &mut reader, "SELECT id FROM later;");
    assert!(
        before_table.is_err(),
        "`SELECT id FROM later` parsed before `later` existed. The parse path is not consulting \
         the catalog at all, so nothing below this line would be evidence of anything."
    );
    let before_column = parse(&ctx, &mut reader, "SELECT note FROM t;");
    assert!(
        before_column.is_err(),
        "`SELECT note FROM t` parsed before the column existed — same problem as above, for the \
         ALTER half of this test."
    );

    // ---- The DDL, on a DIFFERENT connection. `reader`'s cache is not touched by this; only the
    // ---- shared epoch moves, which is the mechanism under test.
    run(&ctx, &mut setup, "CREATE TABLE later (id INTEGER NOT NULL);");
    run(&ctx, &mut setup, "ALTER TABLE t ADD COLUMN note VARCHAR(20);");

    // ---- The same two statements, on the same connection, whose cache still holds the old
    // ---- snapshot until `read_catalog` notices the epoch moved.
    let after_table = parse(&ctx, &mut reader, "SELECT id FROM later;")
        .unwrap_or_else(|e| panic!(
            "`SELECT id FROM later` still fails to parse after another connection created \
             `later`: {e}. The cached snapshot was not refreshed, so D151 moved a stale schema \
             into the parse path."
        ));
    assert!(
        after_table
            .describe_rows()
            .expect("a SELECT describes its columns")
            .iter()
            .any(|f| f.name == "id"),
        "the new table parsed but its column list does not name `id`"
    );

    let after_column = parse(&ctx, &mut reader, "SELECT note FROM t;")
        .unwrap_or_else(|e| panic!(
            "`SELECT note FROM t` still fails to parse after another connection added the column: \
             {e}. `Catalog::epoch` moves on ALTER (`catalog::alter`'s `epoch_bump`), so this is \
             the snapshot not being re-read rather than the epoch not moving."
        ));
    assert!(
        after_column
            .describe_rows()
            .expect("a SELECT describes its columns")
            .iter()
            .any(|f| f.name == "note"),
        "the altered table parsed but its column list does not name `note`"
    );

    // And the statement that was already legal before the DDL must still be legal after it: a
    // refresh that dropped what the old snapshot held would pass every assertion above.
    let rows = run(&ctx, &mut reader, "SELECT v FROM t;");
    assert_eq!(rows.len(), 1, "the seed row must still be readable after the DDL");
}
