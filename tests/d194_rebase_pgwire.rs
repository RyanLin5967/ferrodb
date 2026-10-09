//! D194 step 4 — `REBASE` over pgwire: the extended protocol (Parse, Bind, Describe, Execute, Sync)
//! and the simple-query path, side by side.
//!
//! The extended protocol announces a statement's columns at Parse/Describe, before it runs, and this
//! server refuses outright when what it produces disagrees with what it described: `Execute` of a
//! statement described as returning nothing that then produces rows is an `XX000`. So `REBASE` has to
//! be in `describe_stmt`'s agent-statement list as well as in the executor, and only a test that
//! drives the real message handlers can see whether it is. These go through
//! `pgwire::extended::dispatch` with real message bodies, as `pgwire::serve` does, not through a
//! `Db::exec` fixture that never reaches that code.
//!
//! Expected results: `bench/d194_fork_snapshot/rebase_prereg.md`, Amendment 2 C.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::session::Session;
use ferrodb::pgwire::extended::{dispatch, Connection, Statement};
use ferrodb::pgwire::message::Message;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// The columns `REBASE` declares, in order.
const COLUMNS: [&str; 8] = [
    "branch",
    "rebased",
    "fork_seq_before",
    "fork_seq_after",
    "moved_rows",
    "moved_premises",
    "moved_shapes",
    "detail",
];

fn server(dir: &tempfile::TempDir) -> Arc<ServerContext> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("rebase_wire.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("rebase_wire.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    Arc::new(ServerContext::new(catalog, bp, txn, Arc::new(AgentRuntime::new())))
}

fn conn(ctx: &Arc<ServerContext>) -> Connection {
    let c = Connection::new(Session::with_runtime(Arc::clone(&ctx.runtime)));
    ctx.register_reader(c.read_slot());
    c
}

/// The simple-query path: parse the string, then execute it, as `d54_as_of_in_session` does.
fn run(ctx: &Arc<ServerContext>, c: &mut Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut stmts = Statement::parse_batch(sql, c, ctx).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
    let s = stmts.remove(0);
    s.execute(c, ctx, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

fn pairs(rows: &[Vec<Value>]) -> Vec<(i32, i32)> {
    let mut v: Vec<(i32, i32)> = rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Integer(id), Value::Integer(q)) => (*id, *q),
            other => panic!("not an (INTEGER, INTEGER) row: {other:?}"),
        })
        .collect();
    v.sort();
    v
}

fn cstr(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

/// `Parse`: the unnamed statement, no declared parameter types.
fn parse_body(sql: &str) -> Vec<u8> {
    let mut b = Vec::new();
    cstr(&mut b, "");
    cstr(&mut b, sql);
    b.extend_from_slice(&0i16.to_be_bytes());
    b
}

/// `Bind`: the unnamed portal over the unnamed statement; no parameter formats, no parameters, no
/// result formats (so every column is text).
fn bind_body() -> Vec<u8> {
    let mut b = Vec::new();
    cstr(&mut b, "");
    cstr(&mut b, "");
    for _ in 0..3 {
        b.extend_from_slice(&0i16.to_be_bytes());
    }
    b
}

/// `Describe` of the unnamed portal.
fn describe_portal_body() -> Vec<u8> {
    let mut b = vec![b'P'];
    cstr(&mut b, "");
    b
}

/// `Execute` of the unnamed portal with no row limit.
fn execute_body() -> Vec<u8> {
    let mut b = Vec::new();
    cstr(&mut b, "");
    b.extend_from_slice(&0i32.to_be_bytes());
    b
}

/// What one extended-protocol round trip put on the wire.
struct Exchange {
    /// Every backend message, in order, by kind (`CommandComplete` and errors carry their text).
    kinds: Vec<String>,
    /// The column names `RowDescription` announced.
    described: Vec<String>,
    /// Each `DataRow`, decoded from text format; `None` is SQL NULL.
    rows: Vec<Vec<Option<String>>>,
}

/// Parse, Bind, Describe (portal), Execute, Sync — the round trip a driver makes.
fn extended(ctx: &Arc<ServerContext>, c: &mut Connection, sql: &str) -> Exchange {
    let mut x = Exchange { kinds: Vec::new(), described: Vec::new(), rows: Vec::new() };
    let steps = [
        (b'P', parse_body(sql)),
        (b'B', bind_body()),
        (b'D', describe_portal_body()),
        (b'E', execute_body()),
        (b'S', Vec::new()),
    ];
    for (tag, body) in steps {
        for m in dispatch(tag, &body, c, ctx).messages {
            match m {
                Message::ParseComplete => x.kinds.push("ParseComplete".into()),
                Message::BindComplete => x.kinds.push("BindComplete".into()),
                Message::NoData => x.kinds.push("NoData".into()),
                Message::RowDescription(fields) => {
                    x.kinds.push("RowDescription".into());
                    x.described = fields.into_iter().map(|f| f.name).collect();
                }
                Message::DataRow(cols) => {
                    x.kinds.push("DataRow".into());
                    x.rows.push(
                        cols.into_iter()
                            .map(|col| col.map(|b| String::from_utf8(b).expect("a text-format value")))
                            .collect(),
                    );
                }
                Message::CommandComplete(tag) => x.kinds.push(format!("CommandComplete({tag})")),
                Message::ErrorResponse { code, message, .. } => {
                    x.kinds.push(format!("ErrorResponse({code}: {message})"))
                }
                Message::ReadyForQuery(_) => x.kinds.push("ReadyForQuery".into()),
                _ => x.kinds.push("other".into()),
            }
        }
    }
    x
}

fn texts(v: &[Option<&str>]) -> Vec<Option<String>> {
    v.iter().map(|s| s.map(|s| s.to_string())).collect()
}

fn seeded(ctx: &Arc<ServerContext>) -> Connection {
    let mut main = conn(ctx);
    run(ctx, &mut main, "CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);");
    run(ctx, &mut main, "INSERT INTO inventory VALUES (1, 20);");
    run(ctx, &mut main, "INSERT INTO inventory VALUES (2, 5);");
    main
}

/// **The extended protocol describes REBASE's columns before it runs, and returns exactly one row
/// under them.** Then the simple-query path returns the same shape, and the branch reads main as of
/// the REBASE. If `describe_stmt` did not know REBASE, Describe would answer NoData and Execute would
/// be refused with `XX000`, which is the pre-registered fire-check.
#[test]
fn rebase_over_the_extended_protocol_describes_and_returns_one_row() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = server(&dir);
    let mut main = seeded(&ctx);
    let mut a = conn(&ctx);
    run(&ctx, &mut a, "BEGIN AGENT SESSION AS 'a' RUN 'r1';");
    run(&ctx, &mut main, "UPDATE inventory SET qty = 50 WHERE id = 2;");
    assert_eq!(
        pairs(&run(&ctx, &mut a, "SELECT id, qty FROM inventory;")),
        vec![(1, 20), (2, 5)],
        "premise: the branch reads as of its fork"
    );

    let x = extended(&ctx, &mut a, "REBASE;");
    assert_eq!(
        x.kinds,
        vec![
            "ParseComplete",
            "BindComplete",
            "RowDescription",
            "DataRow",
            "CommandComplete(SELECT 1)",
            "ReadyForQuery",
        ],
        "the extended-protocol exchange for REBASE is not the one a driver expects"
    );
    assert_eq!(x.described, COLUMNS, "Describe announced the wrong columns");
    assert_eq!(x.rows.len(), 1, "REBASE is one row");
    assert_eq!(x.rows[0].len(), x.described.len(), "described and produced widths disagree");
    assert!(x.rows[0][0].is_some(), "the branch column is NULL");
    assert_eq!(
        x.rows[0][1..].to_vec(),
        texts(&[Some("t"), Some("0"), Some("0"), Some("0"), Some("0"), Some("0"), None]),
        "rebased, both fork seqs, the three counts and a NULL detail"
    );
    assert_eq!(
        pairs(&run(&ctx, &mut a, "SELECT id, qty FROM inventory;")),
        vec![(1, 20), (2, 50)],
        "REBASE over the wire did not move the branch's view"
    );

    // The simple-query path, beside it: the same eight columns, and nothing moved since, so it
    // rebases again.
    let simple = run(&ctx, &mut a, "REBASE;");
    assert_eq!(simple.len(), 1, "REBASE over simple query is one row");
    assert_eq!(simple[0].len(), COLUMNS.len(), "simple query and the extended description disagree");
    assert_eq!(simple[0][1], Value::Boolean(true), "{:?}", simple[0]);
}

/// **A refused REBASE is a row, not an error, over the extended protocol too.** The branch staged a
/// row whose base main then changed; the verdict comes back as `rebased = f` with one moved row and a
/// detail naming the table, under `CommandComplete`, and no `ErrorResponse`. Nothing on the branch
/// changes.
#[test]
fn a_refused_rebase_over_the_extended_protocol_is_a_row_not_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = server(&dir);
    let mut main = seeded(&ctx);
    let mut a = conn(&ctx);
    run(&ctx, &mut a, "BEGIN AGENT SESSION AS 'a' RUN 'r1';");
    run(&ctx, &mut a, "UPDATE inventory SET qty = qty - 5 WHERE id = 1;");
    run(&ctx, &mut main, "UPDATE inventory SET qty = 99 WHERE id = 1;");

    let x = extended(&ctx, &mut a, "REBASE;");
    assert_eq!(
        x.kinds,
        vec![
            "ParseComplete",
            "BindComplete",
            "RowDescription",
            "DataRow",
            "CommandComplete(SELECT 1)",
            "ReadyForQuery",
        ],
        "a refused REBASE must still be a described row, not an error"
    );
    assert_eq!(x.rows.len(), 1);
    assert_eq!(
        x.rows[0][1..7].to_vec(),
        texts(&[Some("f"), Some("0"), Some("0"), Some("1"), Some("0"), Some("0")]),
        "not rebased, seq unchanged, one moved row"
    );
    assert!(
        x.rows[0][7].as_deref().is_some_and(|d| d.contains("inventory")),
        "the detail does not name the table: {:?}",
        x.rows[0][7]
    );
    assert_eq!(
        pairs(&run(&ctx, &mut a, "SELECT id, qty FROM inventory;")),
        vec![(1, 15), (2, 5)],
        "a refused REBASE changed the branch"
    );
}
