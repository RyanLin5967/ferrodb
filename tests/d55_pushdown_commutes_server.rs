//! D55 on the **server** path: the same six pushdown-commutation cases, run through a real
//! `ServerContext` over a storage-backed `AgentRuntime` and an `ArenaPageStore`.
//!
//! `tests/d55_pushdown_commutes.rs` covers the two runtimes a test can build directly —
//! `AgentRuntime::new()` (map-backed) and `AgentRuntime::with_storage(..)` over an arena. This
//! file covers the third thing, which is not a runtime but a wiring: the sessions come from
//! `ServerContext::session()` and the catalog is borrowed through `ServerContext::catalog()` for
//! the duration of each statement, which is what `src/pgwire/mod.rs` does for every connection and
//! what `src/cli/cli.rs` does for the CLI. Neither of the other two tests takes that path.
//!
//! ⚠ **Its own test binary on purpose, for the reason `tests/d101_designated_runtime.rs` states.**
//! `ServerContext::new` registers its runtime in a PROCESS-WIDE designation registry
//! (`src/agent_sql/designated.rs`), after which `run_agent_stmt` refuses any agent statement whose
//! session holds a different runtime. `cargo test` runs a binary's tests on parallel threads, so
//! building a `ServerContext` inside `d55_pushdown_commutes.rs` refuses that file's map-backed
//! test with a designation error that has nothing to do with pushdown. Measured, not assumed:
//! placing this test in that file turned
//! `pushdown_agrees_with_filter_afterwards_on_every_overlay_case` red with
//! "this agent statement is running on an AgentRuntime that no ServerContext designated".
//!
//! The six cases, unchanged from the sibling file:
//!
//! * base passes, staged version FAILS   → row must be ABSENT (staged wins, and it fails)
//! * base fails,  staged version PASSES  → row must be PRESENT
//! * staged-only INSERT that passes      → PRESENT
//! * staged-only INSERT that fails       → ABSENT
//! * staged DELETE of a passing base row → ABSENT
//! * base passes, untouched              → PRESENT

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Same floor every other arena test uses, and above the pages `Catalog::create` takes.
const ARENA_BASE: u32 = 1024;

struct Server {
    ctx: Arc<ServerContext>,
    _dir: tempfile::TempDir,
}

/// The wiring `tests/d101_designated_runtime.rs`, `examples/d90_delta_vs_chunk.rs` and
/// `src/cli/cli.rs` use: a durable branch catalog sidecar, a storage-backed runtime over a real
/// arena, handed to a `ServerContext`. Reused rather than reinvented.
fn build() -> Server {
    let owned = tempfile::tempdir().unwrap();
    let dir = owned.path();
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
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            store as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    assert!(
        runtime.storage().is_some(),
        "a `storage: None` runtime here would run the map-backed path under a server name"
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    Server { ctx, _dir: owned }
}

/// One statement, borrowing the catalog through the context exactly as a connection thread does.
fn exec(s: &Server, sql: &str, sess: &mut Session) -> Result<Outcome, FerroError> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() {
        return Err(FerroError::SqlParseError(format!("{:?}", parser.errors)));
    }
    assert_eq!(stmts.len(), 1, "one statement: {sql}");
    let mut cat = s.ctx.catalog();
    let out = run(stmts.remove(0), &mut cat, s.ctx.bp.clone(), s.ctx.txn.clone(), sess);
    drop(cat);
    out
}

fn ok(s: &Server, sql: &str, sess: &mut Session) -> Outcome {
    exec(s, sql, sess).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
}

/// `(id, v)` pairs a SELECT returned, as a set.
fn pairs(s: &Server, sql: &str, sess: &mut Session) -> BTreeSet<(i32, i32)> {
    match ok(s, sql, sess) {
        Outcome::Rows(rows) => rows
            .into_iter()
            .map(|r| match (&r[0], &r[1]) {
                (Value::Integer(id), Value::Integer(v)) => (*id, *v),
                other => panic!("{sql}: unexpected row shape {other:?}"),
            })
            .collect(),
        _ => panic!("{sql}: expected rows"),
    }
}

/// Pages the arena currently holds. `None` — a runtime with no page store — is a failure here,
/// not a zero: `live_page_count` returns `Option` precisely so that "the store does not exist"
/// cannot be read as "the store is empty".
fn arena_pages(s: &Server) -> u32 {
    s.ctx
        .runtime
        .live_page_count()
        .expect("page count")
        .expect("a storage-backed runtime reports Some(_), never None")
}

#[test]
fn pushdown_agrees_with_filter_afterwards_through_a_server_context() {
    let s = build();

    // `ServerContext::session()`, which is what every connection gets. `Session::new()` here would
    // be refused by the designation guard, which is D101's whole subject.
    let mut setup = s.ctx.session();
    ok(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut setup);
    for i in 1..=20 {
        ok(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 10), &mut setup);
    }

    // A SECOND session on the same context, which is the shape that matters: the server shares one
    // runtime across connections, so the agent session below is not the one that seeded the table.
    let mut a = s.ctx.session();
    ok(&s, "BEGIN AGENT SESSION AS 'agent-a' RUN 'r1';", &mut a);

    // Baseline before any staged write; growth against it, never `> 0`, because the trunk root and
    // the fork are already pages.
    let pages_before = arena_pages(&s);

    // base passes (50 < 100), staged FAILS -> must be absent
    ok(&s, "UPDATE t SET v = 999 WHERE id = 5;", &mut a);
    // base fails (150), staged PASSES (50) -> must be present
    ok(&s, "UPDATE t SET v = 50 WHERE id = 15;", &mut a);
    // staged DELETE of a passing base row -> absent
    ok(&s, "DELETE FROM t WHERE id = 3;", &mut a);
    // staged-only INSERTs: one passes, one fails
    ok(&s, "INSERT INTO t VALUES (25, 25);", &mut a);
    ok(&s, "INSERT INTO t VALUES (26, 2600);", &mut a);

    let pages_after = arena_pages(&s);
    assert!(
        pages_after > pages_before,
        "the arena held {pages_before} pages before the five staged writes and {pages_after} \
         after, so nothing was mirrored onto the branch's tree and this test measured a map"
    );

    // The reference: the SAME session's unfiltered view, filtered here in the test.
    let all = pairs(&s, "SELECT id, v FROM t;", &mut a);
    let reference: BTreeSet<(i32, i32)> = all.iter().copied().filter(|(_, v)| *v < 100).collect();

    // The path under test: the predicate pushed down.
    let pushed = pairs(&s, "SELECT id, v FROM t WHERE v < 100;", &mut a);

    assert_eq!(
        pushed, reference,
        "server path: pushdown disagrees with filter-afterwards\n  pushed:    {pushed:?}\n  reference: {reference:?}"
    );

    // And the enumerated cases, spelled out so a failure names WHICH case broke rather than only
    // that the sets differ.
    let ids: BTreeSet<i32> = pushed.iter().map(|(id, _)| *id).collect();
    assert!(!ids.contains(&5), "server: base passes, staged fails -> must be ABSENT (staged wins)");
    assert!(ids.contains(&15), "server: base fails, staged passes -> must be PRESENT");
    assert!(!ids.contains(&3), "server: staged DELETE of a passing row -> must be ABSENT");
    assert!(ids.contains(&25), "server: staged-only INSERT that passes -> must be PRESENT");
    assert!(!ids.contains(&26), "server: staged-only INSERT that fails -> must be ABSENT");
    assert!(ids.contains(&1), "server: untouched passing base row -> must be PRESENT");
    assert!(!ids.contains(&12), "server: untouched failing base row (120) -> must be ABSENT");

    // The probe path (D57): a `pk = literal` predicate answers from a single overlay probe rather
    // than the walk `v < 100` exercises.
    let point = pairs(&s, "SELECT id, v FROM t WHERE id = 15;", &mut a);
    assert_eq!(point, BTreeSet::from([(15, 50)]), "server: point lookup must see the STAGED version");
    let gone = pairs(&s, "SELECT id, v FROM t WHERE id = 3;", &mut a);
    assert!(gone.is_empty(), "server: point lookup of a staged DELETE must find nothing");
}
