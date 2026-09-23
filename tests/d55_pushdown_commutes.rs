//! D55 — pushing a predicate into the base scan must return EXACTLY what filtering afterwards did.
//!
//! `AgentRuntime::visible_rows_where` computes `filter(pred, base) ⊕ filter(pred, staged)` where
//! the unfiltered path computed `filter(pred, base ⊕ staged)`. The design entry argues they are
//! equal because the overlay is per-KEY and the predicate is per-ROW. An argument is not a test,
//! so this constructs every case the argument enumerates and asserts the two paths agree:
//!
//! * base passes, staged version FAILS   → row must be ABSENT (staged wins, and it fails)
//! * base fails,  staged version PASSES  → row must be PRESENT
//! * staged-only INSERT that passes      → PRESENT
//! * staged-only INSERT that fails       → ABSENT
//! * staged DELETE of a passing base row → ABSENT
//! * base passes, untouched              → PRESENT
//!
//! The comparison is against the SAME session's unfiltered `SELECT` with the predicate applied in
//! the test, so a regression in either path shows as disagreement rather than as a hard-coded
//! expected set that could itself be wrong.
//!
//! # Two storage backends, because one of them is not the other
//!
//! Both tests below run the SAME six cases. They differ in exactly one thing: which runtime the
//! session holds.
//!
//! * `AgentRuntime::new()` — `storage: None`. The branch's staged rows live only in the
//!   workspace `BTreeMap`.
//! * `AgentRuntime::with_storage(.., ArenaPageStore, ..)` — `storage: Some(PagedRows)`. Every
//!   staged row is ALSO mirrored onto the branch's own copy-on-write page tree by
//!   `stage_all` (`src/agent_sql/runtime.rs`, the `if self.storage.is_some()` block), which
//!   allocates arena pages, publishes a new branch root through the catalog, and takes the
//!   catalog lock while doing it. None of that code runs on the map-backed path, and a layer
//!   exercised on one backend only is untested on the other.
//!
//! The arena test is deliberately NOT a refactor of the map-backed one into a shared helper: the
//! assertions are spelled out twice so that a change made to satisfy one backend cannot silently
//! move the other backend's expectation with it.
//!
//! # ⚠ What the arena test does and does NOT prove — measured, not argued
//!
//! **`visible_rows_where` never reads the page store.** It filters the base scan and then folds in
//! `state.workspaces[branch].rows`, which is the same in-memory map on both backends; `self.storage`
//! appears nowhere on that path (`src/agent_sql/runtime.rs` — the only uses are `storage()`,
//! `rows()`, `live_page_count` and `stage_all`'s mirror block). So the six case assertions
//! **cannot** diverge between the two backends, and a green arena run is not independent
//! confirmation that pushdown commutes "on disk" — the overlay it reads is not on disk.
//!
//! That was fire-checked rather than reasoned at: with `stage_all`'s mirror block disabled
//! (`if false && self.storage.is_some()`), so that nothing whatsoever reached the arena, all six
//! arena case assertions still PASSED. The one assertion that fired was the page-growth guard.
//!
//! What the arena test therefore does buy, which the map-backed test cannot:
//!
//! 1. Every staged write really executes `put_row` / `delete_row`, arena page allocation and a
//!    `set_root` through the branch catalog — under the catalog lock, beside a live agent session.
//!    A panic, an error or a deadlock in any of that fails here and nowhere else in this file.
//! 2. The page-growth guard pins that the mirror ran at all, so the test cannot silently decay
//!    into a second copy of the map-backed one.
//! 3. It is the regression barrier for the change that would make backend divergence possible:
//!    the day `visible_rows_where` starts answering from the tree, these six cases are already
//!    written and already pointed at it.
//!
//! ⚠ **No `ServerContext` is built in this binary, and that is load-bearing.** `ServerContext::new`
//! designates its runtime process-wide (`src/agent_sql/designated.rs`), after which any agent
//! statement on a *different* runtime is refused — which is every statement of the map-backed test
//! above. `cargo test` runs a binary's tests on parallel threads, so a `ServerContext` here fails
//! the sibling test with a designation error that has nothing to do with pushdown. The
//! `ServerContext` variant therefore gets a process to itself, in
//! `tests/d55_pushdown_commutes_server.rs`, for the same reason `tests/d101_designated_runtime.rs`
//! does.

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
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
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Same floor every other arena test uses, and above the pages `Catalog::create` takes.
const ARENA_BASE: u32 = 1024;

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
            .open(dir.path().join("p.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    /// The same database, with the DISK path underneath: a storage-backed runtime over a real
    /// `ArenaPageStore`. Everything above the runtime — catalog, buffer pool, WAL, txn manager,
    /// and every method below — is byte-for-byte what `new()` builds, so the only thing the two
    /// tests differ in is `storage: None` versus `storage: Some(PagedRows)`.
    ///
    /// The wiring is the one `tests/integration_branch_pages.rs`, `tests/adv_f5_probe.rs` and
    /// `examples/d90_delta_vs_chunk.rs` use, not a new one.
    fn arena() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("p.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let store = Arc::new(
            ArenaPageStore::new(
                bp.clone(),
                Arc::clone(&branches) as Arc<dyn BranchCatalog>,
                ARENA_BASE,
            )
            .unwrap(),
        );
        let runtime = Arc::new(
            AgentRuntime::with_storage(
                Arc::clone(&branches) as Arc<dyn BranchCatalog>,
                Arc::new(MemEffectLog::new()),
                Arc::clone(&store) as Arc<dyn PageStore>,
            )
            .expect("attach arena storage"),
        );
        assert!(
            runtime.storage().is_some(),
            "this harness is only worth running if the runtime really is page-backed; a \
             `storage: None` runtime here would re-run the map-backed test under a disk-path name"
        );
        Db { catalog, bp, txn, runtime, _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(format!("{:?}", parser.errors)));
        }
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    /// `(id, v)` pairs a SELECT returned, as a set.
    fn pairs(&mut self, sql: &str, s: &mut Session) -> BTreeSet<(i32, i32)> {
        match self.ok(sql, s) {
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
}

#[test]
fn pushdown_agrees_with_filter_afterwards_on_every_overlay_case() {
    let mut db = Db::new();
    let mut setup = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut setup);
    for i in 1..=20 {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {});", i * 10), &mut setup);
    }

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r1';", &mut a);
    // base passes (50 < 100), staged FAILS -> must be absent
    db.ok("UPDATE t SET v = 999 WHERE id = 5;", &mut a);
    // base fails (150), staged PASSES (50) -> must be present
    db.ok("UPDATE t SET v = 50 WHERE id = 15;", &mut a);
    // staged DELETE of a passing base row -> absent
    db.ok("DELETE FROM t WHERE id = 3;", &mut a);
    // staged-only INSERTs: one passes, one fails
    db.ok("INSERT INTO t VALUES (25, 25);", &mut a);
    db.ok("INSERT INTO t VALUES (26, 2600);", &mut a);

    // The reference: the SAME session's unfiltered view, filtered here in the test.
    let all = db.pairs("SELECT id, v FROM t;", &mut a);
    let reference: BTreeSet<(i32, i32)> = all.iter().copied().filter(|(_, v)| *v < 100).collect();

    // The path under test: the predicate pushed down.
    let pushed = db.pairs("SELECT id, v FROM t WHERE v < 100;", &mut a);

    assert_eq!(
        pushed, reference,
        "pushdown disagrees with filter-afterwards\n  pushed:    {pushed:?}\n  reference: {reference:?}"
    );

    // And the enumerated cases, spelled out so a failure names WHICH case broke rather than only
    // that the sets differ.
    let ids: BTreeSet<i32> = pushed.iter().map(|(id, _)| *id).collect();
    assert!(!ids.contains(&5), "case: base passes, staged fails -> must be ABSENT (staged wins)");
    assert!(ids.contains(&15), "case: base fails, staged passes -> must be PRESENT");
    assert!(!ids.contains(&3), "case: staged DELETE of a passing row -> must be ABSENT");
    assert!(ids.contains(&25), "case: staged-only INSERT that passes -> must be PRESENT");
    assert!(!ids.contains(&26), "case: staged-only INSERT that fails -> must be ABSENT");
    assert!(ids.contains(&1), "case: untouched passing base row -> must be PRESENT");
    assert!(!ids.contains(&12), "case: untouched failing base row (120) -> must be ABSENT");

    // A point predicate on the key too, which is the shape the planner turns into an index probe --
    // and, since D57, the shape the overlay answers by a single PROBE rather than the walk the
    // predicate above exercises. These two lines pin the probe path; `v < 100` pins the walk.
    let point = db.pairs("SELECT id, v FROM t WHERE id = 15;", &mut a);
    assert_eq!(point, BTreeSet::from([(15, 50)]), "point lookup must see the STAGED version");
    let gone = db.pairs("SELECT id, v FROM t WHERE id = 3;", &mut a);
    assert!(gone.is_empty(), "point lookup of a staged DELETE must find nothing");
}

/// The SAME six cases on the DISK path: a storage-backed runtime over a real `ArenaPageStore`.
///
/// Every staged write below additionally runs `stage_all`'s mirror block — `put_row` /
/// `delete_row` onto the branch's copy-on-write tree, arena page allocation, and a `set_root`
/// through the branch catalog. The map-backed test above runs none of that. If pushdown
/// commutes on one backend and not the other, this is where it shows.
#[test]
fn pushdown_agrees_with_filter_afterwards_on_the_arena_path() {
    let mut db = Db::arena();
    let mut setup = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut setup);
    for i in 1..=20 {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {});", i * 10), &mut setup);
    }

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r1';", &mut a);

    // The arena BEFORE any staged write. Taken here, not compared against a constant: the trunk
    // root `with_storage` creates and the fork the BEGIN above took are already pages, so
    // `count > 0` would hold with the whole mirror block deleted. Growth against this baseline
    // is what cannot.
    let pages_before = arena_pages(&db);

    // base passes (50 < 100), staged FAILS -> must be absent
    db.ok("UPDATE t SET v = 999 WHERE id = 5;", &mut a);
    // base fails (150), staged PASSES (50) -> must be present
    db.ok("UPDATE t SET v = 50 WHERE id = 15;", &mut a);
    // staged DELETE of a passing base row -> absent
    db.ok("DELETE FROM t WHERE id = 3;", &mut a);
    // staged-only INSERTs: one passes, one fails
    db.ok("INSERT INTO t VALUES (25, 25);", &mut a);
    db.ok("INSERT INTO t VALUES (26, 2600);", &mut a);

    // The reason this file needed a second backend at all: the five writes above really did reach
    // the page store, so the six cases below are being answered over a branch that exists as
    // PAGES. Asserted before the cases, so a harness that quietly lost its storage reports that
    // rather than reporting a pushdown result it did not measure.
    let pages_after = arena_pages(&db);
    assert!(
        pages_after > pages_before,
        "the arena held {pages_before} pages before the five staged writes and {pages_after} \
         after, so nothing was mirrored onto the branch's tree and this test re-ran the \
         map-backed path under a disk-path name"
    );

    // The reference: the SAME session's unfiltered view, filtered here in the test.
    let all = db.pairs("SELECT id, v FROM t;", &mut a);
    let reference: BTreeSet<(i32, i32)> = all.iter().copied().filter(|(_, v)| *v < 100).collect();

    // The path under test: the predicate pushed down.
    let pushed = db.pairs("SELECT id, v FROM t WHERE v < 100;", &mut a);

    assert_eq!(
        pushed, reference,
        "arena path: pushdown disagrees with filter-afterwards\n  pushed:    {pushed:?}\n  reference: {reference:?}"
    );

    // And the enumerated cases, spelled out so a failure names WHICH case broke rather than only
    // that the sets differ.
    let ids: BTreeSet<i32> = pushed.iter().map(|(id, _)| *id).collect();
    assert!(!ids.contains(&5), "arena: base passes, staged fails -> must be ABSENT (staged wins)");
    assert!(ids.contains(&15), "arena: base fails, staged passes -> must be PRESENT");
    assert!(!ids.contains(&3), "arena: staged DELETE of a passing row -> must be ABSENT");
    assert!(ids.contains(&25), "arena: staged-only INSERT that passes -> must be PRESENT");
    assert!(!ids.contains(&26), "arena: staged-only INSERT that fails -> must be ABSENT");
    assert!(ids.contains(&1), "arena: untouched passing base row -> must be PRESENT");
    assert!(!ids.contains(&12), "arena: untouched failing base row (120) -> must be ABSENT");

    // The probe path (D57), same as the map-backed test: a `pk = literal` predicate answers from
    // a single overlay probe rather than the walk `v < 100` exercises.
    let point = db.pairs("SELECT id, v FROM t WHERE id = 15;", &mut a);
    assert_eq!(point, BTreeSet::from([(15, 50)]), "arena: point lookup must see the STAGED version");
    let gone = db.pairs("SELECT id, v FROM t WHERE id = 3;", &mut a);
    assert!(gone.is_empty(), "arena: point lookup of a staged DELETE must find nothing");
}

/// Pages the arena currently holds. `None` — a runtime with no page store — is a failure here,
/// not a zero: `live_page_count` returns `Option` precisely so that "the store does not exist"
/// cannot be read as "the store is empty".
fn arena_pages(db: &Db) -> u32 {
    db.runtime
        .live_page_count()
        .expect("page count")
        .expect("a storage-backed runtime reports Some(_), never None")
}
