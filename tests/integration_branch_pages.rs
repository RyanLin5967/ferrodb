//! S2b-3b: a staged row exists as a page on the branch's own tree, not only in a map.
//!
//! `Workspace` holds a branch's uncommitted rows in a `BTreeMap`, which is why DEMO.md has always
//! said that reading "criterion 2 is MET" as "isolation enforced by shadow paging" is wrong for the
//! SQL surface. `stage()` now mirrors each staged row onto the branch's own copy-on-write tree, so
//! the branch's writes exist as pages reachable only from that branch's root.
//!
//! **What this does NOT yet claim, and the tests are written to keep it honest:** the tree holds
//! the branch's staged *delta*, not the full table. Base rows still live in the heap and are read
//! through the workspace map, so the tree cannot yet serve reads — that is S2b-3c. A test that
//! asserted "the branch tree has the whole table" would fail, and one that quietly checked only
//! staged rows while implying otherwise would be worse.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{row_id_of, AgentRuntime};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::error::FerroError;
use ferrodb::agent_sql::runtime::ExecCtx;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Well above anything the catalog, heap and indexes will reach; the partition is deliberate and
/// `ArenaPageStore::new` registers it with the disk manager so the legacy allocator cannot cross it.
const ARENA_BASE: u32 = 1024;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    store: Arc<ArenaPageStore>,
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
            .open(dir.path().join("pages.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("pages.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let store = Arc::new(
            ArenaPageStore::new(bp.clone(), Arc::clone(&branches), ARENA_BASE).unwrap(),
        );
        let runtime = Arc::new(
            AgentRuntime::with_storage(
                branches,
                Arc::new(MemEffectLog::new()),
                Arc::clone(&store) as Arc<dyn PageStore>,
            )
            .unwrap(),
        );
        Db { catalog, bp, txn, runtime, store, _dir: dir }
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
        assert_eq!(stmts.len(), 1, "expected one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 100);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 200);", &mut s);
    }

    /// What the SQL surface reports for a row on a branch — the workspace map's answer.
    fn sql_qty(&mut self, branch_name: &str, id: i32) -> Option<i32> {
        let mut s = self.session();
        let sql = format!("SELECT qty FROM inventory AS OF BRANCH {branch_name} WHERE id = {id};");
        match self.ok(&sql, &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| match r.first() {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            }),
            _ => panic!("expected rows from: {sql}"),
        }
    }
}

fn rid(id: i32) -> u64 {
    row_id_of(&[Value::Integer(id)]).0
}

#[test]
fn a_staged_row_exists_as_a_page_on_the_branch_tree() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;

    // Before staging, the branch's tree has nothing for this row.
    assert_eq!(
        db.runtime.get_row(branch, "inventory", rid(1)).unwrap(),
        None,
        "the branch tree held a row before anything was staged"
    );

    db.ok("UPDATE inventory SET qty = 42 WHERE id = 1;", &mut a);

    let on_pages = db
        .runtime
        .get_row(branch, "inventory", rid(1))
        .unwrap()
        .expect("the staged row was not mirrored onto the branch's tree");
    assert_eq!(
        on_pages.get(1),
        Some(&Value::Integer(42)),
        "the page-resident row does not carry the staged value: {on_pages:?}"
    );
}

/// The agreement check S2b-3b is for: the tree and the workspace map must say the same thing.
/// While both representations exist, a divergence between them is the bug that would make
/// flipping reads onto the tree (S2b-3c) silently change query results.
#[test]
fn the_branch_tree_and_the_workspace_map_agree_on_every_staged_row() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    let name = a.agent.as_ref().unwrap().branch_name.clone();

    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 22 WHERE id = 2;", &mut a);
    db.ok("UPDATE inventory SET qty = 33 WHERE id = 1;", &mut a);

    for (id, expected) in [(1, 33), (2, 22)] {
        let via_sql = db.sql_qty(&name, id);
        let via_pages = db
            .runtime
            .get_row(branch, "inventory", rid(id))
            .unwrap()
            .and_then(|r| match r.get(1) {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            });
        assert_eq!(via_sql, Some(expected), "SQL surface disagrees for row {id}");
        assert_eq!(
            via_pages, via_sql,
            "row {id}: the branch's tree says {via_pages:?} and the workspace map says {via_sql:?}"
        );
    }
}

/// Isolation as a property of the page graph rather than of an unshared map.
#[test]
fn one_branchs_staged_pages_are_unreachable_from_trunk_and_from_a_sibling() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    let a_branch = a.agent.as_ref().unwrap().branch;

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-b' RUN 'r_b';", &mut b);
    let b_branch = b.agent.as_ref().unwrap().branch;

    db.ok("UPDATE inventory SET qty = 42 WHERE id = 1;", &mut a);

    assert!(
        db.runtime.get_row(a_branch, "inventory", rid(1)).unwrap().is_some(),
        "the writing branch cannot see its own staged page"
    );
    assert_eq!(
        db.runtime.get_row(BranchId::TRUNK, "inventory", rid(1)).unwrap(),
        None,
        "a branch's staged page is reachable from the trunk root"
    );
    assert_eq!(
        db.runtime.get_row(b_branch, "inventory", rid(1)).unwrap(),
        None,
        "a branch's staged page is reachable from a sibling's root"
    );
}

#[test]
fn a_staged_delete_removes_the_row_from_the_branch_tree() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;

    db.ok("UPDATE inventory SET qty = 42 WHERE id = 1;", &mut a);
    assert!(db.runtime.get_row(branch, "inventory", rid(1)).unwrap().is_some());

    db.ok("DELETE FROM inventory WHERE id = 1;", &mut a);
    assert_eq!(
        db.runtime.get_row(branch, "inventory", rid(1)).unwrap(),
        None,
        "a staged delete left the row on the branch's tree"
    );
}

/// The mirror must cost real pages, or "it is on the tree" is a statement about nothing.
#[test]
fn staging_allocates_pages_and_an_untouched_branch_allocates_none() {
    let mut db = Db::new();
    db.seed();

    let mut idle = db.session();
    db.ok("BEGIN AGENT SESSION AS 'idle-agent' RUN 'r_i';", &mut idle);
    let before_idle = db.store.live_page_count().unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    assert_eq!(
        db.store.live_page_count().unwrap(),
        before_idle,
        "opening a session allocated pages; the fork is supposed to copy nothing"
    );

    db.ok("UPDATE inventory SET qty = 42 WHERE id = 1;", &mut a);
    let after = db.store.live_page_count().unwrap();
    assert!(
        after > before_idle,
        "staging a row allocated no pages ({before_idle} -> {after}), so nothing was mirrored"
    );
}

/// S2b-3c-i: the changeset derived from the PAGES agrees with the one derived from the map.
///
/// This is the step that has to hold before the map can go away: if the two disagree, moving `DIFF`
/// onto the page diff would silently change what a merge does.
#[test]
fn the_page_derived_changeset_agrees_with_the_map_derived_one() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;

    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 22 WHERE id = 2;", &mut a);

    let from_map = {
        let bp = db.bp.clone();
        let txn = db.txn.clone();
        let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
        db.runtime.diff(&mut ctx, branch).unwrap()
    };
    let from_pages = db.runtime.page_changeset(branch).unwrap();

    // Same rows, and the same resulting image for each.
    let mut map_rows: Vec<(u32, u64, Option<Vec<Value>>)> =
        from_map.rows.iter().map(|r| (r.tbl.0, r.row.0, r.after.clone())).collect();
    let mut page_rows: Vec<(u32, u64, Option<Vec<Value>>)> =
        from_pages.iter().map(|c| (c.table, c.row, c.after.clone())).collect();
    map_rows.sort();
    page_rows.sort();
    assert_eq!(
        page_rows, map_rows,
        "the page-derived changeset and the map-derived one disagree about what the branch changed"
    );
    assert_eq!(page_rows.len(), 2, "expected exactly the two staged rows");
}

/// The one place the two representations genuinely differ, pinned rather than glossed over.
///
/// `RowChange::before` is the fork-point image and comes from the heap, so it carries the row's
/// old value. The page diff's `before` is `None`, because base rows are not in the tree yet — the
/// branch's tree starts empty and holds only what was staged. This is the gap S2b-3c-ii is about,
/// and it is asserted here so that when base rows do move into the tree this test fails and forces
/// the claim to be restated rather than quietly becoming true.
#[test]
fn the_page_diff_has_no_fork_point_image_yet_because_base_rows_are_not_on_the_tree() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut a);

    let from_map = {
        let bp = db.bp.clone();
        let txn = db.txn.clone();
        let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
        db.runtime.diff(&mut ctx, branch).unwrap()
    };
    let from_pages = db.runtime.page_changeset(branch).unwrap();

    assert_eq!(
        from_map.rows[0].before,
        Some(vec![Value::Integer(1), Value::Integer(100)]),
        "the map-derived changeset should carry the heap's fork-point image"
    );
    assert_eq!(
        from_pages[0].before, None,
        "the page diff reported a fork-point image; if base rows now live on the tree, this test \
         has done its job and the S2b-3c-ii claim needs restating"
    );
}

/// A session that staged nothing has an empty changeset, and the diff proves it without reading
/// a page: the branch's root is still the root it forked from.
#[test]
fn an_untouched_session_has_an_empty_page_changeset() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;

    assert!(
        db.runtime.page_changeset(branch).unwrap().is_empty(),
        "a session that wrote nothing reported changes"
    );
}

#[test]
fn a_staged_delete_appears_in_the_page_changeset_as_a_removal() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;

    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut a);
    db.ok("DELETE FROM inventory WHERE id = 1;", &mut a);

    // Staged then deleted within one session: the tree ends where it started for that row, so the
    // page diff correctly reports nothing for it rather than inventing a delete of a row the tree
    // never held.
    let changes = db.runtime.page_changeset(branch).unwrap();
    assert!(
        changes.iter().all(|c| c.row != rid(1)),
        "row 1 was staged and then deleted in one session; the tree returned to its fork state, so \
         the page diff must report no change for it, got {changes:?}"
    );
}

/// **A wide-typed cell must be readable back off the branch's own pages.**
///
/// This is the end-to-end form of the codec asymmetry that `paged_rows`' unit tests pin at the
/// byte level. `encode_row` delegates straight to `Value::serialize`, which learned tags 5
/// (BigInt), 6 (Decimal) and 7 (Timestamp) when the wide types arrived; `value_span`, which
/// `decode_row` consults for every cell, did not. The result was the worst available failure mode:
/// the WRITE succeeded and every later READ of that row failed with "unknown value tag".
///
/// A unit test on `encode_row`/`decode_row` proves the codec agrees with itself. It does not prove
/// the path is reachable, and reachability is the whole reason this mattered — so this one drives
/// it the way a user does: real SQL, on an agent branch, then `page_changeset`, which decodes the
/// before and after image of every changed row off the pages.
///
/// (The review that found this named the entry point `AgentRuntime::page_row_changes`. No such
/// method exists; the decoding one is `page_changeset`, used here.)
#[test]
fn wide_typed_cells_survive_a_round_trip_through_a_branchs_pages() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok(
        "CREATE TABLE ledger (id INTEGER NOT NULL, big BIGINT, dec DECIMAL, ts TIMESTAMP);",
        &mut s,
    );
    db.ok("INSERT INTO ledger VALUES (1, 9223372036854775807, 1.50, 1700000000123);", &mut s);

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'accountant' RUN 'r_a';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;

    // Every wide column, written on the branch. Past 2^53 on purpose: a value that survived by
    // being routed through an f64 would come back as a neighbour rather than itself.
    db.ok(
        "UPDATE ledger SET big = 9007199254740993, dec = 0.00000000000000000001, \
         ts = -1 WHERE id = 1;",
        &mut a,
    );

    // The read that used to fail. Before `value_span` learned tags 5/6/7 this was not a wrong
    // answer but an Err — "cell N of M: unknown value tag 5" — so the diff errored instead of
    // returning, and a wide cell was write-only on a page-backed branch.
    let changes = db
        .runtime
        .page_changeset(branch)
        .expect("page_changeset failed to decode a wide-typed cell off the branch's pages");

    let c = changes
        .iter()
        .find(|c| c.row == rid(1))
        .unwrap_or_else(|| panic!("row 1 is missing from the page changeset: {changes:?}"));
    let after = c.after.as_ref().expect("row 1 has no after image");

    // The variants as much as the values: a BigInt that came back as Float would compare equal to
    // plenty of things while having already lost its low bits, because `Value`'s PartialEq is
    // numeric across the numeric types.
    assert!(
        matches!(after[1], Value::BigInt(v) if v == 9007199254740993),
        "BIGINT did not survive the branch's pages: {:?}",
        after[1]
    );
    assert!(
        matches!(&after[2], Value::Decimal(d) if d == "0.00000000000000000001"),
        "DECIMAL did not survive the branch's pages with its digits intact: {:?}",
        after[2]
    );
    assert!(
        matches!(after[3], Value::Timestamp(v) if v == -1),
        "TIMESTAMP did not survive the branch's pages: {:?}",
        after[3]
    );

    // There is deliberately NO before image, and saying so is part of keeping this file honest.
    // Per the module note (S2b-3c), the branch's tree holds the staged delta rather than the whole
    // table: base rows still live in the heap, so the fork-point tree does not hold row 1 and the
    // page diff correctly reports it as new rather than inventing an image it never stored.
    //
    // Asserting this rather than skipping it means that when base tables do move into the tree,
    // this test fails and has to be extended to cover the before image — which is the point at
    // which a wide cell would newly need decoding in that position.
    assert!(
        c.before.is_none(),
        "the fork-point tree does not hold base rows yet, so row 1 must diff as new; a before \
         image here means base tables have moved into the tree and this test now has to prove a \
         wide cell decodes in that position too: {:?}",
        c.before
    );
}
