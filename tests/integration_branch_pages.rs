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
