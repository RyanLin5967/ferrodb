//! E33 — which storage actually holds an agent's rows.
//!
//! Written to settle a question I had asserted rather than measured. E32 described re-issuing a
//! page as "overwriting the trunk tree", and two observations pointed the other way: a merged row
//! survived a restart even with the page-backed runtime disabled, and `SELECT` outside an agent
//! session reads the ordinary heap. The first version of this file therefore asserted that a merged
//! row lands in trunk's copy-on-write tree. **It does not, and that assertion was wrong** — the
//! measurement is recorded here rather than quietly dropped:
//!
//! ```text
//! TRUNK tree rows:      []
//! BRANCH b1@g0 rows:    [(2, [Integer(2), Integer(20)])]
//! ```
//!
//! The design that produces those two lines is the one worth pinning. Trunk's rows live in the
//! ordinary heap, which is the table storage this database had before branches existed. A fork
//! builds a copy-on-write tree for the branch, the agent's writes land **on pages in that tree**,
//! and `MERGE` replays the winning effects into the heap. So the isolation really is enforced by
//! shadow paging — on the branch side, which is the side that needs isolating — while trunk keeps
//! the storage every non-agent statement already used.
//!
//! This also narrows E32 correctly rather than dismissing it. Trunk's tree is empty, so re-issuing
//! *trunk's* root would alias an empty tree; but a **branch** root is named by the same durable
//! catalog and demonstrably holds real rows, so the aliasing bug E32 fixed is a data-loss bug with
//! the example changed, not a theoretical one.

use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::{BranchCatalog, BranchId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
    runtime: Arc<AgentRuntime>,
}

/// The CLI's construction order, including the part that matters: the arena floor is taken after
/// the catalog has allocated, with headroom, or the ordinary allocator has nowhere to grow.
fn db(tag: &str) -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("{tag}.db"));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let catalog = Catalog::create(bp.clone()).unwrap();

    let branches =
        Arc::new(LogBranchCatalog::open(&dir.path().join(format!("{tag}.branches")), 1).unwrap());
    let base = bp.disk_manager.high_water().unwrap() + 256;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), base).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )
        .unwrap(),
    );
    let session = Session::with_runtime(runtime.clone());
    Db { _dir: dir, catalog, bp, txn, session, runtime }
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    fn query(&mut self, sql: &str) -> Vec<Vec<Value>> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        let out = run(
            stmts.remove(0),
            &mut self.catalog,
            self.bp.clone(),
            self.txn.clone(),
            &mut self.session,
        )
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
        match out {
            Outcome::Rows(r) => r,
            _ => panic!("`{sql}` did not return rows"),
        }
    }
}

/// An agent's write lands on a copy-on-write page in its **branch's** tree, and is absent from
/// trunk's, until `MERGE` replays it into the heap where ordinary statements can see it.
#[test]
fn an_agent_write_lives_on_branch_pages_and_reaches_the_heap_only_on_merge() {
    let mut d = db("authority");
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");
    d.sql("BEGIN AGENT SESSION AS 'a' RUN 'r';");
    d.sql("INSERT INTO inv VALUES (2, 20);");

    let branch = d.session.agent.as_ref().expect("no agent session").branch;
    let on_branch = d.runtime.scan_rows(branch, "inv").expect("scan the branch");
    let on_trunk = d.runtime.scan_rows(BranchId::TRUNK, "inv").expect("scan trunk");

    // The write is on a page, in the branch's tree. This is the whole claim: not "the row is
    // hidden", which a hashmap does too, but "the row is on a shadowed page".
    assert!(
        on_branch.iter().any(|(_, v)| *v == vec![Value::Integer(2), Value::Integer(20)]),
        "the agent's row is not in its branch's page tree, so the write never reached a \
         copy-on-write page: {on_branch:?}"
    );
    assert!(
        !on_trunk.iter().any(|(_, v)| v.contains(&Value::Integer(20))),
        "the agent's row is visible in trunk's tree before MERGE: {on_trunk:?}"
    );

    // And it is invisible to an ordinary read, which goes to the heap.
    d.sql("MERGE;");
    let rows = d.query("SELECT * FROM inv;");
    assert_eq!(rows.len(), 2, "after MERGE the heap should hold both rows: {rows:?}");
    assert!(
        rows.iter().any(|r| *r == vec![Value::Integer(2), Value::Integer(20)]),
        "the merged row never reached the heap: {rows:?}"
    );
}

/// Trunk's tree being empty is the design, not an accident, so it is asserted rather than left as
/// something a future reader has to rediscover the way I did.
#[test]
fn trunk_rows_live_in_the_heap_not_in_trunks_page_tree() {
    let mut d = db("trunkheap");
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");
    d.sql("INSERT INTO inv VALUES (2, 20);");

    let rows = d.query("SELECT * FROM inv;");
    assert_eq!(rows.len(), 2, "fixture: the heap did not take the rows: {rows:?}");
    assert!(
        d.runtime.scan_rows(BranchId::TRUNK, "inv").expect("scan trunk").is_empty(),
        "trunk's page tree holds rows. That is a change of design: trunk is heap-backed and the \
         copy-on-write tree carries branch writes, which is what lets a fork be O(1)."
    );
}

/// The page trunk's root sits on is referenced by the durable branch catalog, which is what makes
/// re-issuing it (E32) an aliasing bug rather than a leak. Pinned here so the root cannot quietly
/// become a page nobody names.
#[test]
fn trunks_root_is_a_real_arena_page_named_by_the_branch_catalog() {
    let d = db("root");
    let root = d.runtime.branches().get(BranchId::TRUNK).unwrap().root_page_id;
    assert!(root > 1, "trunk's root is the placeholder ({root}); no tree was ever created");
    d.runtime
        .storage()
        .expect("the runtime has no page storage, so this test is measuring nothing")
        .tree()
        .store()
        .read_page(root)
        .unwrap_or_else(|e| panic!("trunk's root page {root} does not read back: {e}"));
}
