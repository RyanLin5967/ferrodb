//! D194 × D197: a pinned branch must read the fork-time row through the primary index, even after
//! main deletes the key and reuses it.
//!
//! Pre-registered in `bench/d194_fork_snapshot/rebase_prereg.md`, Amendment 10, decision 6.
//!
//! D194 made every branch read main AS OF ITS FORK, which is a temporal read of main. The engine's
//! reused-key path assumed none existed: a trunk INSERT of a key whose row was deleted writes a new
//! tuple with no `prev` chain and repoints the primary index at it. A reader older than the reuse
//! then reaches the new tuple through the index, finds it invisible, and has no chain to walk back
//! along to the version it should see. So a point read loses the row, while a scan of the same
//! branch, which visits the dead tuple directly, still shows it (review C1, `d194_cost_review.md`).
//!
//! That gap is D197's. The chain fix (`4296723`, `52b66d6`) is in #16's lineage, so D194 lands after
//! #16. **On this branch alone, step (c) FAILS, returning no row. After merging #16 it passes.**
//! Steps (a) and (b) pass on both, and they are what make (c) mean something: (a) shows the pin
//! itself is right, and (b) shows the read took the index path, the one the gap is on.
//!
//! One test in this binary, so the process-wide index-scan counter is read by nothing else while
//! the premise is being measured.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::index_scan::index_scan_counters;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

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
            .open(dir.path().join("key_reuse.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("key_reuse.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "{sql}: {:?}", parser.errors);
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
            .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn pairs(&mut self, sql: &str, s: &mut Session) -> Vec<(i32, i32)> {
        let rows = match self.ok(sql, s) {
            Outcome::Rows(r) => r,
            _ => panic!("{sql}: expected rows"),
        };
        let mut v: Vec<(i32, i32)> = rows
            .iter()
            .map(|row| match (&row[0], &row[1]) {
                (Value::Integer(id), Value::Integer(x)) => (*id, *x),
                other => panic!("not an (INTEGER, INTEGER) row: {other:?}"),
            })
            .collect();
        v.sort();
        v
    }
}

#[test]
fn a_pinned_index_read_after_main_deletes_and_reuses_the_key_returns_the_fork_image() {
    let mut db = Db::new();
    let mut main = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut main);
    // Enough rows that a point read of the key is an index read, not a scan; (b) checks it.
    for id in 1..=200 {
        db.ok(&format!("INSERT INTO t VALUES ({id}, {});", id * 10), &mut main);
    }

    let mut agent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut agent);

    // After the pin: main deletes key 1 and reuses it.
    db.ok("DELETE FROM t WHERE id = 1;", &mut main);
    db.ok("INSERT INTO t VALUES (1, 99);", &mut main);
    assert_eq!(
        db.pairs("SELECT id, v FROM t WHERE id = 1;", &mut main),
        vec![(1, 99)],
        "fixture: main does not see its own reuse"
    );

    // (a) The pin is right: a scan of the branch sees the fork-time row and not the reuse.
    let scan = db.pairs("SELECT id, v FROM t;", &mut agent);
    assert!(scan.contains(&(1, 10)), "the branch's scan lost the fork-time row: {:?}", &scan[..3]);
    assert!(!scan.contains(&(1, 99)), "the branch's scan shows main's reuse after its fork");

    // (b) Premise: the point read takes the primary index, which is the path the gap is on.
    let (index_scans_before, _) = index_scan_counters();
    let point = db.pairs("SELECT id, v FROM t WHERE id = 1;", &mut agent);
    let (index_scans_after, _) = index_scan_counters();
    assert!(
        index_scans_after > index_scans_before,
        "fixture: the branch's point read did not take the index path, so this test would not \
         reach the reused-key gap"
    );

    // (c) The point read returns what the scan returned. RED on D194 alone (D197's gap); GREEN
    //     once #16's reused-key chain is merged.
    assert_eq!(
        point,
        vec![(1, 10)],
        "a pinned index read after main deleted and reused the key lost the fork-time row"
    );
}
