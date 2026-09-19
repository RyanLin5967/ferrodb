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

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
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
            .open(dir.path().join("p.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
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

    // A point predicate on the key too, which is the shape the planner turns into an index probe.
    let point = db.pairs("SELECT id, v FROM t WHERE id = 15;", &mut a);
    assert_eq!(point, BTreeSet::from([(15, 50)]), "point lookup must see the STAGED version");
    let gone = db.pairs("SELECT id, v FROM t WHERE id = 3;", &mut a);
    assert!(gone.is_empty(), "point lookup of a staged DELETE must find nothing");
}
