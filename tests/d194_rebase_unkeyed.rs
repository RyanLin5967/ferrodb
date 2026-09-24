//! D194 step 4 — `REBASE` over a staged row whose KEY no image names: `rebase_key`'s no-key path.
//!
//! A branch that INSERTs a row and then DELETEs it stages `(base None, Deleted)`, and the only image
//! that ever named the row's key is the `RowCreate` in THAT branch's frame. A child forked from it
//! inherits the staged entry with an empty frame, so for the child no image names the key, and the
//! row id is a one-way hash. REBASE still has to know whether main now holds a row under that id:
//! if it does, the inherited `Deleted` would hide main's row from the child's rebased view.
//!
//! Expected results: `bench/d194_fork_snapshot/rebase_prereg.md`, Amendment 2 B.

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
            .open(dir.path().join("unkeyed.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("unkeyed.wal")).unwrap());
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
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    /// `(1, 20)`, `(2, 5)` — the same fixture `agent_sql_surface` uses.
    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 5);", &mut s);
    }

    /// The whole table as `s` sees it, sorted. A full scan on purpose: it retains a predicate
    /// summary, not an exact premise, so looking at a branch here never becomes one of the
    /// premises a later `REBASE` has to validate.
    fn view(&mut self, s: &mut Session) -> Vec<(i32, i32)> {
        let rows = match self.ok("SELECT id, qty FROM inventory;", s) {
            Outcome::Rows(r) => r,
            _ => panic!("expected rows"),
        };
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

    fn rebase(&mut self, s: &mut Session) -> Rebase {
        rebase_of(self.ok("REBASE;", s))
    }
}

/// One `REBASE` result, read by column name.
#[derive(Debug)]
struct Rebase {
    rebased: bool,
    before: i64,
    after: i64,
    moved_rows: i32,
    moved_premises: i32,
    detail: Option<String>,
}

fn rebase_of(out: Outcome) -> Rebase {
    let t = match out {
        Outcome::Agent(a) => a.to_rows(),
        _ => panic!("REBASE returned something other than an agent output"),
    };
    assert_eq!(t.rows.len(), 1, "REBASE reports exactly one row: {:?}", t.header());
    let col = |name: &str| -> Value {
        let at = t
            .column_index(name)
            .unwrap_or_else(|| panic!("REBASE declares no `{name}` column: {:?}", t.header()));
        t.rows[0][at].clone()
    };
    let big = |name: &str| match col(name) {
        Value::BigInt(n) => n,
        other => panic!("`{name}` is {other:?}, not a BIGINT"),
    };
    let int = |name: &str| match col(name) {
        Value::Integer(n) => n,
        other => panic!("`{name}` is {other:?}, not an INTEGER"),
    };
    Rebase {
        rebased: match col("rebased") {
            Value::Boolean(b) => b,
            other => panic!("`rebased` is {other:?}, not a BOOLEAN"),
        },
        before: big("fork_seq_before"),
        after: big("fork_seq_after"),
        moved_rows: int("moved_rows"),
        moved_premises: int("moved_premises"),
        detail: match col("detail") {
            Value::Varchar(s) => Some(s),
            Value::Null => None,
            other => panic!("`detail` is {other:?}"),
        },
    }
}


/// A parent that inserted `(7, 70)` and deleted it again, and a child forked from it afterwards.
fn parent_and_child_after_an_insert_then_delete(db: &mut Db) -> (Session, Session) {
    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut parent);
    db.ok("INSERT INTO inventory VALUES (7, 70);", &mut parent);
    db.ok("DELETE FROM inventory WHERE id = 7;", &mut parent);
    assert_eq!(db.view(&mut parent), vec![(1, 20), (2, 5)], "fixture: the parent still sees row 7");

    let parent_branch = parent.agent.as_ref().unwrap().branch;
    let child = db.runtime.begin_session("sub", Some("r2"), parent_branch).unwrap();
    let mut c = db.session();
    c.agent = Some(child);
    assert_eq!(db.view(&mut c), vec![(1, 20), (2, 5)], "fixture: the child sees row 7");
    (parent, c)
}

/// **The no-key path, where the base holds.** Main never touches key 7. The child can name no key
/// for its inherited row, and it must still be able to REBASE: nothing it depends on moved. Refusing
/// here would leave a child that inherited an insert-then-delete unable to rebase ever again, because
/// the staged entry never goes away. The parent, which has the key in its own `RowCreate`, rebases too.
#[test]
fn a_child_that_inherited_an_insert_then_delete_rebases_when_main_left_the_key_alone() {
    let mut db = Db::new();
    db.seed();
    let mut main = db.session();
    let (mut parent, mut child) = parent_and_child_after_an_insert_then_delete(&mut db);
    db.ok("UPDATE inventory SET qty = 50 WHERE id = 2;", &mut main);

    let rc = db.rebase(&mut child);
    assert!(rc.rebased, "the child's keyless row did not move, yet REBASE refused: {rc:?}");
    assert_eq!((rc.moved_rows, rc.moved_premises), (0, 0), "{rc:?}");
    assert!(rc.detail.is_none(), "a successful REBASE carried a refusal reason: {rc:?}");
    assert!(rc.after >= rc.before, "the fork seq went backwards: {rc:?}");
    assert_eq!(db.view(&mut child), vec![(1, 20), (2, 50)], "the child's rebased view is wrong");

    let rp = db.rebase(&mut parent);
    assert!(rp.rebased, "the parent names the key through its RowCreate and nothing moved: {rp:?}");
    assert_eq!(db.view(&mut parent), vec![(1, 20), (2, 50)]);
}

/// **The no-key path, where the base moved.** Main takes key 7 for a row of its own. Re-pinning
/// either branch would let the inherited `Deleted` hide main's `(7, 77)`, a row neither branch ever
/// deleted, so both are refused, `moved_rows = 1`, and neither view changes. This is the control that
/// stops a keyless row from being treated as "always holds".
#[test]
fn a_child_that_inherited_an_insert_then_delete_is_refused_when_main_took_the_key() {
    let mut db = Db::new();
    db.seed();
    let mut main = db.session();
    let (mut parent, mut child) = parent_and_child_after_an_insert_then_delete(&mut db);
    db.ok("INSERT INTO inventory VALUES (7, 77);", &mut main);
    assert_eq!(db.view(&mut main), vec![(1, 20), (2, 5), (7, 77)], "fixture: main did not take key 7");

    let rc = db.rebase(&mut child);
    assert!(!rc.rebased, "the child was re-pinned over main's row 7: {rc:?}");
    assert_eq!((rc.moved_rows, rc.moved_premises), (1, 0), "{rc:?}");
    assert_eq!(rc.after, rc.before, "a refused REBASE moved the fork seq: {rc:?}");
    assert!(
        rc.detail.as_deref().is_some_and(|d| d.contains("inventory")),
        "the refusal does not name the table: {rc:?}"
    );
    assert_eq!(db.view(&mut child), vec![(1, 20), (2, 5)], "a refused REBASE changed the child");

    let rp = db.rebase(&mut parent);
    assert!(!rp.rebased, "the parent was re-pinned over main's row 7: {rp:?}");
    assert_eq!(rp.moved_rows, 1, "{rp:?}");
    assert_eq!(db.view(&mut parent), vec![(1, 20), (2, 5)], "a refused REBASE changed the parent");
}
