//! D194 step 4 — `REBASE`: move a branch's view of main to NOW, and refuse when that would lie.
//!
//! A branch reads main as of its fork (D194). `REBASE [BRANCH b_n];` re-pins it to main as it
//! stands, so an agent that wants fresh base data asks for it rather than getting it silently.
//! What the branch already DEPENDS ON is checked against the new instant first:
//!
//! * every staged row's base image (its `base_rows` entry) must be the row's image at the new
//!   instant, or the re-pin would leave `base_rows` describing a fork point that no longer exists;
//! * every exact read premise must still be the version visible at the new instant, the same rule
//!   the read-premise gate applies at `MERGE`;
//! * every table's shape must be the one the branch forked from.
//!
//! Any of those moved: nothing changes and the report names what moved. Nothing staged is ever
//! rewritten by `REBASE`: a staged edit whose base moved is reported and kept. `MERGE` is what
//! composes it three-way against main.
//!
//! Every assertion reads the statement's typed result (`AgentOutput::to_rows`) by column name, so
//! this file needs no type that `REBASE` introduces.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::types::BranchState;
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
            .open(dir.path().join("rebase.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("rebase.wal")).unwrap());
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

/// The error from a statement that must fail. `Outcome` has no `Debug`, so this matches rather
/// than calling `expect_err`.
fn err_of(r: Result<Outcome, FerroError>, what: &str) -> FerroError {
    match r {
        Ok(_) => panic!("{what}"),
        Err(e) => e,
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

/// **REBASE refreshes the base a branch reads.** Main moves after the fork in all three ways —
/// UPDATE, INSERT, DELETE — the branch does not see it (D194), `REBASE` succeeds because the branch
/// depends on nothing that moved, and afterwards the branch reads main as it stands. `AS OF BRANCH`
/// from outside agrees, because it is the same pinned view.
#[test]
fn rebase_refreshes_the_base_a_branch_reads() {
    let mut db = Db::new();
    db.seed();
    let mut main = db.session();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);

    db.ok("UPDATE inventory SET qty = 99 WHERE id = 1;", &mut main);
    db.ok("INSERT INTO inventory VALUES (4, 40);", &mut main);
    db.ok("DELETE FROM inventory WHERE id = 2;", &mut main);
    assert_eq!(db.view(&mut main), vec![(1, 99), (4, 40)], "fixture: main did not move");
    assert_eq!(db.view(&mut a), vec![(1, 20), (2, 5)], "premise: the branch reads as of its fork");

    let r = db.rebase(&mut a);
    assert!(r.rebased, "a branch that depends on nothing that moved was refused: {r:?}");
    assert_eq!((r.moved_rows, r.moved_premises), (0, 0), "{r:?}");
    assert!(r.after >= r.before, "the fork seq went backwards: {r:?}");

    assert_eq!(db.view(&mut a), vec![(1, 99), (4, 40)], "REBASE did not move the branch's view");
    let name = a.agent.as_ref().unwrap().branch_name.clone();
    let mut observer = db.session();
    let mut seen: Vec<(Value, Value)> =
        match db.ok(&format!("SELECT id, qty FROM inventory AS OF BRANCH {name};"), &mut observer) {
            Outcome::Rows(r) => r.into_iter().map(|row| (row[0].clone(), row[1].clone())).collect(),
            _ => panic!("expected rows"),
        };
    seen.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap_or(std::cmp::Ordering::Equal));
    assert_eq!(
        seen,
        vec![(Value::Integer(1), Value::Integer(99)), (Value::Integer(4), Value::Integer(40))],
        "AS OF BRANCH disagrees with the branch's own rebased view"
    );
}

/// **Staged edits survive a REBASE**, and the branch still merges them. The branch writes row 1;
/// main changes row 2 and adds row 3, neither of which the branch touched. After `REBASE` the branch
/// sees main's rows AND its own edit, and `MERGE` publishes the edit cleanly.
#[test]
fn staged_edits_survive_a_rebase_and_still_merge() {
    let mut db = Db::new();
    db.seed();
    let mut main = db.session();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = 21 WHERE id = 1;", &mut a);

    db.ok("UPDATE inventory SET qty = 50 WHERE id = 2;", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 30);", &mut main);

    let r = db.rebase(&mut a);
    assert!(r.rebased, "the staged row's base did not move, yet REBASE refused: {r:?}");
    assert_eq!(db.view(&mut a), vec![(1, 21), (2, 50), (3, 30)], "the staged edit or main's rows are missing");
    assert_eq!(db.view(&mut main), vec![(1, 20), (2, 50), (3, 30)], "REBASE published something");

    let merged = match db.ok("MERGE;", &mut a) {
        Outcome::Agent(out) => out.to_rows(),
        _ => panic!("expected a merge report"),
    };
    let at = merged.column_index("applied_to_target").expect("declared");
    assert!(matches!(merged.rows[0][at], Value::Boolean(true)), "the rebased branch did not merge: {:?}", merged.rows);
    assert_eq!(db.view(&mut main), vec![(1, 21), (2, 50), (3, 30)]);
}

/// **A staged edit whose base moved is reported, and kept — never silently overwritten.** Main
/// changes the very row the branch staged. `REBASE` refuses, names one moved row, and changes
/// nothing: the staged value is intact, and the branch still reads its OTHER rows as of the old
/// fork, because a refused REBASE is all-or-nothing.
#[test]
fn a_staged_edit_whose_base_moved_is_reported_and_kept() {
    let mut db = Db::new();
    db.seed();
    let mut main = db.session();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);

    db.ok("UPDATE inventory SET qty = 99 WHERE id = 1;", &mut main);
    db.ok("UPDATE inventory SET qty = 50 WHERE id = 2;", &mut main);

    let r = db.rebase(&mut a);
    assert!(!r.rebased, "REBASE moved a branch whose staged row's base had moved: {r:?}");
    assert_eq!(r.moved_rows, 1, "{r:?}");
    assert_eq!(r.after, r.before, "a refused REBASE moved the fork seq: {r:?}");
    assert!(
        r.detail.as_deref().is_some_and(|d| d.contains("inventory")),
        "the refusal does not name the table whose row moved: {r:?}"
    );
    assert_eq!(
        db.view(&mut a),
        vec![(1, 15), (2, 5)],
        "a refused REBASE changed the branch: the staged 15 must survive and row 2 must still be the fork's"
    );
    assert_eq!(db.view(&mut main), vec![(1, 99), (2, 50)], "REBASE wrote to main");
}

/// **A read premise that moved refuses the REBASE; one that held does not.** Both branches fork
/// before B publishes row 1. C point-read row 2, which B did not touch: its premise holds at the
/// new instant and it rebases, seeing B's row. A point-read row 1 AFTER B published it, so A saw the
/// fork-time version — re-pinning A past B's version would make that read a lie about the view A
/// has, so A is refused and still reads the old value.
#[test]
fn a_moved_read_premise_refuses_the_rebase_and_a_held_one_does_not() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'ra';", &mut a);
    let mut c = db.session();
    db.ok("BEGIN AGENT SESSION AS 'c' RUN 'rc';", &mut c);
    db.ok("SELECT qty FROM inventory WHERE id = 2;", &mut c);

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'rb';", &mut b);
    db.ok("UPDATE inventory SET qty = 111 WHERE id = 1;", &mut b);
    db.ok("MERGE;", &mut b);

    // C: the premise on row 2 held.
    let rc = db.rebase(&mut c);
    assert!(rc.rebased, "C's only premise (row 2) did not move, yet REBASE refused: {rc:?}");
    assert!(rc.after > rc.before, "B's merge advanced the clock, so C's fork seq must move: {rc:?}");
    assert_eq!(db.view(&mut c), vec![(1, 111), (2, 5)], "C does not see B's published row");

    // A: the premise on row 1 moved.
    match db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut a) {
        Outcome::Rows(r) => assert_eq!(r[0][0], Value::Integer(20), "premise: A reads as of its fork"),
        _ => panic!("expected rows"),
    }
    let ra = db.rebase(&mut a);
    assert!(!ra.rebased, "A was re-pinned past a version it read the predecessor of: {ra:?}");
    assert_eq!(ra.moved_premises, 1, "{ra:?}");
    assert_eq!(db.view(&mut a), vec![(1, 20), (2, 5)], "a refused REBASE changed A's view");
}

/// **A child's REBASE moves the child, and never its parent — and each is validated on its own.**
/// The child forked from a live parent shares the parent's pin and inherits its staged row. Re-pinning
/// the child must not re-pin the parent: the pin is per-branch state, shared by `Arc` only until one
/// side moves. The child's INHERITED staged row is part of what it depends on, so once main changes
/// that row both are refused.
#[test]
fn a_childs_rebase_moves_the_child_and_never_its_parent() {
    let mut db = Db::new();
    db.seed();
    let mut main = db.session();
    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut parent);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut parent);

    let parent_branch = parent.agent.as_ref().unwrap().branch;
    let child = db.runtime.begin_session("sub", Some("r2"), parent_branch).unwrap();
    let mut c = db.session();
    c.agent = Some(child);

    db.ok("UPDATE inventory SET qty = 50 WHERE id = 2;", &mut main);

    let rc = db.rebase(&mut c);
    assert!(rc.rebased, "the child's inherited staged row did not move, yet REBASE refused: {rc:?}");
    assert_eq!(db.view(&mut c), vec![(1, 15), (2, 50)], "the child lost its inherited row or missed main's");
    assert_eq!(db.view(&mut parent), vec![(1, 15), (2, 5)], "the child's REBASE moved its parent's pin");

    let rp = db.rebase(&mut parent);
    assert!(rp.rebased, "{rp:?}");
    assert_eq!(db.view(&mut parent), vec![(1, 15), (2, 50)]);

    // Main now changes the row both branches staged over (the child by inheritance).
    db.ok("UPDATE inventory SET qty = 99 WHERE id = 1;", &mut main);
    let rc = db.rebase(&mut c);
    assert!(!rc.rebased, "the child's inherited staged row's base moved, yet it rebased: {rc:?}");
    assert_eq!(rc.moved_rows, 1, "{rc:?}");
    let rp = db.rebase(&mut parent);
    assert!(!rp.rebased, "{rp:?}");
    assert_eq!(rp.moved_rows, 1, "{rp:?}");
    assert_eq!(db.view(&mut c), vec![(1, 15), (2, 50)], "a refused REBASE changed the child");
    assert_eq!(db.view(&mut parent), vec![(1, 15), (2, 50)], "a refused REBASE changed the parent");
}

/// **A held branch is held.** `REBASE` moves what a branch sees, and quarantine exists so an operator
/// can inspect the branch as the gate judged it; letting REBASE through would change the evidence
/// under the hold. Refused with the reason, and the branch is still quarantined afterwards.
#[test]
fn rebase_refuses_a_quarantined_branch() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'ra';", &mut a);
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'rb';", &mut b);
    let b_branch = b.agent.as_ref().unwrap().branch;
    db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b);

    db.ok("UPDATE inventory SET qty = 111 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);
    db.ok("UPDATE inventory SET qty = 222 WHERE id = 2;", &mut b);
    db.ok("MERGE;", &mut b);
    assert_eq!(
        db.runtime.branches().get(b_branch).unwrap().state,
        BranchState::Quarantined,
        "fixture: B was not held by the read-premise gate"
    );

    let err = err_of(db.exec("REBASE;", &mut b), "REBASE of a quarantined branch succeeded");
    assert!(err.to_string().contains("quarantined"), "refused for the wrong reason: {err}");
    assert_eq!(db.runtime.branches().get(b_branch).unwrap().state, BranchState::Quarantined);
}

/// **No session, no branch named: an error that says so**, the same one `DIFF;` / `MERGE;` give.
#[test]
fn rebase_outside_a_session_names_the_missing_branch() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    let err = err_of(db.exec("REBASE;", &mut s), "REBASE with no session and no branch succeeded");
    assert!(err.to_string().contains("no agent session"), "{err}");
}
