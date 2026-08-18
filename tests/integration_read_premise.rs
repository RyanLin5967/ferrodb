//! B1 — a branch that read a row someone else then changed does not merge silently.
//!
//! # The gap this closes
//!
//! Every agent run retains the exact versions it READ; that retention is what makes causal revert
//! possible. Nothing consulted it at merge. `.reads` had two consumers in the whole runtime and both
//! fed a blind-write *metric*, while the merge engine validated only the cells a branch **wrote**.
//!
//! So two agents could each read one row, each reason from it, each write somewhere else, and both
//! merges were accepted — because nothing the conflict resolver looks at overlaps. The second agent
//! acted on a premise that had already been replaced, and the database said Clean.
//!
//! The canonical shape is a hospital one: two agents each read that one on-call physician remains, each
//! releases a different physician, neither touches the row the other wrote, and the invariant everyone
//! believed they were preserving is gone.
//!
//! # What is checked here
//!
//! That the premise check FIRES (a stale read is held), that it does not fire on disjoint reads (or it
//! would refuse everything), and that a held branch is still queryable — which is what makes quarantine
//! different from rejection: the agent's work is not wrong, it was computed against state that moved.

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
            .open(dir.path().join("premise.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("premise.wal")).unwrap());
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

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE oncall (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO oncall VALUES (1, 100);", &mut s);
        self.ok("INSERT INTO oncall VALUES (2, 200);", &mut s);
        self.ok("INSERT INTO oncall VALUES (3, 300);", &mut s);
    }

    fn qty(&mut self, id: i32) -> Option<i32> {
        let mut s = self.session();
        match self.ok(&format!("SELECT qty FROM oncall WHERE id = {id};"), &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| r.first()).and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            }),
            _ => None,
        }
    }
}

/// **The premise check fires: a branch whose read was replaced is held rather than published.**
#[test]
fn a_branch_that_read_a_row_another_branch_then_changed_is_held() {
    let mut db = Db::new();
    db.seed();

    // Both agents read row 1 — the shared premise — as a POINT read, which is what retains exact
    // versions rather than a predicate summary.
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_a';", &mut a);
    db.ok("SELECT qty FROM oncall WHERE id = 1;", &mut a);

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r_b';", &mut b);
    db.ok("SELECT qty FROM oncall WHERE id = 1;", &mut b);
    let b_branch = b.agent.as_ref().unwrap().branch;

    // They write DIFFERENT rows, so nothing the conflict resolver compares overlaps. This is the
    // whole point: without a read-set check there is nothing here to object to.
    db.ok("UPDATE oncall SET qty = 111 WHERE id = 1;", &mut a);
    db.ok("UPDATE oncall SET qty = 222 WHERE id = 2;", &mut b);

    // A merges first and moves the row B read.
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(1), Some(111), "A's merge did not publish, so B's premise never moved");

    // B's merge must not publish: it reasoned from row 1 as it was before A changed it.
    db.ok("MERGE;", &mut b);
    assert_eq!(
        db.qty(2),
        Some(200),
        "B published a write computed from a row that had already been replaced — the read-set was \
         retained and never validated, so nothing objected"
    );

    let rec = db.runtime.branches().get(b_branch).expect("branch record");
    assert_eq!(
        rec.state,
        BranchState::Quarantined,
        "B was not held; a stale premise has to land somewhere an operator can see it"
    );
    let reason = db.runtime.quarantine_reason(b_branch).unwrap_or_default();
    assert!(
        reason.contains("read-premise") && reason.contains("changed in the base"),
        "the reason does not name what actually happened: {reason}"
    );

    // Quarantine, not rejection: the branch is held, and the record still reports itself readable.
    // That is the property that separates the two — a rejected branch's work is gone, a held branch's
    // work is inspectable — and `check_readable` returning Ok for a quarantined record is where the
    // design puts it. Asserted on the record rather than through `AS OF BRANCH <name>`, because name
    // resolution is a separate question and a held branch's name is not the thing under test here.
    assert!(
        rec.check_readable(b_branch).is_ok(),
        "a held branch reported itself unreadable, which makes quarantine indistinguishable from \
         rejection: {:?}",
        rec.check_readable(b_branch).err().map(|e| e.to_string())
    );
}

/// **Anti-vacuity, and the half that matters most: disjoint reads still merge.**
///
/// A check that held every second merge would satisfy the test above completely. Here the two agents
/// read *different* rows, so neither premise moves, and both merges must publish.
#[test]
fn two_branches_reading_different_rows_both_merge() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_a';", &mut a);
    db.ok("SELECT qty FROM oncall WHERE id = 1;", &mut a);
    db.ok("UPDATE oncall SET qty = 111 WHERE id = 1;", &mut a);

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r_b';", &mut b);
    db.ok("SELECT qty FROM oncall WHERE id = 3;", &mut b);
    db.ok("UPDATE oncall SET qty = 333 WHERE id = 3;", &mut b);
    let b_branch = b.agent.as_ref().unwrap().branch;

    db.ok("MERGE;", &mut a);
    db.ok("MERGE;", &mut b);

    assert_eq!(db.qty(1), Some(111), "A did not publish");
    assert_eq!(
        db.qty(3),
        Some(333),
        "B was held even though the row it read was never touched — the check is refusing on \
         something other than a moved premise"
    );
    // A branch that merged cleanly is sealed and its id slot retired, so there is no record left to
    // inspect — which is itself the evidence it was not held. What must be absent is a reason.
    assert!(
        db.runtime.quarantine_reason(b_branch).is_none(),
        "B was quarantined with no stale read: {:?}",
        db.runtime.quarantine_reason(b_branch)
    );
}
