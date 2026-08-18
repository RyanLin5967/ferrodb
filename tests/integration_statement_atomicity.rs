//! B2 — a refused write inside an agent session must leave nothing behind.
//!
//! # The claim that was false
//!
//! `AgentRuntime::stage` refuses an escrow over-draw *before* anything is recorded, and the comment
//! above it says so:
//!
//! ```text
//! // Write-time escrow. This runs BEFORE anything is recorded, so a refused overdraw leaves
//! // no trace in the workspace, the frame or the log — the statement simply fails, which is
//! // the entire point of moving the check off the merge path.
//! ```
//!
//! That is true of one row and false of one statement. `branch_update` resolves its assignments, then
//! loops over the matched rows calling `self.stage(...)?` per row. A multi-row `UPDATE` whose *second*
//! row over-draws therefore returns an error to the client with the *first* row already staged into the
//! workspace, the frame and the log — and with the first row's escrow units already spent.
//!
//! A refusal that half-applied is not a refusal. For an agent-isolation database whose whole pitch is
//! that a rejected write leaves the shared state untouched, this is the wrong half to get wrong.
//!
//! # Why the existing escrow suite could not see it
//!
//! Every escrow test writes `WHERE id = 1` — a single row. With one row per statement, per-row and
//! per-statement atomicity are the same property, so the suite pinned the weaker one without noticing.
//! The breaking shape is a statement that matches more than one row and is refused on a row that is not
//! the first, and nothing generated that shape.

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
use ferrodb::tel::ids::{ColId, RowId};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const QTY: ColId = ColId(1);

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
            .open(dir.path().join("atomic.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("atomic.wal")).unwrap());
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

    /// Every `(id, qty)` the given session can see, sorted by id.
    fn rows(&mut self, s: &mut Session) -> Vec<(i32, i32)> {
        match self.ok("SELECT id, qty FROM inventory;", s) {
            Outcome::Rows(rows) => {
                let mut out: Vec<(i32, i32)> = rows
                    .iter()
                    .map(|r| match (&r[0], &r[1]) {
                        (Value::Integer(a), Value::Integer(b)) => (*a, *b),
                        other => panic!("unexpected row: {other:?}"),
                    })
                    .collect();
                out.sort_unstable();
                out
            }
            _ => panic!("expected rows"),
        }
    }

    /// Two rows, both bounded, and a branch allowed to spend on one of them but not the other.
    ///
    /// Returns the session, already inside an agent session.
    fn seed_two_bounded_rows(&mut self) -> Session {
        let mut setup = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut setup);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut setup);
        self.ok("INSERT INTO inventory VALUES (2, 20);", &mut setup);
        // Headroom above a floor of 0 on both rows.
        self.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();
        self.runtime.open_escrow("inventory", RowId(2), QTY, 20).unwrap();

        let mut s = self.session();
        self.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_a';", &mut s);
        let branch = s.agent.as_ref().unwrap().branch;
        // Enough to take 12 from ONE row, and only 1 unit on the other. A statement that takes 12 from
        // both must therefore be refused on whichever of them it reaches second.
        self.runtime.claim_escrow(branch, "inventory", RowId(1), QTY, 12).unwrap();
        self.runtime.claim_escrow(branch, "inventory", RowId(2), QTY, 1).unwrap();
        s
    }
}

/// **A refused multi-row statement records nothing.**
#[test]
fn a_refused_multi_row_update_leaves_no_row_rewritten() {
    let mut db = Db::new();
    let mut s = db.seed_two_bounded_rows();

    let before = db.rows(&mut s);
    assert_eq!(before, vec![(1, 20), (2, 20)], "the seed is not what this test assumes: {before:?}");

    // Matches BOTH rows. One is within its claim, the other is not.
    let msg = match db.exec("UPDATE inventory SET qty = qty - 12 WHERE qty >= 0;", &mut s) {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("a statement that over-draws on one of its rows was accepted"),
    };

    let after = db.rows(&mut s);
    assert_eq!(
        after, before,
        "the statement was refused with `{msg}` and still rewrote a row: {before:?} -> {after:?}. A \
         refusal that half-applied is not a refusal — for a database whose pitch is that a rejected \
         agent write leaves shared state untouched, this is the wrong half to get wrong."
    );
}

/// **And the budget the refused statement did not spend is still there.**
///
/// The first row's units were charged before the second row was refused, so the obvious retry — take
/// less — failed too, on a claim that had already been drained by a statement that never landed. This is
/// the half that makes the defect actively worse than a plain error: the caller cannot recover.
#[test]
fn a_refused_statement_does_not_consume_the_budget_it_never_used() {
    let mut db = Db::new();
    let mut s = db.seed_two_bounded_rows();

    let _ = db.exec("UPDATE inventory SET qty = qty - 12 WHERE qty >= 0;", &mut s);

    // Row 1 was allowed 12 and the refused statement must not have spent any of it. Taking 12 from row
    // 1 alone is now the caller's natural retry, and it has to work.
    db.exec("UPDATE inventory SET qty = qty - 12 WHERE id = 1;", &mut s).unwrap_or_else(|e| {
        panic!(
            "the retry was refused with `{e}`: the failed statement charged row 1's escrow claim for a \
             write it rolled back, so the caller cannot recover from its own refusal"
        )
    });
    let after = db.rows(&mut s);
    assert_eq!(after, vec![(1, 8), (2, 20)], "the retry did not land cleanly: {after:?}");
}

/// Anti-vacuity: a multi-row statement that stays inside every claim still applies to every row.
///
/// Without this, code that refused every multi-row statement outright would pass both tests above.
#[test]
fn a_multi_row_update_within_budget_still_applies_to_every_row() {
    let mut db = Db::new();
    let mut s = db.seed_two_bounded_rows();

    // 1 unit from each row: inside row 1's claim of 12 and exactly row 2's claim of 1.
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE qty >= 0;", &mut s);
    let after = db.rows(&mut s);
    assert_eq!(
        after,
        vec![(1, 19), (2, 19)],
        "a statement within every claim did not apply to both rows: {after:?}"
    );
}
