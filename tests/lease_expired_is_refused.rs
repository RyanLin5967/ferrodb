//! F1, adjacent point — **an expired lease is refused, not merely awaited.**
//!
//! Source: `artie-research/frontier/research_reclaim-with-live-children.md` §5 F1, "Adjacent
//! point": `BranchError::LeaseExpired` is defined (`src/branch/types.rs`) and constructed nowhere in
//! `src/`, so no operation refuses a branch whose lease has run out but which the scan has not
//! reached yet. A fork from such a parent succeeds and the child then pins it; a write to it
//! succeeds and is then thrown away with the branch. Neon forbids the first (*"create children from
//! expiring branches"*).
//!
//! # The window these tests stand in
//!
//! Expired-but-not-reaped is an ordinary state, not a race: it lasts up to one scan interval
//! (`DEFAULT_SCAN_MILLIS`, thirty seconds) for every branch that expires, and for as long as a
//! statement holds the runtime lock. This harness has **no reaper at all**, so a branch whose
//! deadline is in the past stays exactly there — `Live`, readable, unreaped — for the whole test.
//! Each test asserts that premise rather than assuming it.
//!
//! # How each test is kept from passing vacuously
//!
//! * **The same operation succeeds first**, on the same branch, while its lease is live. A refusal
//!   that fired for any other reason (a harness that cannot fork at all, a table that does not
//!   exist) would fail the control, not pass the test.
//! * **The refusal must name the lease.** Every `BranchError` reaches a caller flattened into
//!   `FerroError::Branch(String)`, so the text is the only place the variant survives; the tests
//!   match `LeaseExpired`'s own `Display`, which names the branch.
//! * **The refused operation must leave nothing behind** — no child branch, no staged row, no
//!   staged schema edit.
//!
//! The catalog is `TableBranchCatalog` on a sidecar file: the implementation both binaries open.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::types::{BranchId, BranchState, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, TableBranchCatalog};
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
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

/// A deadline in the past on any clock this process can read, without reading one.
const EXPIRED: LeaseDeadline = LeaseDeadline(1);

/// One database, several connections — the shape `tests/w4_stale_branch_crosses_agents.rs` uses,
/// over the durable branch catalog instead of the in-memory log.
struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    branches: Arc<TableBranchCatalog>,
    runtime: Arc<AgentRuntime>,
}

impl Db {
    fn new() -> Db {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("lease.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.path().join("lease.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        recover(&txn).unwrap();
        let catalog = Catalog::create(bp.clone()).unwrap();
        let branches = Arc::new(
            TableBranchCatalog::open_sidecar(&dir.path().join("lease.db.branchcat"), 1).unwrap(),
        );
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::clone(&branches) as Arc<dyn BranchCatalog>));
        Db { _dir: dir, catalog, bp, txn, branches, runtime }
    }

    fn connection(&self) -> Session {
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
        let mut s = self.connection();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
    }

    /// Open an agent session on a fresh connection and return both.
    fn session(&mut self, agent: &str, run: &str) -> (Session, BranchId) {
        let mut s = self.connection();
        self.ok(&format!("BEGIN AGENT SESSION AS '{agent}' RUN '{run}';"), &mut s);
        let branch = s.agent.as_ref().expect("the session holds a branch").branch;
        (s, branch)
    }

    /// Expire `branch`'s lease **and prove it is still there to be refused**: `Live`, and expired
    /// on the clock the runtime decides with.
    fn expire(&self, branch: BranchId) {
        self.branches.renew_lease(branch, EXPIRED).expect("expire the lease");
        let rec = self.branches.get(branch).expect("an expired branch is still readable");
        assert_eq!(
            rec.state,
            BranchState::Live,
            "premise: the branch must be expired but NOT reaped, or these tests are about a \
             reaped branch, which the catalog already refuses"
        );
        assert!(
            rec.lease_deadline.is_expired_at(LeaseDeadline::now_millis()),
            "premise: the lease must actually have expired"
        );
    }
}

/// The text `BranchError::LeaseExpired` renders for `branch`. Matched, rather than the whole
/// message, because the deadline and the clock reading in the rest of it vary by run.
fn names_the_expired_lease(err: &str, branch: BranchId) -> bool {
    err.contains(&format!("lease on branch {branch} expired"))
}

fn ids_seen(out: Outcome) -> Vec<i32> {
    match out {
        Outcome::Rows(rows) => rows
            .iter()
            .map(|r| match r.first() {
                Some(Value::Integer(i)) => *i,
                other => panic!("unexpected id cell: {other:?}"),
            })
            .collect(),
        _ => panic!("expected rows from a SELECT"),
    }
}

#[test]
fn forking_from_a_branch_whose_lease_expired_but_is_not_yet_reaped_is_refused() {
    let mut db = Db::new();
    db.seed();
    let (_parent_conn, parent) = db.session("planner", "r_parent");

    // CONTROL: the same fork, from the same parent, while its lease is live.
    let live_child = db
        .runtime
        .begin_session("sub", Some("r_live_child"), parent)
        .expect("forking from a parent whose lease is live must succeed");
    assert_ne!(live_child.branch, parent);

    db.expire(parent);
    let before = db.branches.live_count().unwrap();

    let refused = db.runtime.begin_session("sub", Some("r_expired_child"), parent);
    let err = match refused {
        Ok(s) => panic!(
            "forked {} from {parent}, whose lease has expired. The child now pins a branch the \
             next scan is entitled to reap, and extends nothing — Neon forbids exactly this",
            s.branch
        ),
        Err(e) => e.to_string(),
    };
    assert!(
        names_the_expired_lease(&err, parent),
        "the fork was refused, but not for the lease — so this test is not about it: {err}"
    );
    assert_eq!(
        db.branches.live_count().unwrap(),
        before,
        "the refused fork still created a branch"
    );
    assert_eq!(
        db.branches.get(parent).unwrap().state,
        BranchState::Live,
        "the refusal must leave the parent for the reaper, not reap or quarantine it itself"
    );
}

#[test]
fn writing_to_a_branch_whose_lease_expired_but_is_not_yet_reaped_is_refused() {
    let mut db = Db::new();
    db.seed();
    let (mut conn, branch) = db.session("writer", "r_write");

    // CONTROL: the same session writes while its lease is live.
    db.ok("INSERT INTO inventory VALUES (2, 5);", &mut conn);

    db.expire(branch);

    // All three verbs, because each reaches the write funnel through its own statement path
    // (`branch_insert`, `branch_update`, `branch_delete`) and a check placed in one of them would
    // leave the other two open.
    for sql in [
        "INSERT INTO inventory VALUES (3, 7);",
        "UPDATE inventory SET qty = 0 WHERE id = 1;",
        "DELETE FROM inventory WHERE id = 2;",
    ] {
        let err = match db.exec(sql, &mut conn) {
            Ok(_) => panic!(
                "`{sql}` was accepted on {branch}, whose lease has expired. The write lands in a \
                 branch the next scan is entitled to reap"
            ),
            Err(e) => e.to_string(),
        };
        assert!(
            names_the_expired_lease(&err, branch),
            "`{sql}` was refused, but not for the lease: {err}"
        );
    }

    // Nothing the refused statements tried reached the branch. Reads are NOT refused — an agent
    // must still be able to look at what it has before the branch goes.
    let seen = ids_seen(db.ok("SELECT id FROM inventory;", &mut conn));
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![1, 2],
        "the branch's view changed after its lease expired: row 3 staged, or row 2 deleted"
    );
    let qty = match db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut conn) {
        Outcome::Rows(rows) => rows[0][0].clone(),
        _ => panic!("expected rows from a SELECT"),
    };
    assert_eq!(qty, Value::Integer(20), "the refused UPDATE reached the branch");
    assert_eq!(db.branches.get(branch).unwrap().state, BranchState::Live);
}

#[test]
fn altering_a_table_on_a_branch_whose_lease_expired_is_refused() {
    // `ALTER TABLE` inside a session does not pass the DML funnel: the executor diverts it to
    // `stage_schema_edit`, which writes the branch's own staged state directly
    // (`src/branch/record.rs`, "The branch-scoped schema path"). A refusal placed only in the DML
    // funnel leaves this door open.
    let mut db = Db::new();
    db.seed();
    let (mut conn, branch) = db.session("migrator", "r_alter");

    // CONTROL.
    db.ok("ALTER TABLE inventory ADD COLUMN note VARCHAR(20);", &mut conn);
    assert_eq!(db.runtime.pending_schema_edits(branch).len(), 1, "the control edit was not staged");

    db.expire(branch);

    let err = match db.exec("ALTER TABLE inventory ADD COLUMN memo VARCHAR(20);", &mut conn) {
        Ok(_) => panic!("a schema edit was staged on {branch}, whose lease has expired"),
        Err(e) => e.to_string(),
    };
    assert!(names_the_expired_lease(&err, branch), "refused, but not for the lease: {err}");
    assert_eq!(
        db.runtime.pending_schema_edits(branch).len(),
        1,
        "the refused edit was staged anyway"
    );
}
