//! W4 / D158 item 1 — a connection whose branch was reaped must not be answered about the
//! **new occupant** of its id slot.
//!
//! # Why this is reachable, and not a race you have to win
//!
//! `State.workspaces` is `BTreeMap<u64, Workspace>` — keyed by the id SLOT — while a `BranchId`
//! is `{ id, generation }` and the catalog recycles a reaped slot (`reaper.rs:715`
//! `release_id`, `catalog.rs:424` `fork` pops it) with the generation bumped
//! (`catalog.rs:528`). A connection caches its `AgentSession` in `execution::session::Session`
//! and hands `a.branch` to the runtime on every statement (`dispatch.rs:519`) — it is never
//! re-read from the catalog.
//!
//! Nothing renews a lease. `renew_lease` has no caller in `src` outside the catalogs' own tests,
//! so a session's lease is set once at fork (`DEFAULT_LEASE_MILLIS`, 15 minutes) and expires
//! while the connection is open and idle. That is the ordinary shape of an agent run, not an
//! edge: the lease reaper exists precisely to collect branches whose agent stopped talking.
//! So the interleaving below needs no timing luck at all —
//!
//!   1. connection A opens a session on slot S, generation 0;
//!   2. A goes quiet past its lease; the sweep reaps S, bumps the generation and releases the
//!      slot, and `forget_branches` drops A's workspace (`lease_thread.rs:634-674`);
//!   3. connection B opens a session and `fork` pops S back off the free list at generation 1;
//!   4. A speaks again, holding `S@g0`.
//!
//! The reap is performed here by the three catalog calls the sweep itself makes —
//! `set_state(.., Reaped)` (which is what bumps the generation), `release_id`, and the
//! `runtime.forget_branches(&reaped)` that `scan_once` calls inside its `with_lock`. Driving a
//! wall-clock lease thread would test the timer, which `lease_thread`'s own tests already do;
//! what is under test here is what the runtime answers a stale handle, and that is decided by
//! the state the sweep leaves behind, not by which thread produced it.
//!
//! # What each test asserts
//!
//! Both run over the real statement path — `Scanner` -> `Parser` -> `executor::run` -> two
//! `execution::session::Session`s sharing one `AgentRuntime`, which is the pgwire server's own
//! shape (a thread and a `Session` per connection). Neither asserts on an internal map.
//!
//! * the READ half: A's `SELECT` inside its dead session must not return B's staged, unmerged
//!   row. In a database whose thesis is agent isolation this is the most expensive answer
//!   available.
//! * the DESTRUCTIVE half: A's `ABANDON;` binds to `current` — its own stale `BranchId`
//!   (`binder.rs:289`) — and `seal` removes `workspaces[&branch.id]` and unbinds `names[..]`
//!   **before** its first catalog read. This is the hazard `forget_one_branch` (`runtime.rs:6227`)
//!   already validates the whole `BranchId` against, with a comment saying acting on a stale
//!   answer "would then delete a LIVE agent's workspace, release its escrow and unbind its name".
//!   Only that one write path took the argument.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{AgentRuntime, BranchResolver};
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, BranchState};
use ferrodb::branch::BranchCatalog;
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

/// One database, several connections — the same shape `integration_capability_envelope.rs` uses.
struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
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
            .open(dir.path().join("recycle.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.path().join("recycle.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        recover(&txn).unwrap();
        let catalog = Catalog::create(bp.clone()).unwrap();
        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::clone(&branches) as Arc<dyn BranchCatalog>));
        Db { _dir: dir, catalog, bp, txn, runtime }
    }

    /// A fresh connection over the shared runtime, exactly as the server builds one per socket.
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

    /// The qty this connection sees for row 1, whatever session it holds.
    fn qty_seen_by(&mut self, s: &mut Session) -> Result<i32, FerroError> {
        match self.exec("SELECT qty FROM inventory WHERE id = 1;", s)? {
            Outcome::Rows(rows) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Integer(i)) => Ok(*i),
                other => panic!("unexpected qty cell: {other:?}"),
            },
            _ => panic!("expected rows from a SELECT"),
        }
    }
}

/// What the lease sweep leaves behind for one expired branch, in the order it leaves it.
///
/// `reaper.rs:697-715`: `set_state(.., Reaped)` — which is the call that bumps the generation —
/// then `release_id`, which returns the slot to the free list. `lease_thread.rs:674`:
/// `runtime.forget_branches(&reaped)`, inside the same statement-lock hold.
fn sweep_reaps(rt: &AgentRuntime, branch: BranchId) {
    let rec = rt.branches().get(branch).expect("the branch is live before the sweep");
    rt.branches().set_state(branch, rec.state, BranchState::Reaped).expect("mark reaped");
    rt.branches().release_id(branch.id);
    rt.forget_branches(&[branch]);
    assert!(
        rt.branches().get(branch).is_err(),
        "the catalog must refuse the old generation once the slot is reaped"
    );
}

/// Drive both connections to the moment of the collision: A holds `S@g0`, B holds `S@g1` and has
/// staged a row nobody has merged. Returns `(conn_a, stale_branch, conn_b, slot)`.
fn recycled_slot(db: &mut Db) -> (Session, BranchId, Session, u64) {
    db.seed();

    let mut conn_a = db.connection();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'run-a';", &mut conn_a);
    let stale = conn_a.agent.as_ref().expect("connection A holds a session").branch;
    let slot = stale.id;
    assert_eq!(stale.generation, 0, "a fresh slot starts at generation 0");

    // A goes quiet past its lease. Nothing renews it; the sweep collects the branch with the
    // connection still open and still holding `stale`.
    sweep_reaps(&db.runtime, stale);
    assert!(
        conn_a.agent.is_some(),
        "the sweep touches the runtime's state, not a connection's cached session — if this ever \
         fails, the premise of this file has changed and the tests below are about nothing"
    );

    let mut conn_b = db.connection();
    db.ok("BEGIN AGENT SESSION AS 'agent-b' RUN 'run-b';", &mut conn_b);
    let live = conn_b.agent.as_ref().expect("connection B holds a session").branch;
    assert_eq!(live.id, slot, "this test needs the SLOT recycled; the allocator did not reuse it");
    assert_eq!(live.generation, 1, "a recycled slot must come back at a new generation");
    assert_ne!(live, stale, "the two handles must differ only in generation");

    // B stages a row. It is in B's workspace and nowhere else: unmerged, invisible to the shared
    // tables, and — this is the whole claim of the product — invisible to every other agent.
    db.ok("UPDATE inventory SET qty = 999 WHERE id = 1;", &mut conn_b);

    (conn_a, stale, conn_b, slot)
}

/// **The isolation break.** A's SELECT, inside a session whose branch is gone, must not be shown
/// another agent's uncommitted row.
///
/// The refusal it should get is the one the rest of that session's surface already gives — "no
/// agent session on branch" — because the branch really is gone. Returning `20` would be wrong in
/// a different way (it would report success for a session that cannot retain its read, which
/// `record_read`'s own comment rules out), so the assertion is on a refusal, and separately on
/// `999` never being the answer.
#[test]
fn a_reaped_sessions_select_must_not_read_the_new_occupants_staged_row() {
    let mut db = Db::new();
    let (mut conn_a, stale, mut conn_b, slot) = recycled_slot(&mut db);

    // The control, and it is not decoration: if B's write were already visible outside B, the
    // assertion below would pass for a reason that has nothing to do with slot recycling.
    let mut bystander = db.connection();
    assert_eq!(
        db.qty_seen_by(&mut bystander).expect("a plain connection can read"),
        20,
        "B's staged write must be invisible to a connection holding no agent session"
    );
    assert_eq!(
        db.qty_seen_by(&mut conn_b).expect("B can read its own session"),
        999,
        "B must see its own staged row"
    );

    let answer = db.qty_seen_by(&mut conn_a);
    assert!(
        answer.is_err(),
        "connection A's session was reaped and slot {slot} now belongs to agent-b at generation \
         1, yet A's SELECT on {stale} was answered: qty = {:?}. `workspaces` is keyed by the id \
         slot alone, so A was handed agent-b's workspace. 999 is agent-b's staged, unmerged row.",
        answer.as_ref().ok()
    );
}

/// **The destructive half.** `ABANDON;` binds to the connection's own cached `BranchId`, and
/// `seal` empties `workspaces[&branch.id]` and `names` before it ever reads the catalog. A dead
/// session's ABANDON must not retire a live agent's session.
///
/// Asserted on what B can still do afterwards, not on a map: a count or a return value is
/// satisfied by removing the wrong thing, and which workspace went is the whole question.
#[test]
fn a_reaped_sessions_abandon_must_not_retire_the_new_occupants_session() {
    let mut db = Db::new();
    let (mut conn_a, stale, mut conn_b, slot) = recycled_slot(&mut db);
    let live = conn_b.agent.as_ref().expect("B holds a session").branch;

    // A's connection tidies up. It has no idea its branch is gone; `ABANDON;` with no branch
    // named resolves to `current`, which is `stale`.
    let abandoned = db.exec("ABANDON;", &mut conn_a);

    assert_eq!(
        db.runtime.resolve_branch(&format!("b_{slot}")).ok(),
        Some(live),
        "b_{slot} must still name agent-b's live branch {live}; A's ABANDON of the dead {stale} \
         unbound it"
    );
    let b_still_works = db.qty_seen_by(&mut conn_b);
    assert_eq!(
        b_still_works.as_ref().ok(),
        Some(&999),
        "agent-b's session was destroyed by agent-a's ABANDON of a branch that no longer exists: \
         B's own SELECT on {live} now answers {b_still_works:?}. `seal` removes the workspace and \
         unbinds the name keyed by the id slot alone, before its first catalog read."
    );

    // The refusal arrives, and it arrives too late to matter — recorded so that a later change
    // which only makes `seal` louder cannot be mistaken for a fix.
    assert!(abandoned.is_err(), "ABANDON of a reaped branch must be refused, and it returned Ok");
}
