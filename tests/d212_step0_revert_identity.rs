//! **D212 Step 0 — the two live wrong answers REVERT gave, whatever the durability decision.**
//!
//! Ledger rows D217 and D218, SCALE-DESIGN "D212 — REVERT metadata survives a restart".
//!
//! * **D217.** `next_merge` lived only in the runtime's memory, so a restarted server minted `m_1`
//!   again, and `REVERT MERGE m_1` typed from a report written before the restart reverted the NEW
//!   merge. The refusal that looked safe — "unknown merge" for a pre-restart id — held only until the
//!   post-restart counter reached the old id.
//! * **D218.** `REVERT` recorded nothing, so a second `REVERT MERGE` of one id inverted every op
//!   again: an `Add` merge moved its cell twice. And a cascade committed one op at a time, so a
//!   failure part-way left some ops inverted and nothing saying which.
//!
//! # What makes these tests mean something
//!
//! Every test drives the SQL surface a user drives, and every restart here is real: the database's
//! files are checkpointed, every in-process object is dropped, and a new `Catalog`, WAL, buffer pool,
//! branch catalog and `AgentRuntime` are built from the files alone — the sequence `cli.rs` runs. No
//! `Arc` survives the restart, so nothing a process happens to carry can stand in for durability.
//!
//! Every test compiles against the base `9aa6968`, and none names an API the fix adds. Every one
//! except `a_live_dependent_the_cascade_could_not_undo_stays_revertible_once_it_publishes` is
//! written to be RED there; the reason each fails at the base is stated on it and pre-registered in
//! `frontier/lane_d212_revert.md`. That one is a regression test for a defect the first Step 0
//! commit introduced (`3fd20eb`), so it is green at the base and red at that commit.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::MergeReport;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::revert::RevertPlan;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::ids::TxnId;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;

/// One database on disk, and everything a process builds over it.
struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    dir: PathBuf,
}

impl Db {
    /// Open the database in `dir`, creating it if it is not there — `cli.rs`'s sequence: recover
    /// the WAL, open or create the catalog, rebuild indexes after a recovery.
    fn open(dir: &Path) -> Db {
        let path = dir.join("d212.db");
        let existed = path.exists();
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join("d212.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let recovered = recover(&txn).unwrap();
        let mut catalog = if existed {
            Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID).unwrap()
        } else {
            Catalog::create(bp.clone()).unwrap()
        };
        if recovered {
            rebuild_indexes(&mut catalog, &bp).unwrap();
            txn.checkpoint().unwrap();
        }
        // A durable branch catalog, as the CLI has: branch ids and generations must not restart
        // either, or a "restart" here would be easier on the runtime than a real one.
        let branches = LogBranchCatalog::open(&dir.join("d212.branches"), 1).unwrap();
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::new(branches) as Arc<dyn BranchCatalog>));
        Db { catalog, bp, txn, runtime, dir: dir.to_path_buf() }
    }

    /// A clean restart: checkpoint, drop every object this process built, reopen from the files.
    fn restart(self) -> Db {
        self.txn.checkpoint().unwrap();
        let dir = self.dir.clone();
        drop(self);
        Db::open(&dir)
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
        match self.exec(sql, s) {
            Ok(o) => o,
            Err(e) => panic!("{sql} failed: {e}"),
        }
    }

    /// `inventory` with the given rows, written by plain SQL outside any agent session.
    fn seed(&mut self, rows: &[(i32, i32)]) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        for (id, qty) in rows {
            self.ok(&format!("INSERT INTO inventory VALUES ({id}, {qty});"), &mut s);
        }
    }

    /// Open an agent session and return it with its txn id.
    fn agent(&mut self, name: &str) -> (Session, TxnId) {
        let mut s = self.session();
        let txn = match self.ok(&format!("BEGIN AGENT SESSION AS '{name}' RUN 'r_{name}';"), &mut s)
        {
            Outcome::Agent(AgentOutput::SessionStarted(a)) => a.txn,
            _ => panic!("BEGIN AGENT SESSION did not report a session"),
        };
        (s, txn)
    }

    fn merge(&mut self, s: &mut Session) -> MergeReport {
        match self.ok("MERGE;", s) {
            Outcome::Agent(AgentOutput::Merge(m)) => m,
            _ => panic!("MERGE did not return a merge report"),
        }
    }

    /// Whether row `id` exists, read by plain SQL outside any session.
    fn has_row(&mut self, id: i32) -> bool {
        let mut s = self.session();
        match self.ok("SELECT id, qty FROM inventory;", &mut s) {
            Outcome::Rows(r) => r.iter().any(|row| row[0] == Value::Integer(id)),
            _ => panic!("SELECT did not return rows"),
        }
    }

    /// `qty` of row `id`, read by plain SQL outside any session, so nothing is retained.
    fn qty_of(&mut self, id: i32) -> i32 {
        let mut s = self.session();
        let rows = match self.ok("SELECT id, qty FROM inventory;", &mut s) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        let row = rows
            .into_iter()
            .find(|r| r[0] == Value::Integer(id))
            .unwrap_or_else(|| panic!("row {id} is missing"));
        match row[1] {
            Value::Integer(q) => q,
            ref other => panic!("qty of row {id} is {other:?}, not an integer"),
        }
    }
}

fn plan(out: Outcome) -> RevertPlan {
    match out {
        Outcome::Agent(AgentOutput::Revert(p)) => p,
        _ => panic!("REVERT did not return a revert plan"),
    }
}

// ---- D217 ------------------------------------------------------------------------------------

/// **The ledger's red test: merge, reopen, merge, and `REVERT MERGE m_1` must not touch the second
/// merge.**
///
/// RED at the base, at `assert_ne!(second.merge_id, first.merge_id)`: the post-restart merge is `m_1`
/// again. The assertions after it are what that reuse would have cost — `REVERT MERGE m_1` reverting
/// the post-restart merge, row 2 back to 20 — and they are not reached at the base.
///
/// The assertions are the ones that must hold whether or not pre-restart merges are revertible: at
/// Step 0 the revert of `m_1` is REFUSED as belonging to an earlier server run; under option (a) it
/// reverts the pre-restart merge. Either way it never touches the post-restart one, which is the
/// whole of D217.
#[test]
fn a_merge_id_from_before_a_restart_never_names_a_merge_made_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 10), (2, 20)]);

    let (mut a, _) = db.agent("before");
    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut a);
    let first = db.merge(&mut a);
    assert!(first.applied_to_target, "the pre-restart merge did not land: {first}");

    // A session holds the runtime; it goes first, or the old runtime outlives the "restart".
    drop(a);
    let mut db = db.restart();

    let (mut b, b_txn) = db.agent("after");
    db.ok("UPDATE inventory SET qty = 22 WHERE id = 2;", &mut b);
    let second = db.merge(&mut b);
    assert!(second.applied_to_target, "the post-restart merge did not land: {second}");
    assert_eq!(db.qty_of(2), 22);
    assert_ne!(
        second.merge_id, first.merge_id,
        "the post-restart merge was issued the id an earlier run already handed out"
    );

    let mut main = db.session();
    match db.exec(&format!("REVERT MERGE {};", first.merge_id), &mut main) {
        // Step 0: refused, and refused for the reason that is TRUE.
        Err(e) => {
            let msg = e.to_string();
            assert!(msg.contains("earlier server run"), "refused for the wrong reason: {msg}");
        }
        // Option (a): the pre-restart merge is revertible, so this is ITS revert.
        Ok(out) => {
            let p = plan(out);
            assert_ne!(p.target, b_txn, "REVERT of the first id planned the post-restart merge's txn");
        }
    }
    assert_eq!(db.qty_of(2), 22, "REVERT of the first id reverted the merge made after the restart");
}

/// **An id an earlier run issued and never published is refused as an earlier run's**, which is a
/// positive fact read from the database — not "unknown", which is what an absence would say.
///
/// Holds under Step 0 and under option (a) alike: (a) makes PUBLISHED pre-restart merges
/// revertible, and a conflict published nothing.
///
/// RED at the base: the reply is `unknown merge m_2`, which does not name the earlier run.
///
/// Anti-vacuity: an id no run ever issued is still `unknown`, so the refusal is not a blanket
/// "earlier server run" for anything not in memory.
#[test]
fn an_unpublished_id_from_an_earlier_run_is_refused_as_that_runs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 10)]);

    // Two agents assign the same cell. The first merge lands as m_1; the second conflicts under
    // the default REJECT policy, publishes nothing, and is still handed an id.
    let (mut x, _) = db.agent("x");
    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut x);
    let (mut y, _) = db.agent("y");
    db.ok("UPDATE inventory SET qty = 12 WHERE id = 1;", &mut y);
    let landed = db.merge(&mut x);
    assert!(landed.applied_to_target, "{landed}");
    let refused = db.merge(&mut y);
    assert!(!refused.applied_to_target, "the fixture needs a merge that publishes nothing: {refused}");
    assert_eq!(db.qty_of(1), 11);

    drop((x, y));
    let mut db = db.restart();
    let mut main = db.session();

    let msg = match db.exec(&format!("REVERT MERGE {};", refused.merge_id), &mut main) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("REVERT of the conflict's id returned a plan for a merge that published nothing"),
    };
    assert!(msg.contains(&refused.merge_id), "the refusal does not name the id: {msg}");
    assert!(
        msg.contains("earlier server run"),
        "an id an earlier run issued was refused without saying so: {msg}"
    );

    let never = match db.exec("REVERT MERGE m_1000000;", &mut main) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("REVERT MERGE of an id nobody issued returned a plan"),
    };
    assert!(never.contains("unknown merge"), "got: {never}");
    assert!(
        !never.contains("earlier server run"),
        "an id NO run issued was attributed to an earlier run, so the refusal says nothing: {never}"
    );
    assert_eq!(db.qty_of(1), 11, "a refused revert changed a row");
}

// ---- D218 ------------------------------------------------------------------------------------

/// **The ledger's red test: `REVERT MERGE` twice over an `Add` merge moves the value once.**
///
/// RED at the base: the second `REVERT MERGE m_1` returns a plan instead of an error, and applies
/// `Add(-5)` again, leaving `qty` at 15.
#[test]
fn reverting_an_add_merge_twice_moves_the_value_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 20)]);

    let (mut a, _) = db.agent("adder");
    db.ok("UPDATE inventory SET qty = qty + 5 WHERE id = 1;", &mut a);
    let m = db.merge(&mut a);
    assert!(m.applied_to_target, "{m}");
    assert_eq!(db.qty_of(1), 25);

    let mut main = db.session();
    let first = plan(db.ok(&format!("REVERT MERGE {};", m.merge_id), &mut main));
    assert!(!first.is_blocked(), "nothing read the row, so the revert proceeds: {first:?}");
    assert_eq!(db.qty_of(1), 20, "the first revert did not undo the Add");

    let second = db.exec(&format!("REVERT MERGE {};", m.merge_id), &mut main);
    let msg = match second {
        Err(e) => e.to_string(),
        Ok(_) => panic!(
            "a second REVERT of {} was accepted; qty is now {}",
            m.merge_id,
            db.qty_of(1)
        ),
    };
    assert!(msg.contains("already reverted"), "refused for the wrong reason: {msg}");
    assert_eq!(db.qty_of(1), 20, "the refused second revert moved the value");
}

/// **A dependent a cascade already undid is not undone again by its own revert.**
///
/// RED at the base: `REVERT MERGE <B>` returns a plan and applies `Add(-7)` to row 2 a second time,
/// taking it from 5 to -2.
#[test]
fn a_dependent_undone_by_a_cascade_is_not_undone_again_by_its_own_revert() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 20), (2, 5)]);

    let (mut a, _) = db.agent("a");
    db.ok("UPDATE inventory SET qty = qty + 5 WHERE id = 1;", &mut a);
    let ma = db.merge(&mut a);
    assert!(ma.applied_to_target, "{ma}");

    // B reads A's row by key — an exact version — and writes a DIFFERENT row, so the only edge
    // from A to B is the read.
    let (mut b, b_txn) = db.agent("b");
    db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b);
    db.ok("UPDATE inventory SET qty = qty + 7 WHERE id = 2;", &mut b);
    let mb = db.merge(&mut b);
    assert!(mb.applied_to_target, "{mb}");
    assert_eq!((db.qty_of(1), db.qty_of(2)), (25, 12));

    let mut main = db.session();
    let cascaded = plan(db.ok(&format!("REVERT MERGE {} CASCADE;", ma.merge_id), &mut main));
    assert_eq!(cascaded.cascade, vec![b_txn], "the fixture needs B undone by the cascade");
    assert_eq!((db.qty_of(1), db.qty_of(2)), (20, 5));

    let msg = match db.exec(&format!("REVERT MERGE {};", mb.merge_id), &mut main) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("B's own revert was accepted after the cascade undid it"),
    };
    assert!(msg.contains("already reverted"), "refused for the wrong reason: {msg}");
    assert!(msg.contains(&ma.merge_id), "the refusal does not name the revert that undid it: {msg}");
    assert_eq!(db.qty_of(2), 5, "B's writes were undone twice");
}

/// **A cascade that fails part-way changes nothing.**
///
/// The cascade undoes B first and then A, and A's row has been deleted by plain SQL, so undoing A
/// fails with "row 1 is gone". RED at the base: B's inverse had already committed in a transaction
/// of its own, so row 2 is 5 after a revert that reported failure.
///
/// Anti-vacuity: the failed revert must not have MARKED B either — B's own revert afterwards
/// proceeds and undoes it exactly once.
#[test]
fn a_cascade_that_fails_part_way_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 20), (2, 5)]);

    let (mut a, _) = db.agent("a");
    db.ok("UPDATE inventory SET qty = qty + 5 WHERE id = 1;", &mut a);
    let ma = db.merge(&mut a);
    let (mut b, b_txn) = db.agent("b");
    db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b);
    db.ok("UPDATE inventory SET qty = qty + 7 WHERE id = 2;", &mut b);
    let mb = db.merge(&mut b);
    assert!(ma.applied_to_target && mb.applied_to_target, "{ma} / {mb}");

    // A later, unattributed statement removes the row A's merge wrote.
    let mut plain = db.session();
    db.ok("DELETE FROM inventory WHERE id = 1;", &mut plain);

    let mut main = db.session();
    let msg = match db.exec(&format!("REVERT MERGE {} CASCADE;", ma.merge_id), &mut main) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("the cascade reported success although A's row is gone"),
    };
    assert!(msg.contains("gone"), "the cascade failed for a reason the fixture did not set up: {msg}");
    assert_eq!(db.qty_of(2), 12, "the failed cascade left B's inverse committed");

    let later = plan(db.ok(&format!("REVERT MERGE {};", mb.merge_id), &mut main));
    assert!(!later.is_blocked(), "{later:?}");
    assert_eq!(later.target, b_txn);
    assert_eq!(db.qty_of(2), 5, "B's own revert did not undo it exactly once");
}

/// **A cascade marks only the txns whose ops it inverted.** A live session that read the target is
/// in the cascade — it is a dependent — but it has published nothing, so nothing of it is undone.
/// Marking it "reverted" anyway would refuse the revert of whatever it publishes LATER, while those
/// writes stayed live.
///
/// Found by the fresh-context review of `3fd20eb`, which marked every txn in the cascade. GREEN at
/// the base (nothing is marked there), RED at `3fd20eb`: `REVERT MERGE <L>` is refused as "already
/// reverted" and row 2 stays 12.
#[test]
fn a_live_dependent_the_cascade_could_not_undo_stays_revertible_once_it_publishes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 20), (2, 5)]);

    let (mut a, _) = db.agent("a");
    db.ok("UPDATE inventory SET qty = qty + 5 WHERE id = 1;", &mut a);
    let ma = db.merge(&mut a);
    assert!(ma.applied_to_target, "{ma}");

    // L reads A's row and stages a write, and is still open when A is reverted.
    let (mut l, l_txn) = db.agent("live");
    db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut l);
    db.ok("UPDATE inventory SET qty = qty + 7 WHERE id = 2;", &mut l);

    let mut main = db.session();
    let cascaded = plan(db.ok(&format!("REVERT MERGE {} CASCADE;", ma.merge_id), &mut main));
    assert_eq!(cascaded.cascade, vec![l_txn], "the fixture needs the live task in the cascade");
    assert_eq!((db.qty_of(1), db.qty_of(2)), (20, 5), "L published nothing, so only A moved");

    let ml = db.merge(&mut l);
    assert!(ml.applied_to_target, "L's merge after the cascade did not land: {ml}");
    assert_eq!(db.qty_of(2), 12);

    let later = plan(db.ok(&format!("REVERT MERGE {};", ml.merge_id), &mut main));
    assert!(!later.is_blocked(), "{later:?}");
    assert_eq!(db.qty_of(2), 5, "L's published write was not reverted");
}

/// **A failed cascade must leave the operator a revert that still works** — the schedule the lead's
/// review of `3fd20eb` (`frontier/d212_step0_review.md` F2) built against that commit.
///
/// B deletes row 3, so B's inverse is an INSERT. With the one-transaction REVERT of `3fd20eb`, the
/// cascade inserted row 3, then failed on A's deleted row and aborted — and an aborted INSERT leaves
/// its primary-index entry behind (D202), so every later INSERT of key 3, including B's own revert,
/// failed. RED at `3fd20eb` at `REVERT MERGE <B>`. RED at the base too, earlier: B's inverse
/// committed on its own, so row 3 is back after a revert that reported failure.
///
/// Green from `509c305`, where `plan_undo` refuses A's missing row before anything is written.
#[test]
fn a_failed_cascade_does_not_poison_the_key_its_retry_reinserts() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 20), (2, 5), (3, 30)]);

    let (mut a, _) = db.agent("a");
    db.ok("UPDATE inventory SET qty = qty + 5 WHERE id = 1;", &mut a);
    let ma = db.merge(&mut a);
    let (mut b, _) = db.agent("b");
    db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b);
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut b);
    let mb = db.merge(&mut b);
    assert!(ma.applied_to_target && mb.applied_to_target, "{ma} / {mb}");
    assert!(!db.has_row(3), "the fixture needs B's delete published");

    let mut plain = db.session();
    db.ok("DELETE FROM inventory WHERE id = 1;", &mut plain);

    let mut main = db.session();
    let msg = match db.exec(&format!("REVERT MERGE {} CASCADE;", ma.merge_id), &mut main) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("the cascade reported success although A's row is gone"),
    };
    assert!(msg.contains("gone"), "the cascade failed for a reason the fixture did not set up: {msg}");
    assert!(!db.has_row(3), "the failed cascade put row 3 back");

    let later = plan(db.ok(&format!("REVERT MERGE {};", mb.merge_id), &mut main));
    assert!(!later.is_blocked(), "{later:?}");
    assert_eq!(db.qty_of(3), 30, "B's revert did not bring row 3 back");
}
