//! Causal edges out of a SCAN, end to end through the agent SQL surface.
//!
//! Design authority: DESIGN.md section 2 and exit criterion 10.
//!
//! `REVERT ... CASCADE` found dependents through exact version identity, which a point or index
//! lookup retains and a scan does not. The query surface has no `LIMIT` and no `ORDER BY`, so an
//! agent's natural read IS a full scan: cascade under-reported precisely where agents read, and it
//! under-reported by finding *zero* dependents, not by finding fewer.
//!
//! Every test here states the input shape that fails without the fix, and every one carries its own
//! anti-vacuity half — a case in the same run that must NOT produce an edge — because a dependency
//! finder that names everything is as useless as one that names nothing.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::MergeReport;
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
            .open(dir.path().join("scan.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("scan.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    /// A connection sharing this database's agent runtime, so branches are mutually visible.
    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }

    fn ok(&mut self, sql: &str, session: &mut Session) -> Outcome {
        match self.exec(sql, session) {
            Ok(o) => o,
            Err(e) => panic!("{} failed: {}", sql, e),
        }
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 5);", &mut s);
    }

    /// Every row of the table, read outside any agent session (so nothing is retained).
    fn all(&mut self) -> Vec<Vec<Value>> {
        let mut s = self.session();
        rows(self.ok("SELECT id, qty FROM inventory;", &mut s))
    }

    fn has_row(&mut self, id: i32) -> bool {
        self.all().iter().any(|r| r[0] == Value::Integer(id))
    }

    fn qty_of(&mut self, id: i32) -> i32 {
        let row = self
            .all()
            .into_iter()
            .find(|r| r[0] == Value::Integer(id))
            .unwrap_or_else(|| panic!("row {} missing", id));
        match row[1] {
            Value::Integer(i) => i,
            ref other => panic!("qty is not an integer: {:?}", other),
        }
    }

    /// Open an agent session and run one scan on it, leaving the session open. The scan is the
    /// read whose retained region is under test. `expect_rows` is how many rows the region really
    /// holds at this point in the fixture.
    ///
    /// **The count is exact, and it used to be `seen.len() <= 3`.** That bound could never bind: no
    /// fixture in this file reaches three rows through here, and — the part that mattered — it is
    /// satisfied by `seen.len() == 0`, which is the clean negative result it was commented as
    /// preventing. A failed statement already panics inside `Db::ok`, so it added nothing at all.
    ///
    /// What the exact count DOES prove: the scan saw the rows the fixture believes are in range, so
    /// a test asserting "no edge" cannot be passing because the read quietly matched nothing. What
    /// it does NOT prove, stated because a zero here reads like a hole: for an expected-zero scan
    /// (the phantom case, where an absence is the whole observation) the count cannot distinguish an
    /// empty region from a read that returned nothing for another reason. That is why the claim that
    /// retention HAPPENED is made on the graph and on the blind-write metric, never on this number.
    fn scanning_session(
        &mut self,
        agent: &str,
        run_id: &str,
        where_clause: &str,
        expect_rows: usize,
    ) -> Session {
        let mut s = self.session();
        self.ok(&format!("BEGIN AGENT SESSION AS '{}' RUN '{}';", agent, run_id), &mut s);
        let sql = format!("SELECT id, qty FROM inventory{};", where_clause);
        let seen = rows(self.ok(&sql, &mut s));
        assert_eq!(
            seen.len(),
            expect_rows,
            "the scan did not see the rows this fixture puts in range: {} returned {:?}",
            sql,
            seen
        );
        s
    }
}

fn rows(out: Outcome) -> Vec<Vec<Value>> {
    match out {
        Outcome::Rows(r) => r,
        _ => panic!("expected rows"),
    }
}

fn agent(out: Outcome) -> AgentOutput {
    match out {
        Outcome::Agent(a) => a,
        Outcome::Rows(_) => panic!("expected an agent output, got rows"),
        _ => panic!("expected an agent output"),
    }
}

fn report(out: Outcome) -> MergeReport {
    match agent(out) {
        AgentOutput::Merge(m) => m,
        other => panic!("expected a merge report, got {}", other),
    }
}

fn plan(out: Outcome) -> RevertPlan {
    match agent(out) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    }
}

/// One agent task that inserts a row and merges it, returning its merge id.
fn insert_and_merge(db: &mut Db, agent_id: &str, run_id: &str, id: i32, qty: i32) -> String {
    let mut s = db.session();
    db.ok(&format!("BEGIN AGENT SESSION AS '{}' RUN '{}';", agent_id, run_id), &mut s);
    db.ok(&format!("INSERT INTO inventory VALUES ({}, {});", id, qty), &mut s);
    let r = report(db.ok("MERGE;", &mut s));
    assert!(r.applied_to_target, "{} failed to merge: {}", agent_id, r);
    r.merge_id
}

// ---- the phantom edge --------------------------------------------------------------------------

/// **BREAKING SHAPE: the dependent's read is a SCAN over a range, and the write it consumed is an
/// INSERT INTO that range.** No version identity exists for a phantom — the row was not there when
/// anything could have named it — so the exact-version path finds nothing, and before this lane the
/// runtime had no other path: cascade reported zero dependents and undid the insert under an agent
/// whose whole decision came from the scanned range.
#[test]
fn a_dependent_reached_by_a_scan_is_named_by_cascade() {
    let mut db = Db::new();
    db.seed();

    // (a) restock-agent inserts a row INSIDE the range the reporter is about to scan, and merges.
    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);
    assert!(db.has_row(7));

    // (b) reporting-agent scans that range — a full scan with a residual, which is what the surface
    //     gives an agent — and then writes on the strength of what it saw.
    let mut b = db.scanning_session("reporting-agent", "r_report", " WHERE qty >= 20 AND qty < 50", 2);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut b);
    let second = report(db.ok("MERGE;", &mut b));
    assert!(second.applied_to_target, "{}", second);
    assert_eq!(db.qty_of(2), 6);

    // (c) HALT is the default, and the scanning task is named.
    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(
        halted.is_blocked(),
        "the scan consumed the inserted row; the revert must not proceed silently"
    );
    assert_eq!(halted.blocked_by, vec![TxnId(2)], "the reporting task is txn 2");
    assert!(db.has_row(7), "a halted revert changes nothing");
    assert_eq!(db.qty_of(2), 6);

    // (d) CASCADE, on explicit request only: the dependent is undone first, then the target.
    let cascaded = plan(db.ok(&format!("REVERT MERGE {} CASCADE;", m1), &mut main));
    assert!(!cascaded.is_blocked());
    assert_eq!(cascaded.cascade, vec![TxnId(2)]);
    assert!(!db.has_row(7), "the reverted insert is gone");
    assert_eq!(db.qty_of(2), 5, "the dependent's write is undone too");
}

/// **BREAKING SHAPE: two branches scan the SAME region, one before the write and one after.**
/// Coverage alone — "did what you wrote fall inside the region I scanned" — makes both of them
/// dependents, and the one that scanned first cannot be: the row did not exist when it looked. This
/// is the anti-vacuity half of the test above, run against the same predicate so that the only
/// difference between the two branches is *when* they read.
///
/// **This test used to be unable to fail from lost retention, which is the one failure it exists to
/// rule out.** Its only positive claim was that the late branch appears, and the early branch being
/// ABSENT is satisfied equally by "the temporal rule excluded it" and by "its read was never
/// retained at all". Measured: a planted one-liner at the top of `TxnCapture::on_read`,
/// `if observed_at == 1 { return; }`, deletes retention for exactly any read taken before anything
/// has been published — precisely this early branch — and all five tests in this file stayed green.
/// That is a plausible regression shape rather than a contrived one: an `apply_seq == 0` early-out
/// or a sentinel guard produces it.
///
/// So the early branch's retention is now asserted DIRECTLY, and it has to be asserted on something
/// that works at `observed_at == 1`: there is by definition no published write for the early branch
/// to depend on, so no dependency edge can carry the claim. The blind-write metric can. The early
/// branch's scan looked at the whole `inventory` table, so a row it then writes without reading is
/// NOT reported blind — and with its retention dropped it is. That row's qty is put OUTSIDE the
/// scanned range so it cannot itself become the dependency the rest of the test is about.
#[test]
fn a_branch_that_scanned_before_the_write_is_not_a_dependent() {
    let mut db = Db::new();
    db.seed();

    // (a) early-agent scans first, at a snapshot where row 7 does not exist. txn 1.
    let mut early =
        db.scanning_session("early-agent", "r_early", " WHERE qty >= 20 AND qty < 50", 1);

    // (a2) THE RETENTION CLAIM, without which the absence in (d) below is indistinguishable from a
    //      read that retained nothing.
    db.ok("INSERT INTO inventory VALUES (9, 500);", &mut early);
    let early_report = report(db.ok("MERGE;", &mut early));
    assert!(early_report.applied_to_target, "{}", early_report);
    assert!(
        early_report.blind_writes.is_empty(),
        "the early branch scanned the whole inventory table, so the row it wrote without reading \
         must not be reported blind. This is the assertion that fails when the early branch's \
         retention is silently dropped: {:?}",
        early_report.blind_writes
    );

    // (b) restock-agent inserts into that range and merges. txn 2.
    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);

    // (c) late-agent scans the same range afterwards, and DOES see the row. txn 3.
    let _late = db.scanning_session("late-agent", "r_late", " WHERE qty >= 20 AND qty < 50", 2);

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    // (d) The forced-to-fire half: the write is inside both regions, so if timestamps were ignored
    //     both tasks would appear here.
    assert!(halted.is_blocked(), "late-agent read the inserted row and must be named");
    assert_eq!(
        halted.blocked_by,
        vec![TxnId(3)],
        "only the task that scanned AFTER the write is a dependent, got {:?}",
        halted.blocked_by
    );
}

/// **BREAKING SHAPE: a write inside the scanned table but outside the scanned RANGE.** With the
/// region left unbounded over the table — which is what a scan retained before the bounds were
/// derived from the clause — every write to any row of that table becomes a dependency of every
/// scan of it, and a cascade that names everything gets ignored, which is the same as naming
/// nothing. Both halves are in this one test: same scan, one write in range and one out.
#[test]
fn a_write_outside_the_scanned_range_is_not_a_dependency() {
    let mut db = Db::new();
    db.seed();

    // Two merges: row 8 lands well above the range, row 9 lands inside it.
    let m_out = insert_and_merge(&mut db, "bulk-agent", "r_bulk", 8, 500);
    let m_in = insert_and_merge(&mut db, "restock-agent", "r_restock", 9, 30);

    // One scanner, reading after BOTH merges, so timestamps cannot be what separates them.
    let _reader =
        db.scanning_session("reporting-agent", "r_report", " WHERE qty >= 20 AND qty < 50", 2);

    let mut main = db.session();
    // Out of range: nothing depends on it, and the revert proceeds and really happens.
    let free = plan(db.ok(&format!("REVERT MERGE {};", m_out), &mut main));
    assert!(!free.is_blocked(), "qty 500 is outside [20, 50): {:?}", free.blocked_by);
    assert!(!db.has_row(8), "an unblocked revert actually reverts");

    // In range: the same scan, the same reader, and now it is a dependent.
    let blocked = plan(db.ok(&format!("REVERT MERGE {};", m_in), &mut main));
    assert!(blocked.is_blocked(), "qty 30 is inside [20, 50) and the scan saw it");
    assert_eq!(blocked.blocked_by, vec![TxnId(3)], "the reporting task is txn 3");
    assert!(db.has_row(9), "a halted revert changes nothing");
}

/// **BREAKING SHAPE: one merge that publishes MORE THAN ONE row.** Everything on the write path here
/// is per-row and per-column — the published pre/post images, the valued write recorded for each
/// column, and the version sequence `merge` reserves before publishing — and a workload with one row
/// per commit exercises none of that arithmetic. Two data-loss bugs in this repository survived
/// precisely that gap, because the generator only ever produced one row per commit. This test also
/// pins the de-duplication: three published rows inside one merge must name the reading task ONCE,
/// not three times, and the cascade must undo all three.
#[test]
fn a_multi_row_merge_is_named_once_by_the_scan_that_read_it_and_reverts_whole() {
    let mut db = Db::new();
    db.seed();

    // One agent task, three published rows: two inserts inside the range the reporter will scan, and
    // one update well outside it.
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'restock-agent' RUN 'r_restock';", &mut a);
    db.ok("INSERT INTO inventory VALUES (7, 30);", &mut a);
    db.ok("INSERT INTO inventory VALUES (8, 45);", &mut a);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut a);
    let m1 = report(db.ok("MERGE;", &mut a));
    assert!(m1.applied_to_target, "{}", m1);
    assert_eq!(m1.rows.len(), 3, "the merge must publish three rows: {}", m1);
    assert_eq!(db.qty_of(2), 6);

    // The reporter scans the range afterwards and sees rows 1, 7 and 8 — not row 2.
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'reporting-agent' RUN 'r_report';", &mut b);
    let seen = rows(db.ok("SELECT id, qty FROM inventory WHERE qty >= 20 AND qty < 50;", &mut b));
    assert_eq!(seen.len(), 3, "expected rows 1, 7 and 8 in [20, 50): {:?}", seen);

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1.merge_id), &mut main));
    assert!(halted.is_blocked());
    assert_eq!(
        halted.blocked_by,
        vec![TxnId(2)],
        "three published rows, one reader: named once, got {:?}",
        halted.blocked_by
    );

    let cascaded = plan(db.ok(&format!("REVERT MERGE {} CASCADE;", m1.merge_id), &mut main));
    assert_eq!(cascaded.cascade, vec![TxnId(2)]);
    assert!(!db.has_row(7), "every row the merge published is undone");
    assert!(!db.has_row(8), "every row the merge published is undone");
    assert_eq!(db.qty_of(2), 5, "including the one outside the scanned range");
    assert_eq!(db.qty_of(1), 20, "and nothing else moved");
}

/// **BREAKING SHAPE: the write moved a value OUT of the scanned range, so its post-image is outside
/// the region and its pre-image is inside.** The scan that ran afterwards observed an *absence* that
/// this write caused; reverting the write puts the row back inside its range. A check that only
/// looks at what a write produced finds no edge here — which is the phantom case with the sign
/// flipped, and phantom coverage is the entire reason a scan retains a region rather than a row set.
#[test]
fn a_scan_depends_on_the_write_that_moved_a_row_out_of_its_range() {
    let mut db = Db::new();
    db.seed();

    // pricing-agent moves row 1 from 20 (inside [20, 50)) to 500 (outside it) and merges.
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_price';", &mut a);
    db.ok("UPDATE inventory SET qty = 500 WHERE id = 1;", &mut a);
    let m1 = report(db.ok("MERGE;", &mut a)).merge_id;
    assert_eq!(db.qty_of(1), 500);

    // The reporter scans that range afterwards and finds it empty — an observation it made only
    // because of the write above.
    let _reader =
        db.scanning_session("reporting-agent", "r_report", " WHERE qty >= 20 AND qty < 50", 0);
    // The anti-vacuity half: a scan of a region that NEITHER image touches must not be named.
    let _elsewhere =
        db.scanning_session("audit-agent", "r_audit", " WHERE qty >= 1000 AND qty < 2000", 0);

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(
        halted.is_blocked(),
        "reverting puts qty 20 back inside the scanned range, so the reporter's empty answer \
         depended on this write"
    );
    assert_eq!(
        halted.blocked_by,
        vec![TxnId(2)],
        "only the reporter scanned a region either image falls in, got {:?}",
        halted.blocked_by
    );
}
