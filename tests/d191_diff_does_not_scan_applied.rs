//! D191 / branch-count wall #17: `DIFF` must not pay for every merge anyone has ever published.
//!
//! `AgentRuntime::diff` answered each changed row's `concurrent` flag with
//! `state.applied.iter().any(|a| a.seq > ws.fork_seq && a.tbl == t && a.row == r)`, once per changed
//! row, over a log that is global across branches and never pruned. That is O(delta × |applied|),
//! and |applied| grows with every merge of every branch. D86's `applied_by_cell` index could not
//! serve it: it is keyed `(tbl, row, col)` and skips whole-row ops entirely.
//!
//! # Why this file holds exactly ONE test, and must keep holding exactly one
//!
//! `DIFF_APPLIED_VISITED` and `DIFF_ROWS` (D191's instrument) are **process-global atomics**,
//! scoped to one `DIFF;` by reading them twice and subtracting. `cargo test` runs the tests of one
//! binary on a thread pool, so a second test here would run its own `DIFF` inside this one's window.
//! The reasoning is the same as `tests/d178_dml_index_counters.rs`. ⛔ **Do not add a second `#[test]`
//! here.**
//!
//! # What is asserted, and what each arm is for
//!
//! Every expected value comes from the FIXTURE, never from the subject. It is the number of changed
//! rows that have any published history at all, which the fixture decides by construction:
//!
//! | arm | session | changed rows | published history on them | concurrent | visited |
//! |---|---|---|---|---|---|
//! | A | forked before every merge | ids 1..4 | none | none | **0** |
//! | C | forked before every merge | ids 1,2,3,19,20 | 19: a cell UPDATE after C's fork; 20: a whole-row DELETE after C's fork | **19, 20** | **2** (the FIRE-CHECK) |
//! | P | forked immediately after the merge that published 19 | ids 1,2,3,19 | 19: one op whose `seq == P.fork_seq` | none | **1** |
//!
//! * **C is the fire-check.** A zero from A means nothing unless C, in the same process and the same
//!   run, reads non-zero. It is also the only arm with a WHOLE-ROW op (the DELETE of 20). A fix built
//!   on D86's cell index cannot see that op, so C's outcome for 20 is what kills that fix.
//! * **P is the boundary.** The one op on 19 has `seq == fork_seq` exactly, so it predates the fork
//!   and is NOT concurrent. An off-by-one (`>=`) turns P's 19 into `PendingConcurrent`. The entry is
//!   still examined, which is why P reads 1 and not 0.
//! * **Two checkpoints.** Between them, 40 more merges publish 160 more entries. Every integer must
//!   be identical at both. That is the slope, not one point: a scan is linear in the axis, and the
//!   high-water lookup is flat on it.
//!
//! # Pre-registered, before any build of this file
//!
//! * **Before the fix** (the commit that adds this file, on top of D191's instrument), the outcome
//!   assertions pass and the FIRST COUNT assertion fails. That is C's fire-check at checkpoint 1,
//!   with `visited = 3·|applied₁| + (p19 + 1) + (p20 + 1)`. Here `p19` and `p20` are the log positions
//!   of the two ops, read from merge reports, and the failure message prints both numbers. At the
//!   inferred one op per single-column UPDATE, that is 3·82 + 41 + 42 = **329**. The inference is
//!   unverified; the formula stands either way.
//! * **After the fix**, every assertion passes with the integers in the table above.
//!
//! # Blind spots, stated here rather than discovered later
//!
//! * A high-water map that OVERWROTE instead of taking the max is invisible through SQL, because
//!   publishes are serialised and seqs arrive in increasing order. The unit test
//!   `the_row_high_water_is_the_max_over_every_push_on_that_row` in `agent_sql::runtime` plants an
//!   out-of-order push to cover it.
//! * One table only. A key that dropped `tbl` is covered by that same unit test, not here.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{diff_scan_counters, AgentRuntime};
use ferrodb::agent_sql::{ChangeOutcome, ChangeSet, MergeReport};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::ids::RowId;
use ferrodb::tel::op::OpKind;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const NROWS: i32 = 80;
/// Noise merges draw their rows from `POOL_LO..=NROWS`, so they never touch ids 1..20.
const POOL_LO: i32 = 21;
/// Rows each noise branch updates before merging.
const NOISE_ROWS: usize = 4;
/// C updates it; merge X publishes one CELL op on it, after C's fork.
const HIT_CELL: i32 = 19;
/// C updates it; merge Y publishes one WHOLE-ROW delete of it, after C's fork.
const HIT_ROW: i32 = 20;
const NOISE_BEFORE_X: usize = 10;
const NOISE_AFTER_Y: usize = 10;
/// Merges between the two checkpoints: |applied| grows by 4 × this and nothing may move.
const NOISE_BETWEEN: usize = 40;

/// The `tests/agent_sql_surface.rs` database. `AgentRuntime::new()` is map-backed, and that is
/// enough here: `diff`, `record_applied` and `push_applied` do not branch on storage.
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
            .open(dir.path().join("d191.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d191.wal")).unwrap());
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
        assert!(parser.errors.is_empty(), "parse failed for {sql}: {:?}", parser.errors);
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
            .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    /// Open an agent session and stage one single-column UPDATE per id.
    fn open(&mut self, name: &str, ids: &[i32], v: i32) -> Session {
        let mut s = self.session();
        self.ok(&format!("BEGIN AGENT SESSION AS '{name}';"), &mut s);
        for id in ids {
            self.ok(&format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut s);
        }
        s
    }

    fn merge(&mut self, s: &mut Session) -> MergeReport {
        let rep = match self.ok("MERGE;", s) {
            Outcome::Agent(AgentOutput::Merge(r)) => r,
            _ => panic!("MERGE did not return a merge report"),
        };
        assert!(rep.applied_to_target, "a fixture merge did not publish, so |applied| is wrong: {rep}");
        rep
    }

    fn diff(&mut self, s: &mut Session) -> Seen {
        let (v0, r0, _) = diff_scan_counters();
        let cs = match self.ok("DIFF;", s) {
            Outcome::Agent(AgentOutput::Diff(cs)) => cs,
            _ => panic!("DIFF did not return a changeset, so the window is not a DIFF"),
        };
        let (v1, r1, _) = diff_scan_counters();
        Seen::of(cs, v1 - v0, r1 - r0)
    }
}

/// One `DIFF;`: the two counters scoped to it, and what it reported.
struct Seen {
    visited: u64,
    rows: u64,
    /// `(id, RowId, outcome)` per reported row, sorted by id.
    reported: Vec<(i32, RowId, ChangeOutcome)>,
}

impl Seen {
    fn of(cs: ChangeSet, visited: u64, rows: u64) -> Seen {
        let mut reported: Vec<(i32, RowId, ChangeOutcome)> = cs
            .rows
            .iter()
            .map(|r| {
                let id = match r.after.as_ref().and_then(|v| v.first()) {
                    Some(Value::Integer(i)) => *i,
                    other => panic!("every fixture change is an UPDATE with an integer key, got {other:?}"),
                };
                (id, r.row, r.outcome.clone())
            })
            .collect();
        reported.sort_by_key(|(id, _, _)| *id);
        Seen { visited, rows, reported }
    }

    fn ids(&self) -> Vec<i32> {
        self.reported.iter().map(|(id, _, _)| *id).collect()
    }

    fn concurrent(&self) -> Vec<i32> {
        self.reported
            .iter()
            .filter(|(_, _, o)| *o == ChangeOutcome::PendingConcurrent)
            .map(|(id, _, _)| *id)
            .collect()
    }

    fn row_of(&self, id: i32) -> RowId {
        self.reported
            .iter()
            .find(|(i, _, _)| *i == id)
            .map(|(_, r, _)| *r)
            .unwrap_or_else(|| panic!("id {id} is not in this DIFF, so the fixture did not stage it"))
    }
}

fn applied_len(rep: &MergeReport) -> u64 {
    rep.rows.iter().map(|r| r.applied.len() as u64).sum()
}

/// Publish one noise branch: `NOISE_ROWS` pool rows, each set to a value no earlier write used.
fn noise(db: &mut Db, cursor: &mut usize, m: &mut i32) -> u64 {
    let pool = (NROWS - POOL_LO + 1) as usize;
    let ids: Vec<i32> = (0..NOISE_ROWS)
        .map(|_| {
            let id = POOL_LO + (*cursor % pool) as i32;
            *cursor += 1;
            id
        })
        .collect();
    *m += 1;
    // Negative and distinct per merge; the seed values are positive. So every UPDATE moves its
    // cell and publishes an op; an unchanged cell would publish nothing and shrink |applied|.
    let mut s = db.open(&format!("n{m}"), &ids, -*m);
    applied_len(&db.merge(&mut s))
}

#[test]
fn diff_reads_one_high_water_entry_per_changed_row_however_long_the_applied_log_grows() {
    let mut db = Db::new();
    let mut seed = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut seed);
    for i in 1..=NROWS {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut seed);
    }

    // ---- the fixture: every published entry is counted from MERGE REPORTS -----------------------
    let mut a = db.open("A", &[1, 2, 3, 4], 1);
    let mut c = db.open("C", &[1, 2, 3, HIT_CELL, HIT_ROW], 1);
    // C's RowIds, from C's own DIFF, so no assumption about how a key becomes a RowId.
    let c0 = db.diff(&mut c);
    let (row_cell, row_row) = (c0.row_of(HIT_CELL), c0.row_of(HIT_ROW));

    let (mut cursor, mut m, mut applied) = (0usize, 0i32, 0u64);
    for _ in 0..NOISE_BEFORE_X {
        applied += noise(&mut db, &mut cursor, &mut m);
    }

    // X: exactly one CELL op, on C's id 19.
    let p_cell = applied;
    let mut x = db.open("X", &[HIT_CELL], -1000);
    let rx = db.merge(&mut x);
    let x_ops: Vec<_> = rx.rows.iter().flat_map(|r| r.applied.iter()).collect();
    assert_eq!(x_ops.len(), 1, "merge X must publish exactly one op: {rx}");
    assert!(x_ops[0].col.is_some(), "merge X's op must be a CELL op: {rx}");
    assert_eq!(x_ops[0].row, row_cell, "merge X's op is not on C's id-{HIT_CELL} row");
    applied += 1;

    // P forks NOW, so its `fork_seq` equals the seq of X's one op: the exact boundary.
    let mut p = db.open("P", &[1, 2, 3, HIT_CELL], 2);

    // Y: exactly one WHOLE-ROW op, the delete of C's id 20. D86's cell index never sees this op.
    let p_row = applied;
    let mut y = db.session();
    db.ok("BEGIN AGENT SESSION AS 'Y';", &mut y);
    db.ok(&format!("DELETE FROM t WHERE id = {HIT_ROW};"), &mut y);
    let ry = db.merge(&mut y);
    let y_ops: Vec<_> = ry.rows.iter().flat_map(|r| r.applied.iter()).collect();
    assert_eq!(y_ops.len(), 1, "merge Y must publish exactly one op: {ry}");
    assert!(
        y_ops[0].col.is_none() && matches!(y_ops[0].kind, OpKind::RowDelete),
        "merge Y's op must be a whole-row RowDelete: {ry}"
    );
    assert_eq!(y_ops[0].row, row_row, "merge Y's op is not on C's id-{HIT_ROW} row");
    applied += 1;

    for _ in 0..NOISE_AFTER_Y {
        applied += noise(&mut db, &mut cursor, &mut m);
    }
    let applied_1 = applied;
    assert!(
        applied_1 >= (NOISE_BEFORE_X + NOISE_AFTER_Y) as u64 * NOISE_ROWS as u64 + 2,
        "only {applied_1} entries were published; the axis this test is about barely exists"
    );

    let mut prev: Option<(u64, u64, u64)> = None;
    for checkpoint in 1..=2 {
        if checkpoint == 2 {
            for _ in 0..NOISE_BETWEEN {
                applied += noise(&mut db, &mut cursor, &mut m);
            }
            assert!(applied > applied_1, "the axis did not move between the checkpoints");
        }
        let (sa, sc, sp) = (db.diff(&mut a), db.diff(&mut c), db.diff(&mut p));
        eprintln!(
            "d191 checkpoint {checkpoint}: |applied|={applied} | A visited={} rows={} | \
             C visited={} rows={} concurrent={:?} | P visited={} rows={}",
            sa.visited, sa.rows, sc.visited, sc.rows, sc.concurrent(), sp.visited, sp.rows
        );

        // ---- 1. What DIFF reports. True before the fix and after it: this is the behaviour the
        //         fix must preserve, so it is asserted before any count.
        assert_eq!(sa.ids(), vec![1, 2, 3, 4]);
        assert_eq!(sc.ids(), vec![1, 2, 3, HIT_CELL, HIT_ROW]);
        assert_eq!(sp.ids(), vec![1, 2, 3, HIT_CELL]);
        assert_eq!(sa.concurrent(), Vec::<i32>::new(), "A: nothing touched ids 1..4");
        assert_eq!(
            sc.concurrent(),
            vec![HIT_CELL, HIT_ROW],
            "C: a cell op AND a whole-row delete were published on its rows after it forked"
        );
        assert_eq!(
            sp.concurrent(),
            Vec::<i32>::new(),
            "P: the op on id {HIT_CELL} has seq == P's fork_seq, so it PREDATES the fork"
        );

        // ---- 2. The loop ran once per changed row, so a small `visited` is not an idle DIFF.
        assert_eq!((sa.rows, sc.rows, sp.rows), (4, 5, 4), "DIFF_ROWS is not the pinned delta");

        // ---- 3. The FIRE-CHECK: two of C's rows have published history, so C must read 2. Placed
        //         first among the counts so a zero below can mean something.
        let retired_scan_c = 3 * applied + (p_cell + 1) + (p_row + 1);
        assert_eq!(
            sc.visited, 2,
            "C visited {} entries at |applied|={applied}; the retired per-row scan of \
             State::applied reads exactly {retired_scan_c} here (3·|applied| + (p19+1) + (p20+1), \
             p19={p_cell}, p20={p_row}). Expected 2: one high-water entry for each of ids \
             {HIT_CELL} and {HIT_ROW}, the only changed rows with published history",
            sc.visited
        );
        assert_eq!(
            sp.visited, 1,
            "P visited {} entries at |applied|={applied}; expected 1, the id-{HIT_CELL} entry, \
             examined and rejected at the boundary (the retired scan reads 4·|applied| = {})",
            sp.visited,
            4 * applied
        );
        assert_eq!(
            sa.visited, 0,
            "A visited {} entries at |applied|={applied}; none of its rows was ever published, so \
             it has nothing to examine (the retired scan reads 4·|applied| = {})",
            sa.visited,
            4 * applied
        );

        // ---- 4. The slope: 4·NOISE_BETWEEN more entries, and not one integer moved.
        let now = (sa.visited, sc.visited, sp.visited);
        if let Some(before) = prev {
            assert_eq!(now, before, "a count moved with |applied|: {before:?} -> {now:?}");
        }
        prev = Some(now);
    }
}
