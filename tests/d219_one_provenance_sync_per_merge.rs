//! **D219 — a `MERGE` makes its row authorship durable with ONE fsync, whatever δ is.**
//!
//! With the durable provenance store — the CLI's shape, `cli.rs` `with_durable_provenance` —
//! `AgentRuntime::record_applied` held the runtime's `state` lock across one `stamp_row` per
//! applied op, and `DurableProvenanceStore::stamp_row` appended one frame and `sync_data`ed it per
//! call. A merge of δ ops therefore held the lock every agent statement takes across δ fsyncs.
//!
//! # The instrument
//!
//! `ProvenanceStore::sync_counts().row_authors`, read immediately before and after the `MERGE`
//! statement. It counts the fsyncs that made logical row authorship durable, which are exactly the
//! syncs `record_applied` issues. The executor's physical `(page, slot)` stamps are counted apart,
//! in `stamps`, and are deliberately NOT asserted by the first test. When it was written they were
//! one sync per published version on the publish loop, outside the `state` lock; since `86e1762`
//! they ride the row-authorship sync, so the column its failure message labels "physical stamp
//! syncs" reads 0 there.
//!
//! **The second test asserts them.** D219's exit is ONE provenance sync per MERGE whatever δ is,
//! physical and logical together, so
//! `every_provenance_sync_a_merge_issues_is_one_whatever_delta_is` reads `total()` — every sync the
//! store issued inside the MERGE, whatever it carried — and adds the one physical-stamp path the
//! publish loop does not own: an ALTER's rewrite re-stamping the rows it moves.
//!
//! **The third and fourth tests are the one exception, and why.** Each table a merge ALTERS is a
//! durability point of its own — its rewrite can reach the disk before the publish begins — so the
//! merge syncs once per altered table whose rewrite moved an attributed row, plus once for the
//! publish, whatever δ is:
//! `a_merge_that_alters_a_table_makes_the_rewrites_stamps_durable_with_the_rewrite` (one table, 2)
//! and `each_altered_tables_rewrite_stamps_are_durable_before_the_next_rewrite` (two tables, 3).
//!
//! # Pre-registered, from reading the source at `9aa6968`, before this file was ever run
//!
//! Ops per row come from `evaluate`: an insert is one `RowCreate`; an update is one op per CHANGED
//! cell. `record_applied` stamps once per op, so a row updated in two columns is stamped twice.
//!
//! | arm            | rows | ops | `row_authors` per MERGE, before the fix | after |
//! |----------------|------|-----|-----------------------------------------|-------|
//! | insert δ=1     | 1    | 1   | 1                                       | 1     |
//! | insert δ=4     | 4    | 4   | 4                                       | 1     |
//! | insert δ=16    | 16   | 16  | 16                                      | 1     |
//! | update δ=4 ×2  | 4    | 8   | 8                                       | 1     |
//!
//! The δ=1 arm is non-discriminating ON PURPOSE: "whatever δ" includes 1, and a fix that broke the
//! single-op merge would show here and nowhere else. The update arm is what separates "one sync per
//! op" from "one sync per row".
//!
//! # What the fix must NOT change, checked in the same run
//!
//! * **Authorship is in the FILE before `MERGE` returns.** A second store opened on the same path
//!   after each merge must name this arm's run for every row it published. A fix that deferred the
//!   append past the acknowledgement fails here.
//! * **The file carries the same records.** The reopened file's `RowAuthor` count grows by exactly
//!   the arm's op count: a batch that deduplicated or dropped frames would change what a reopen
//!   replays, and this row promised one sync, not a different file.
//!
//! What this cannot see: whether the sync happens AFTER the write. The counter proves a sync
//! returned and the reopen proves the bytes are in the file; the order between the two is argued in
//! `DurableProvenanceStore`'s append, not tested, because no fault-injecting file sits under it.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{table_id, AgentRuntime};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::{DurableProvenanceStore, ProvenanceStore, SyncCounts};
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
    /// A database whose provenance lives in a file at `prov`, as the CLI's does.
    fn with_provenance(prov: &std::path::Path) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("agent.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let runtime = Arc::new(
            AgentRuntime::new().with_durable_provenance(prov).expect("open durable provenance"),
        );
        Db { catalog, bp, txn, runtime, _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn ok(&mut self, sql: &str, session: &mut Session) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new())
            .scan_tokens()
            .unwrap_or_else(|e| panic!("{sql} did not scan: {e}"));
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "{sql} did not parse: {:?}", parser.errors);
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
            .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn syncs(&self) -> SyncCounts {
        self.runtime.provenance().sync_counts()
    }
}

/// One arm's measurement: what the `MERGE` statement cost in syncs, and what it left in the file.
struct Measured {
    arm: &'static str,
    rows: usize,
    ops: usize,
    during: SyncCounts,
}

/// `after - before`, field by field. Every counter is monotone, so a negative is a broken
/// instrument and is refused rather than wrapped.
fn delta(before: SyncCounts, after: SyncCounts) -> SyncCounts {
    let d = |a: u64, b: u64| b.checked_sub(a).expect("a sync counter went backwards");
    SyncCounts {
        runs: d(before.runs, after.runs),
        stamps: d(before.stamps, after.stamps),
        row_authors: d(before.row_authors, after.row_authors),
        forgets: d(before.forgets, after.forgets),
    }
}

/// Fork a session under run `run_id`, apply `writes` on it, `MERGE`, and measure the MERGE alone.
///
/// Then open a SECOND store on the same file and check it names `run_id` for every row in `ids`,
/// and that its `RowAuthor` record count grew by exactly `ops` since `frames_before`.
#[allow(clippy::too_many_arguments)]
fn merge_arm(
    db: &mut Db,
    prov: &std::path::Path,
    arm: &'static str,
    run_id: &str,
    writes: &[String],
    ids: &[i64],
    ops: usize,
    frames_before: &mut usize,
) -> Measured {
    let mut s = db.session();
    db.ok(
        &format!("BEGIN AGENT SESSION AS 'd219' RUN '{run_id}' MODEL 'claude-opus-5/2026-05';"),
        &mut s,
    );
    for w in writes {
        db.ok(w, &mut s);
    }

    let before = db.syncs();
    let out = db.ok("MERGE;", &mut s);
    let after = db.syncs();
    match out {
        Outcome::Agent(AgentOutput::Merge(report)) => {
            assert!(report.applied_to_target, "{arm}: the merge did not reach the target")
        }
        _ => panic!("{arm}: MERGE did not return a merge report"),
    }
    let during = delta(before, after);
    // Nothing else durable happens inside a MERGE: the run was interned at BEGIN, and nothing here
    // drops a table. A non-zero here means the window measured something other than the merge.
    assert_eq!(during.runs, 0, "{arm}: a run was interned inside the MERGE window");
    assert_eq!(during.forgets, 0, "{arm}: a table was forgotten inside the MERGE window");

    // Authorship is in the file by the time MERGE has returned.
    let reopened = DurableProvenanceStore::open(prov).expect("reopen the provenance file");
    let tbl = table_id("t").0;
    for id in ids {
        let who = reopened.row_author(tbl, *id as u64).unwrap();
        assert!(
            !who.is_none(),
            "{arm}: row {id} has no author in the file after MERGE returned — authorship was \
             acknowledged before it was written"
        );
        assert_eq!(
            reopened.lookup(who).unwrap().run_id,
            run_id,
            "{arm}: row {id}'s author in the file is not the run that just merged it"
        );
    }
    let frames = reopened.recovery().row_authors;
    assert_eq!(
        frames - *frames_before,
        ops,
        "{arm}: the file gained {} RowAuthor records for {ops} applied ops; one sync per merge must \
         not mean a different file",
        frames - *frames_before
    );
    *frames_before = frames;
    assert_eq!(reopened.discarded_tail_bytes(), 0, "{arm}: the file had a torn tail");

    Measured { arm, rows: ids.len(), ops, during }
}

#[test]
fn a_merge_makes_its_row_authorship_durable_with_one_sync_whatever_delta_is() {
    let dir = tempfile::tempdir().unwrap();
    let prov = dir.path().join("d219.provenance");
    let mut db = Db::with_provenance(&prov);

    let mut setup = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, a INTEGER, b INTEGER);", &mut setup);
    // Rows the update arm will change, written OUTSIDE any agent session: a plain write carries no
    // author and syncs nothing, so these start unattributed in the file.
    let seeded: Vec<i64> = (1001..=1004).collect();
    for id in &seeded {
        db.ok(&format!("INSERT INTO t VALUES ({id}, 0, 0);"), &mut setup);
    }
    assert_eq!(db.syncs(), SyncCounts::default(), "the plain setup writes synced provenance");

    let mut frames = 0usize;
    let mut measured = Vec::new();
    for (arm, run_id, first, n) in
        [("insert d=1", "ins-1", 1i64, 1usize), ("insert d=4", "ins-4", 11, 4), ("insert d=16", "ins-16", 101, 16)]
    {
        let ids: Vec<i64> = (first..first + n as i64).collect();
        let writes: Vec<String> =
            ids.iter().map(|id| format!("INSERT INTO t VALUES ({id}, {id}, {id});")).collect();
        measured.push(merge_arm(&mut db, &prov, arm, run_id, &writes, &ids, n, &mut frames));
    }
    let writes: Vec<String> = seeded
        .iter()
        .map(|id| format!("UPDATE t SET a = a + 1, b = b + 1 WHERE id = {id};"))
        .collect();
    measured.push(merge_arm(
        &mut db,
        &prov,
        "update d=4 x2 cols",
        "upd-4",
        &writes,
        &seeded,
        2 * seeded.len(),
        &mut frames,
    ));

    let table: Vec<String> = measured
        .iter()
        .map(|m| {
            format!(
                "{:<20} rows={:<3} ops={:<3} row_author syncs={:<3} (physical stamp syncs={})",
                m.arm, m.rows, m.ops, m.during.row_authors, m.during.stamps
            )
        })
        .collect();
    assert!(
        measured.iter().all(|m| m.during.row_authors == 1),
        "a MERGE must make its row authorship durable with exactly ONE sync, whatever δ is. \
         Measured, per MERGE statement:\n  {}",
        table.join("\n  ")
    );
}

/// **D219's whole exit: EVERY provenance sync a MERGE issues is ONE, whatever δ is** — the
/// publish loop's physical stamps, the stamps an ALTER's rewrite writes for the rows it moves, and
/// the row authorship, together.
///
/// The test above asserts `row_authors` only, and at `80ff247` it passes while every MERGE still
/// pays one sync per published version on the publish loop. This one asserts `total()`.
///
/// # Pre-registered at `80ff247`, from the source, before this test was ever run
///
/// | arm                        | versions published | ops | all syncs per MERGE at `80ff247` | after |
/// |----------------------------|--------------------|-----|----------------------------------|-------|
/// | packed insert d=41         | 41                 | 41  | 42 (41 stamps + 1 authorship)    | 1     |
/// | schema only (ADD COLUMN)   | 0                  | 0   | m, one per moved attributed row  | 1     |
/// | insert d=1                 | 1                  | 1   | 2                                | 1     |
/// | insert d=16                | 16                 | 16  | 17                               | 1     |
/// | update d=4 ×2 cols         | 4                  | 8   | 5                                | 1     |
///
/// **m = 41 is predicted and NOT asserted.** The rewrite stamps only rows that MOVE pages and
/// already carry attribution. 41 rows of `(INTEGER, 60-character VARCHAR)` fill exactly one heap
/// page — the packing `integration_alter_refusal_safety.rs` uses to force relocation — and are
/// attributed because a MERGE published them. `Page::update` grows a tuple only into fresh
/// contiguous space, heap pages are never compacted, and ADD COLUMN widens every row, so every row
/// should relocate. What IS asserted is that at least two `Stamp` records appeared in the file
/// across the schema merge: with fewer, the arm could not tell one sync from one per row, and it
/// says so instead of passing.
#[test]
fn every_provenance_sync_a_merge_issues_is_one_whatever_delta_is() {
    let dir = tempfile::tempdir().unwrap();
    let prov = dir.path().join("d219-all.provenance");
    let mut db = Db::with_provenance(&prov);
    let mut setup = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));", &mut setup);
    let stamp_records =
        || DurableProvenanceStore::open(&prov).expect("reopen the provenance file").recovery().stamps;

    let mut frames = 0usize;
    let mut measured = Vec::new();

    let pad = "y".repeat(60);
    let packed: Vec<i64> = (1..=41).collect();
    let writes: Vec<String> =
        packed.iter().map(|id| format!("INSERT INTO t VALUES ({id}, '{pad}');")).collect();
    measured.push(merge_arm(
        &mut db,
        &prov,
        "packed insert d=41",
        "packed-41",
        &writes,
        &packed,
        41,
        &mut frames,
    ));

    let before_rewrite = stamp_records();
    measured.push(merge_arm(
        &mut db,
        &prov,
        "schema only (rewrite)",
        "alter-1",
        &["ALTER TABLE t ADD COLUMN w VARCHAR(10);".to_string()],
        &[],
        0,
        &mut frames,
    ));
    let rewritten = stamp_records() - before_rewrite;
    assert!(
        rewritten >= 2,
        "the schema merge's rewrite wrote {rewritten} Stamp record(s). The fixture needs at least two \
         moved, attributed rows, or this arm cannot tell one sync from one per moved row"
    );

    let insert_arms = [("insert d=1", "ins-1", 1001i64, 1usize), ("insert d=16", "ins-16", 2001, 16)];
    for (arm, run_id, first, n) in insert_arms {
        let ids: Vec<i64> = (first..first + n as i64).collect();
        let writes: Vec<String> =
            ids.iter().map(|id| format!("INSERT INTO t VALUES ({id}, 'x', 'z');")).collect();
        measured.push(merge_arm(&mut db, &prov, arm, run_id, &writes, &ids, n, &mut frames));
    }
    let updated: Vec<i64> = (1..=4).collect();
    let writes: Vec<String> = updated
        .iter()
        .map(|id| format!("UPDATE t SET v = 'u', w = 'u' WHERE id = {id};"))
        .collect();
    measured.push(merge_arm(
        &mut db,
        &prov,
        "update d=4 x2 cols",
        "upd-4",
        &writes,
        &updated,
        2 * updated.len(),
        &mut frames,
    ));

    let table: Vec<String> = measured
        .iter()
        .map(|m| {
            format!(
                "{:<22} rows={:<3} ops={:<3} all syncs={:<3} (stamps={} row_authors={})",
                m.arm,
                m.rows,
                m.ops,
                m.during.total(),
                m.during.stamps,
                m.during.row_authors
            )
        })
        .collect();
    assert!(
        measured.iter().all(|m| m.during.total() == 1),
        "a MERGE must make ALL of its provenance — physical stamps, rewrite stamps and row \
         authorship — durable with exactly ONE sync, whatever δ is. The schema merge moved \
         {rewritten} attributed row(s). Measured, per MERGE statement:\n  {}",
        table.join("\n  ")
    );
}

/// **A MERGE that ALTERS a table has two durability points, so it syncs the provenance file twice —
/// whatever δ is.**
///
/// The schema phase makes its heap rewrite durable (a flush and a sync of the database file,
/// `publish_evaluation_as`) BEFORE the publish transaction begins, and that rewrite stays durable
/// whether or not the publish ever commits. The stamps it wrote for the rows it moved must be
/// durable no later than the rewrite: left for the merge's final sync, a crash anywhere in the
/// publish reopens with the ALTER applied and every moved row unattributed, for good. So the merge
/// syncs once at the schema phase (`flush`, booked under `stamps`) and once after the commit (row
/// authorship carrying the publish loop's stamps, booked under `row_authors`). Found by the D219
/// whole-exit review (F1); a merge that alters nothing still syncs once.
///
/// # Pre-registered, from the source, before this test was ever run
///
/// | tree | `stamps` | `row_authors` | `total()` |
/// |---|---|---|---|
/// | `bf10eec`, stamps eager | m + 4 | 1 | m + 5 (m = 41 predicted) |
/// | `86e1762`, every stamp deferred to the final sync | 0 | 1 | 1: RED, the rewrite's stamps waited for the publish |
/// | the fix | 1 | 1 | 2 |
///
/// The rows are staged BEFORE the ALTER, in the table's current shape (`branch_update` binds against
/// the shared shape either way); the merge lands the schema first and carries them into the widened
/// shape (`conform_to`). `integration_merge_ddl_atomicity.rs` merges an ALTER and an UPDATE from one
/// branch the same way.
#[test]
fn a_merge_that_alters_a_table_makes_the_rewrites_stamps_durable_with_the_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let prov = dir.path().join("d219-alter.provenance");
    let mut db = Db::with_provenance(&prov);
    let mut setup = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));", &mut setup);
    let stamp_records =
        || DurableProvenanceStore::open(&prov).expect("reopen the provenance file").recovery().stamps;

    let mut frames = 0usize;
    let pad = "y".repeat(60);
    let packed: Vec<i64> = (1..=41).collect();
    let writes: Vec<String> =
        packed.iter().map(|id| format!("INSERT INTO t VALUES ({id}, '{pad}');")).collect();
    merge_arm(&mut db, &prov, "packed insert d=41", "packed-41", &writes, &packed, 41, &mut frames);

    let updated: Vec<i64> = (1..=4).collect();
    let mut writes: Vec<String> =
        updated.iter().map(|id| format!("UPDATE t SET v = 'q' WHERE id = {id};")).collect();
    writes.push("ALTER TABLE t ADD COLUMN w VARCHAR(10);".to_string());
    let before = stamp_records();
    let m = merge_arm(
        &mut db,
        &prov,
        "alter + update d=4",
        "alter-upd-4",
        &writes,
        &updated,
        updated.len(),
        &mut frames,
    );
    let written = stamp_records() - before;
    assert!(
        written >= updated.len() + 2,
        "the fixture wrote {written} Stamp records for {} published versions, so its rewrite moved \
         fewer than two attributed rows and cannot tell a schema-phase sync from none",
        updated.len()
    );
    assert_eq!(
        (m.during.stamps, m.during.row_authors, m.during.total()),
        (1, 1, 2),
        "a MERGE that alters a table must sync the provenance file exactly twice: once at the \
         schema phase, so the rewrite's stamps are durable with the rewrite, and once after the \
         commit. Measured stamps={} row_authors={} total={} ({written} Stamp records written)",
        m.during.stamps,
        m.during.row_authors,
        m.during.total()
    );
}

/// **Each altered table's rewrite stamps are durable before the NEXT table's rewrite begins.**
///
/// A merge that alters two tables rewrites them one after the other, and the buffer pool can write
/// the first table's rewritten pages, and the catalog page that makes them reachable, to the
/// database file by eviction while the second is still being rewritten. A process abort keeps what
/// was written. So the first table's moved-row stamps must be durable by the end of ITS rewrite,
/// not only by the schema phase's final heap flush: otherwise an abort during the second rewrite
/// reopens the first table cleanly altered and its moved rows unattributed. Found by the third D219
/// review (F1).
///
/// # Pre-registered, from the source, before this test was ever run
///
/// Two packed tables, `t` and `u` (41 rows each, published by one MERGE, so attributed); then one
/// MERGE staging two UPDATEs on each and an ADD COLUMN on each.
///
/// | tree | `stamps` | `row_authors` | `total()` |
/// |---|---|---|---|
/// | `bf10eec`, stamps eager | m_t + m_u + 4 | 1 | m_t + m_u + 5 (m = 41 each, predicted) |
/// | `882e475`, every stamp deferred to the final sync | 0 | 1 | 1 |
/// | `eff03e8`, one flush after both rewrites | 1 | 1 | 2: RED, one table's stamps waited for the other's rewrite |
/// | the fix, one flush at the end of each rewrite | 2 | 1 | 3 |
#[test]
fn each_altered_tables_rewrite_stamps_are_durable_before_the_next_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let prov = dir.path().join("d219-two-tables.provenance");
    let mut db = Db::with_provenance(&prov);
    let mut setup = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));", &mut setup);
    db.ok("CREATE TABLE u (id INTEGER NOT NULL, v VARCHAR(120));", &mut setup);
    let stamp_records =
        || DurableProvenanceStore::open(&prov).expect("reopen the provenance file").recovery().stamps;

    let mut frames = 0usize;
    let pad = "y".repeat(60);
    let packed: Vec<i64> = (1..=41).collect();
    let mut writes: Vec<String> = Vec::new();
    for table in ["t", "u"] {
        writes.extend(packed.iter().map(|id| format!("INSERT INTO {table} VALUES ({id}, '{pad}');")));
    }
    // The author check reads table `t`; both tables' rows count toward the op total.
    merge_arm(&mut db, &prov, "packed t + u d=82", "packed-82", &writes, &packed, 82, &mut frames);

    let updated: Vec<i64> = vec![1, 2];
    let mut writes: Vec<String> = Vec::new();
    for table in ["t", "u"] {
        writes.extend(updated.iter().map(|id| format!("UPDATE {table} SET v = 'q' WHERE id = {id};")));
    }
    writes.push("ALTER TABLE t ADD COLUMN w VARCHAR(10);".to_string());
    writes.push("ALTER TABLE u ADD COLUMN w VARCHAR(10);".to_string());
    let before = stamp_records();
    let m = merge_arm(&mut db, &prov, "alter t + u, update 2+2", "alter-2", &writes, &updated, 4, &mut frames);
    let written = stamp_records() - before;
    assert!(
        written >= 4 + 2 * 2,
        "the fixture wrote {written} Stamp records for 4 published versions, so its two rewrites \
         did not each move at least two attributed rows and cannot show one flush per table"
    );
    assert_eq!(
        (m.during.stamps, m.during.row_authors, m.during.total()),
        (2, 1, 3),
        "a MERGE that alters two tables must make each table's rewrite stamps durable at the end of \
         that table's rewrite (two syncs) and the publish once after the commit. Measured \
         stamps={} row_authors={} total={} ({written} Stamp records written)",
        m.during.stamps,
        m.during.row_authors,
        m.during.total()
    );
}
