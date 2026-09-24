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
//! in `stamps`, and are deliberately NOT asserted here: they are one sync per published version on
//! the publish loop, outside the `state` lock and outside this row. They appear in the failure
//! message so a reader sees the whole bill.
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
