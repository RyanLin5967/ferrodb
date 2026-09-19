//! B5 — run identity, from the write path through the WAL and out into the change feed.
//!
//! Nothing here is a mock. Rows go in through the SQL front end, the identity record is written by
//! `TxnManager::commit` exactly where the production path writes it, and the events come out of the
//! real `LogicalDecoder` and the real `FeedStreamer`.
//!
//! # The property these tests exist for
//!
//! Attribution used to stop at the database boundary. `ChangeEvent` carried txn, lsn, commit_lsn,
//! commit_end_lsn, table, columns and op — and no writer at all — so a consumer holding a million
//! rows from a model since found unsound had no way to ask which of them came from it.
//!
//! The hard part is not the field. It is **where the identity record sits in the log**, and
//! `an_identity_record_written_at_session_begin_is_stepped_over_by_the_feed_cursor` is the test that
//! pins it: the same workload, the same streamer, two placements, and one of them ships every row
//! attributed to nobody while reporting a clean run.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::lower;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::planner::physical_plan::PhysicalPlan;
use ferrodb::provenance::sha256::prompt_digest;
use ferrodb::provenance::store::MemProvenanceStore;
use ferrodb::provenance::{DurableProvenanceStore, ProvId, ProvenanceStore, RunEntity};
use ferrodb::replication::logical::{ChangeOp, Decoded, LogicalDecoder};
use ferrodb::replication::stream::FeedStreamer;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::wal::log::{RecKind, WalManager};
use ferrodb::wal::txn::{ReadView, TxnManager};

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db(tag: &str) -> Db {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { dir, catalog, wal, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    /// The open transaction id. Only meaningful between `BEGIN` and `COMMIT`.
    fn open_txn(&self) -> u64 {
        self.session.current.expect("no transaction is open; run BEGIN first")
    }

    fn decode_all(&self) -> Decoded {
        self.wal.flush().unwrap();
        LogicalDecoder::new(&self.catalog)
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
    }

    /// The physical slot currently holding the row with this surrogate id.
    fn rid_of(&self, table: &str, id: i32) -> RecordId {
        let view = Arc::new(ReadView { snapshot: Arc::new(self.txn.read_snapshot()), txn_id: 0 });
        let mut exec =
            lower(PhysicalPlan::SeqScan { table: table.into() }, &self.catalog, self.bp.clone(), view)
                .unwrap();
        while let Some(row) = exec.next() {
            let (rid, values) = row.unwrap();
            if values[0] == Value::Integer(id) {
                return rid;
            }
        }
        panic!("row {id} not found in {table}");
    }
}

fn a_run(prov: u32, agent: &str, run_id: &str, model_version: &str, prompt: &str) -> RunEntity {
    RunEntity::new(
        ProvId(prov),
        agent,
        run_id,
        "claude-opus",
        model_version,
        prompt_digest(prompt),
        1_700_000_000_000,
        BranchId::new(prov as u64, 0),
    )
}

/// Row changes only. Schema declarations are re-emitted at every checkpoint and are not a run's
/// writes, so counting them would drown the number the tests are about.
fn writes(d: &Decoded) -> Vec<&ferrodb::replication::logical::ChangeEvent> {
    d.events.iter().filter(|e| e.op.is_write()).collect()
}

// ---------------------------------------------------------------------------------------------
// Exit criterion: every committed change event carries its writer.
// ---------------------------------------------------------------------------------------------

/// **Breaking shape:** more than one agent run writing to one table in one log, with at least one
/// multi-row transaction. A feed that attached the *last* identity it saw to everything, or that
/// attributed a transaction to whichever run committed most recently, passes a single-run workload
/// and fails this one.
#[test]
fn every_committed_row_change_names_the_run_that_wrote_it() {
    let mut d = db("attributed");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");

    let restock = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");
    let auditor = a_run(2, "auditor-agent", "run-99", "2026-07", "check the restock for overshoot");

    // Two rows in ONE transaction, so a decoder that attributes per-record rather than per-commit
    // has somewhere to go wrong.
    d.sql("BEGIN;");
    d.txn.bind_run(d.open_txn(), restock.clone()).unwrap();
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("INSERT INTO inventory VALUES (2, 20);");
    d.sql("COMMIT;");

    d.sql("BEGIN;");
    d.txn.bind_run(d.open_txn(), auditor.clone()).unwrap();
    d.sql("UPDATE inventory SET qty = 99 WHERE id = 1;");
    d.sql("COMMIT;");

    let decoded = d.decode_all();
    let rows = writes(&decoded);
    assert_eq!(rows.len(), 3, "expected two inserts and one update, got {:?}", rows.len());
    assert!(decoded.fully_attributed(), "unattributed: {:?}", decoded.unattributed_commits);
    assert_eq!(decoded.unattributed_events, 0);

    for e in &rows {
        let w = e.writer.as_ref().unwrap_or_else(|| panic!("{} carried no writer", e.op.name()));
        match &e.op {
            ChangeOp::Insert { .. } => {
                assert_eq!(w.agent_id, "restock-agent", "insert attributed to the wrong run");
                assert_eq!(w.run_id, "run-42");
                assert_eq!(w.model_version, "2026-05");
            }
            ChangeOp::Update { .. } => {
                assert_eq!(w.agent_id, "auditor-agent", "update attributed to the wrong run");
                assert_eq!(w.model_version, "2026-07");
            }
            other => panic!("unexpected op {}", other.name()),
        }
        // The prompt travels as a digest and is non-zero — the field's whole purpose.
        assert_ne!(w.prompt_hash, [0u8; 32], "the prompt hash is the all-zero placeholder");
    }

    // Both runs are nameable from the log alone, with no provenance store consulted.
    assert_eq!(decoded.runs.len(), 2, "the log did not describe its own writers: {:?}", decoded.runs);
    assert_eq!(decoded.runs[&1].agent_id, "restock-agent");
    assert_eq!(decoded.runs[&2].agent_id, "auditor-agent");

    // The two prompts really are distinguished, so `prompt_hash` carries information rather than
    // merely being non-zero.
    assert_ne!(decoded.runs[&1].prompt_hash, decoded.runs[&2].prompt_hash);
}

/// **E79b: the same assertion, over the SQL path.**
///
/// The test above builds its `RunEntity` in Rust and hands it to `bind_run`, so its non-zero
/// `prompt_hash` proves only that the field can hold a digest — it says nothing about the path a
/// client actually uses. Over SQL the value was `[0u8; 32]` at every site, because
/// `begin_session_with_model` had no prompt parameter and `BEGIN AGENT SESSION` had no clause
/// carrying one. The whole chain is exercised here instead: the clause is parsed, the runtime hashes
/// it into the interned run, `MERGE` binds that run to the publishing transaction, and the digest
/// comes back out of the **real** `LogicalDecoder` on the change event.
///
/// **Breaking shape:** one agent session whose prompt is known to the test, so the digest can be
/// asserted against `prompt_digest` of the exact text rather than against "not all zeroes" — the
/// weaker assertion is one a second wrong constant would pass.
#[test]
fn a_prompt_declared_over_sql_reaches_the_feed_as_a_digest() {
    let mut d = db("sql_prompt");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");

    let prompt = "top up everything below reorder";
    d.sql(&format!(
        "BEGIN AGENT SESSION AS 'restock-agent' RUN 'run-42' MODEL 'claude-opus/2026-05' \
         PROMPT '{prompt}';"
    ));
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("MERGE;");

    let decoded = d.decode_all();
    let rows = writes(&decoded);
    assert_eq!(rows.len(), 1, "expected the merged insert on the feed, got {:?}", rows.len());

    let w = rows[0].writer.as_ref().expect("the merged row carried no writer");
    assert_eq!(w.agent_id, "restock-agent");
    assert_eq!(w.run_id, "run-42");
    assert_ne!(w.prompt_hash, [0u8; 32], "the prompt hash is the all-zero placeholder over SQL");
    assert_eq!(
        w.prompt_hash,
        prompt_digest(prompt),
        "the feed carries a hash, but not this prompt's"
    );

    // The log describes its own writer, so a consumer needs no provenance store to read the digest.
    assert_eq!(decoded.runs.len(), 1, "{:?}", decoded.runs);
    let (_, run) = decoded.runs.iter().next().unwrap();
    assert_eq!(run.prompt_hash, prompt_digest(prompt));

    // And the plaintext went nowhere: what the WAL holds is 32 bytes, which is the claim
    // `provenance::sha256`'s header makes for the whole field.
    let wal_bytes = std::fs::read(d.dir.path().join("sql_prompt.wal")).expect("no wal file");
    assert!(
        !wal_bytes.windows(prompt.len()).any(|c| c == prompt.as_bytes()),
        "the prompt was written to the WAL in plain text"
    );
    assert!(
        wal_bytes.windows(32).any(|c| c == prompt_digest(prompt)),
        "the digest is not in the WAL either, so the assertion above proved nothing"
    );
}

/// **The anti-vacuity half, and the count that must never be assumed to be zero.**
///
/// A transaction nobody bound a run to is a legitimate thing — every ordinary SQL write is one — and
/// its rows are emitted with `writer: None`. What must not happen is that they are emitted while the
/// feed reports a clean run, because from the events alone the two are identical.
///
/// **Breaking shape:** a mixed log, agent writes and ordinary writes together. A count taken over
/// the whole feed, or one that gave up as soon as it saw a single writer, reads zero here.
#[test]
fn a_commit_with_no_run_bound_ships_unattributed_and_is_counted() {
    let mut d = db("unattributed");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    let restock = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");

    d.sql("BEGIN;");
    d.txn.bind_run(d.open_txn(), restock.clone()).unwrap();
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("COMMIT;");

    // No bind: an ordinary transaction, two rows.
    d.sql("BEGIN;");
    let anonymous = d.open_txn();
    d.sql("INSERT INTO inventory VALUES (2, 20);");
    d.sql("INSERT INTO inventory VALUES (3, 30);");
    d.sql("COMMIT;");

    let decoded = d.decode_all();
    let rows = writes(&decoded);
    assert_eq!(rows.len(), 3);
    assert!(!decoded.fully_attributed(), "an unattributed commit was reported as fully attributed");
    assert_eq!(
        decoded.unattributed_commits.iter().copied().collect::<Vec<_>>(),
        vec![anonymous],
        "the wrong transaction was named, or none was"
    );
    assert_eq!(decoded.unattributed_events, 2, "the rows that shipped without an author");

    let attributed: Vec<_> = rows.iter().filter(|e| e.writer.is_some()).collect();
    assert_eq!(attributed.len(), 1, "the bound transaction's row lost its writer");
    assert_eq!(attributed[0].writer.as_ref().unwrap().agent_id, "restock-agent");
}

// ---------------------------------------------------------------------------------------------
// THE HARD PART: where the record sits in the log.
// ---------------------------------------------------------------------------------------------

/// Run a two-transaction workload and stream it in two pumps, returning what the feed delivered.
///
/// The shape is the one that breaks a naive placement, and it is deliberately awkward:
///
/// * T1 opens and writes rows, and stays open;
/// * T2 opens, writes, and commits;
/// * the feed is pumped — T2's rows go out, and the cursor is clamped back to T1's first staged
///   record because `Decoded::open_from` says it must be;
/// * T1 commits;
/// * the feed is pumped again, from that clamped cursor.
///
/// Everything below the clamp has been read once and will never be read again. An identity record
/// written when T1's session began sits there.
fn stream_two_pumps(early_identity: bool) -> (usize, usize, Vec<Option<String>>) {
    let tag = if early_identity { "early" } else { "at-commit" };
    let mut d = db(tag);
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    let writer = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");

    // B7 gave every render path a publication. This feed is about attribution, not egress
    // policy, so it publishes everything - `unrestricted()` is the identity policy, not a
    // weakening of one.
    let streamer = FeedStreamer::new(
        LogicalDecoder::new(&d.catalog),
        ferrodb::replication::publication::Publication::unrestricted(),
    );
    let mut cursor = FeedStreamer::start_cursor(&d.wal);
    let mut delivered_through = 0u64;

    // ---- T1 opens and writes, and does NOT commit --------------------------------------------
    d.sql("BEGIN;");
    let t1 = d.open_txn();
    if early_identity {
        // **The wrong position**: written when the session begins, exactly as a naive
        // implementation would. `append_chained` is the same call `commit` uses, so the record
        // itself is identical — only where it sits differs.
        d.txn
            .append_chained(t1, &RecKind::RunIdentity { run: writer.clone() })
            .unwrap();
    } else {
        // The real path: bound now, written by `commit` immediately before the `Commit` record.
        d.txn.bind_run(t1, writer.clone()).unwrap();
    }
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("INSERT INTO inventory VALUES (2, 20);");

    // ---- T2 opens, writes and commits while T1 is still open ---------------------------------
    // A second session is needed because one `Session` holds one open transaction.
    let mut other = Session::new();
    {
        let sql = |s: &str, catalog: &mut Catalog, bp: &Arc<BufferPoolManager>, txn: &Arc<TxnManager>, session: &mut Session| {
            let tokens = Scanner::new(s.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty(), "parse error in `{s}`");
            run(stmts.remove(0), catalog, bp.clone(), txn.clone(), session).unwrap();
        };
        sql("BEGIN;", &mut d.catalog, &d.bp, &d.txn, &mut other);
        sql("INSERT INTO inventory VALUES (9, 90);", &mut d.catalog, &d.bp, &d.txn, &mut other);
        sql("COMMIT;", &mut d.catalog, &d.bp, &d.txn, &mut other);
    }

    // ---- pump one: T2 goes out, the cursor is clamped back for T1 -----------------------------
    d.wal.flush().unwrap();
    let mut feed = Vec::new();
    let first = streamer.pump(&d.wal, cursor, delivered_through, &mut feed).unwrap();
    assert_eq!(first.withheld, 1, "T1 should have been withheld, not {}", first.withheld);
    cursor = first.cursor;
    delivered_through = first.emitted_through;

    // ---- T1 commits, and the feed is pumped from the clamped cursor ---------------------------
    d.sql("COMMIT;");
    d.wal.flush().unwrap();
    let second = streamer.pump(&d.wal, cursor, delivered_through, &mut feed).unwrap();

    let text = String::from_utf8(feed).unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
    // The writer's agent id per delivered row change, in order. `None` where the line carried
    // `"writer":null`.
    let mut writers = Vec::new();
    for line in &lines {
        if !line.contains("\"op\":\"INSERT\"")
            && !line.contains("\"op\":\"UPDATE\"")
            && !line.contains("\"op\":\"DELETE\"")
        {
            continue;
        }
        if line.contains("\"writer\":null") {
            writers.push(None);
        } else {
            let at = line.find("\"agent\":\"").expect("a writer object with no agent");
            let rest = &line[at + "\"agent\":\"".len()..];
            let end = rest.find('"').unwrap();
            writers.push(Some(rest[..end].to_string()));
        }
    }
    let _ = &d.dir;
    (first.unattributed, second.unattributed, writers)
}

/// **The correctness property this whole lane turns on.**
///
/// `Decoded::open_from` is the earliest *staged* record of any still-open transaction, and the feed
/// cursor may never advance past it. A `Begin` stages nothing and neither does an identity record,
/// so a record written at session begin sits BELOW the clamp: it is read once, stepped over, and
/// never read again. When the transaction finally commits, the decoder has no idea who wrote it and
/// every one of its rows ships attributed to nobody — while the pump reports a clean run.
///
/// Written immediately before the `Commit` instead, the record cannot be separated from the commit
/// it describes: a clamp for a still-open transaction always rewinds to *below* its first staged
/// row, and the identity record sits above that.
///
/// **Breaking shape:** a transaction that is still open when a later one commits, so the cursor is
/// clamped at all. A workload where every transaction commits before the next begins never clamps,
/// so both placements pass it — which is exactly how this defect would reach production.
#[test]
fn an_identity_record_written_at_session_begin_is_stepped_over_by_the_feed_cursor() {
    let (early_first, early_second, early_writers) = stream_two_pumps(true);
    let (late_first, late_second, late_writers) = stream_two_pumps(false);

    // Both placements deliver the same rows: T2's one row, then T1's two.
    assert_eq!(early_writers.len(), 3, "the early-identity run did not deliver three row changes");
    assert_eq!(late_writers.len(), 3, "the at-commit run did not deliver three row changes");

    // T2 was never bound to a run under either placement, so its row is unattributed in both. That
    // is the anti-vacuity anchor: the difference below is about T1 and not about the feed having
    // lost the ability to attribute anything.
    assert_eq!(early_writers[0], None);
    assert_eq!(late_writers[0], None);
    assert_eq!(early_first, 1, "pump one should have reported T2's row as unattributed");
    assert_eq!(late_first, 1);

    // T1's two rows: stepped over under the early placement, attributed under the real one.
    assert_eq!(
        &early_writers[1..],
        &[None, None],
        "the identity record written at session begin was somehow still found; if this ever \
         becomes true the cursor rule in `stream::pump` changed and this test is measuring \
         something else"
    );
    assert_eq!(
        early_second, 2,
        "pump two shipped T1's rows with no writer and did not count them"
    );

    assert_eq!(
        &late_writers[1..],
        &[Some("restock-agent".to_string()), Some("restock-agent".to_string())],
        "the identity record written immediately before the Commit was lost too — the placement \
         fix does not hold"
    );
    assert_eq!(late_second, 0, "the at-commit placement still reported unattributed rows");
}

// ---------------------------------------------------------------------------------------------
// Retention across truncation.
// ---------------------------------------------------------------------------------------------

/// **A checkpoint discards the log whole, and must not erase the database's writers with it.**
///
/// `WalManager::truncate` throws the log away and restarts it at the current end — it cannot drop a
/// prefix — so every identity record in it is gone the moment a checkpoint runs. `TxnManager`
/// re-declares the retained run table at the head of the new log, exactly as `replay_schema` does
/// for DDL.
///
/// **Breaking shape:** any workload that checkpoints at all, which at the default interval means
/// any workload of 256 commits — or one line of code calling `checkpoint()`. Everything shorter
/// passes without the retention.
#[test]
fn a_checkpoint_does_not_erase_the_databases_writers() {
    let mut d = db("checkpointed");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    let restock = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");
    let auditor = a_run(2, "auditor-agent", "run-99", "2026-07", "check the restock for overshoot");

    for (r, id) in [(&restock, 1), (&auditor, 2)] {
        d.sql("BEGIN;");
        d.txn.bind_run(d.open_txn(), r.clone()).unwrap();
        d.sql(&format!("INSERT INTO inventory VALUES ({id}, {});", id * 10));
        d.sql("COMMIT;");
    }
    assert_eq!(d.txn.retained_runs(), 2, "the runs were not retained for replay");

    // Before: the log names both writers because the binding records are still in it.
    let before = d.decode_all();
    assert_eq!(before.runs.len(), 2);
    assert!(before.fully_attributed());

    d.txn.checkpoint().expect("checkpoint");

    // After: every record written above is gone. What survives is what was re-declared.
    let after = d.decode_all();
    assert!(
        writes(&after).is_empty(),
        "the checkpoint did not actually truncate; this test would pass without the retention"
    );
    assert_eq!(
        after.runs.len(),
        2,
        "the checkpoint erased the database's writers: a reader starting at the new base cannot \
         name run {:?}",
        after.runs.keys().collect::<Vec<_>>()
    );
    assert_eq!(after.runs[&1].agent_id, "restock-agent");
    assert_eq!(after.runs[&2].model_version, "2026-07");
    assert_eq!(
        after.runs[&1].prompt_hash,
        prompt_digest("top up everything below reorder"),
        "the replayed declaration lost the prompt digest"
    );

    // A declaration binds nothing: it is written under transaction 0, which never commits. If it
    // bound, every row of the next transaction would be attributed to whichever run was declared
    // last.
    assert!(
        after.unattributed_commits.is_empty() && writes(&after).is_empty(),
        "declarations produced events or bindings of their own"
    );

    // And a write after the checkpoint is still attributable, so the retention did not leave the
    // manager in a state where new bindings stop working.
    d.sql("BEGIN;");
    d.txn.bind_run(d.open_txn(), restock.clone()).unwrap();
    d.sql("INSERT INTO inventory VALUES (7, 70);");
    d.sql("COMMIT;");
    let later = d.decode_all();
    assert_eq!(writes(&later).len(), 1);
    assert_eq!(writes(&later)[0].writer.as_ref().unwrap().agent_id, "restock-agent");
    assert!(later.fully_attributed());
}

// ---------------------------------------------------------------------------------------------
// Durability of `who_wrote_row`, against real storage.
// ---------------------------------------------------------------------------------------------

/// **Exit criterion: `who_wrote_row` answers after a reopen.**
///
/// The unit test in `provenance::durable` proves the store round-trips synthetic ids. This one uses
/// the `RecordId`s of rows that really went through the SQL front end and really landed in a heap
/// page, and asks after the store object is gone.
///
/// **Breaking shape:** asking in a different process from the one that wrote. `MemProvenanceStore`
/// passes every other provenance test in this repo and fails this one, because all of them intern,
/// stamp and ask inside one live store — which is precisely how `reopen_with_storage` could exist,
/// attaching to another process's tree, and restore no attribution at all.
#[test]
fn who_wrote_row_answers_after_the_database_is_reopened() {
    let mut d = db("reopen");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("INSERT INTO inventory VALUES (2, 20);");
    let one = d.rid_of("inventory", 1);
    let two = d.rid_of("inventory", 2);
    assert_ne!(one, two, "both rows landed in one slot; the test would prove nothing");

    let prov_path = d.dir.path().join("provenance.log");
    let restock = a_run(0, "restock-agent", "run-42", "2026-05", "top up everything below reorder");
    let auditor = a_run(0, "auditor-agent", "run-99", "2026-07", "check the restock for overshoot");

    let (id_a, id_b) = {
        let store = DurableProvenanceStore::open(&prov_path).unwrap();
        let a = store.intern(&restock).unwrap();
        let b = store.intern(&auditor).unwrap();
        store.stamp(one, a).unwrap();
        store.stamp(two, b).unwrap();
        assert_eq!(store.who_wrote(one).unwrap().agent_id, "restock-agent");
        (a, b)
    };

    // The store object is gone. A `MemProvenanceStore` here answers nothing.
    let store = DurableProvenanceStore::open(&prov_path).unwrap();
    assert_eq!(store.discarded_tail_bytes(), 0);
    assert_eq!(store.who_wrote(one).unwrap().agent_id, "restock-agent");
    assert_eq!(store.who_wrote(one).unwrap().run_id, "run-42");
    assert_eq!(store.who_wrote(two).unwrap().agent_id, "auditor-agent");
    assert_eq!(store.attribute(one).unwrap(), id_a);
    assert_eq!(store.attribute(two).unwrap(), id_b);
    assert_eq!(store.rows_written_by(id_a).unwrap(), vec![one]);

    // Anti-vacuity: a row nobody stamped is still unattributed, so the answers above are
    // attribution rather than a store that says yes to whatever it is asked.
    d.sql("INSERT INTO inventory VALUES (3, 30);");
    let three = d.rid_of("inventory", 3);
    assert_eq!(store.attribute(three).unwrap(), ProvId::NONE);
    assert_eq!(store.describe_row(three), "unattributed");

    // The same workload against the in-memory store, to show what this test is actually measuring:
    // a fresh `MemProvenanceStore` is what a reopened runtime used to get, and it knows nothing.
    let fresh = MemProvenanceStore::new();
    assert_eq!(fresh.attribute(one).unwrap(), ProvId::NONE);
    assert_eq!(fresh.describe_row(one), "unattributed");
}

/// **The handoff to the runtime lane, compiled rather than described.**
///
/// `AgentRuntime` holds its provenance store as `Arc<dyn ProvenanceStore>` and all three of its
/// constructors build a `MemProvenanceStore`. Wiring the durable one is a one-line change in each,
/// in a file this lane does not own — so this test does the part that can be checked from here:
/// that `DurableProvenanceStore` really is usable through that exact type, with every trait method
/// exercised behind the object rather than on the concrete struct.
///
/// Without this, "it is a drop-in replacement" would be a claim in a summary. A missing `Send`, a
/// method taking `&mut self`, or a signature that did not match would only be discovered by the
/// lane that has to do the wiring.
#[test]
fn the_durable_store_is_usable_as_the_trait_object_agent_runtime_holds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provenance.log");
    let entity = a_run(0, "restock-agent", "run-42", "2026-05", "top up everything below reorder");
    let rid = RecordId { page_id: 4, slot_num: 2 };

    let id = {
        let store: Arc<dyn ProvenanceStore> =
            Arc::new(DurableProvenanceStore::open(&path).unwrap());
        let id = store.intern(&entity).unwrap();
        store.stamp(rid, id).unwrap();
        assert_eq!(store.attribute(rid).unwrap(), id);
        assert_eq!(store.lookup(id).unwrap().agent_id, "restock-agent");

        // Shared across threads, which is what `AgentRuntime` does with it: two sessions for one
        // run must reach the same slot.
        let other = Arc::clone(&store);
        let mine = entity.clone();
        let same = std::thread::spawn(move || other.intern(&mine).unwrap()).join().unwrap();
        assert_eq!(same, id, "two sessions for one run got two slots");
        id
    };

    // And the durability is intact through the trait object, which is the whole point of swapping
    // the implementation there.
    let store: Arc<dyn ProvenanceStore> = Arc::new(DurableProvenanceStore::open(&path).unwrap());
    assert_eq!(store.attribute(rid).unwrap(), id);
    assert_eq!(store.lookup(id).unwrap().run_id, "run-42");

    // Anti-vacuity: the store the runtime builds today answers nothing about the same row.
    let today: Arc<dyn ProvenanceStore> = Arc::new(MemProvenanceStore::new());
    assert_eq!(today.attribute(rid).unwrap(), ProvId::NONE);
}

/// A run bound to a transaction that then aborts leaves nothing in the log to retract.
///
/// **Breaking shape:** a rolled-back agent transaction. If the identity record were written when
/// the run was bound, it would be sitting in the log describing work that never happened, and a
/// consumer enumerating runs from the log would report an actor whose writes do not exist.
#[test]
fn an_aborted_transactions_run_leaves_no_binding_in_the_log() {
    let mut d = db("aborted");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    let restock = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");

    d.sql("BEGIN;");
    let t = d.open_txn();
    d.txn.bind_run(t, restock.clone()).unwrap();
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("ROLLBACK;");

    let decoded = d.decode_all();
    assert!(decoded.aborted.contains(&t), "the rollback was not seen: {:?}", decoded.aborted);
    assert!(writes(&decoded).is_empty(), "an aborted transaction's rows were emitted");
    assert!(
        decoded.runs.is_empty(),
        "the log declared a run for a transaction that rolled back: {:?}",
        decoded.runs
    );
    assert!(decoded.fully_attributed());

    // Anti-vacuity: the same run, committed, does reach the log.
    d.sql("BEGIN;");
    d.txn.bind_run(d.open_txn(), restock).unwrap();
    d.sql("INSERT INTO inventory VALUES (2, 20);");
    d.sql("COMMIT;");
    let decoded = d.decode_all();
    assert_eq!(decoded.runs.len(), 1, "a committed run did not reach the log either");
    assert_eq!(writes(&decoded).len(), 1);
    assert_eq!(writes(&decoded)[0].writer.as_ref().unwrap().run_id, "run-42");
}

/// The guards on `bind_run`, and the case each of them lets through.
#[test]
fn binding_a_run_refuses_the_shapes_that_would_produce_a_wrong_answer() {
    let mut d = db("guards");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    let restock = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");
    let auditor = a_run(2, "auditor-agent", "run-99", "2026-07", "check the restock for overshoot");

    // A transaction that is not open: nothing would ever write the record.
    let err = d.txn.bind_run(999_999, restock.clone()).unwrap_err();
    assert!(format!("{err}").contains("not active"), "{err}");

    d.sql("BEGIN;");
    let t = d.open_txn();

    // `ProvId::NONE` is the value meaning "unattributed"; a record claiming a writer must name one.
    let nameless = a_run(0, "restock-agent", "run-42", "2026-05", "top up everything below reorder");
    let err = d.txn.bind_run(t, nameless).unwrap_err();
    assert!(format!("{err}").contains("ProvId::NONE"), "{err}");

    // One transaction, one writer.
    d.txn.bind_run(t, restock.clone()).unwrap();
    d.txn.bind_run(t, restock.clone()).expect("rebinding the identical run must be a no-op");
    let err = d.txn.bind_run(t, auditor).unwrap_err();
    assert!(format!("{err}").contains("already bound"), "{err}");

    // Anti-vacuity: the legal binding still works, end to end.
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("COMMIT;");
    let decoded = d.decode_all();
    assert_eq!(writes(&decoded)[0].writer.as_ref().unwrap().agent_id, "restock-agent");
}

/// **An agent's MERGE ships its rows WITH a writer — the gap E79 closed.**
///
/// Every test above binds a run by hand (`txn.bind_run(...)`), which proves the log and the decoder
/// carry attribution correctly. None of them proved the *product* does, and it did not: nothing in
/// `AgentRuntime` called `bind_run`, so an agent could open a session, write, merge, and every event
/// on the resulting feed carried `"writer":null`. Row-level authorship was stamped all along — it is
/// what `who_wrote_row` reads — but that lives in memory beside the heap, and a reader of the log has
/// no access to it. The feed is the product's only outward attribution surface, and it said nobody.
///
/// **Breaking shape, and why the anti-vacuity half is not optional:** `writer` is `Option` by design,
/// and `jsonl.rs` documents null as the correct answer for a change no run produced. So "the feed has
/// a writer" is only evidence when the same feed also still says null for a plain SQL write — one
/// assertion without the other passes both on a fixed runtime and on one that stamps every event with
/// a bogus author.
#[test]
fn an_agent_merge_ships_its_rows_with_a_writer_and_a_plain_write_still_ships_none() {
    use ferrodb::agent_sql::runtime::AgentRuntime;

    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("merge_writer.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("merge_writer.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let runtime = Arc::new(AgentRuntime::new());

    let mut exec = |sql: &str, session: &mut Session| {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut catalog, bp.clone(), txn.clone(), session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    };

    let mut plain = Session::with_runtime(runtime.clone());
    exec("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);", &mut plain);
    // A plain write, outside any agent session: it has no run and must keep its honest null.
    exec("INSERT INTO inventory VALUES (1, 10);", &mut plain);

    let mut agent = Session::with_runtime(runtime.clone());
    exec("BEGIN AGENT SESSION AS 'restock-agent' RUN 'run-42';", &mut agent);
    exec("UPDATE inventory SET qty = 99 WHERE id = 1;", &mut agent);
    exec("MERGE;", &mut agent);

    wal.flush().unwrap();
    let decoded = LogicalDecoder::new(&catalog)
        .decode(
            &wal,
            wal.base_lsn.load(Ordering::SeqCst),
            wal.next_lsn.load(Ordering::SeqCst),
        )
        .expect("decode");

    let rows: Vec<_> = decoded
        .events
        .iter()
        .filter(|e| matches!(e.op, ChangeOp::Insert { .. } | ChangeOp::Update { .. } | ChangeOp::Delete { .. }))
        .collect();
    assert!(!rows.is_empty(), "the feed carried no row changes at all, so neither half is testable");

    let attributed: Vec<_> = rows.iter().filter(|e| e.writer.is_some()).collect();
    assert!(
        !attributed.is_empty(),
        "the agent's merge shipped {} row change(s) and not one carried a writer — `bind_run` is not \
         being called on the publish path, so the log cannot say who wrote them",
        rows.len()
    );
    let w = attributed[0].writer.as_ref().unwrap();
    assert_eq!(w.agent_id, "restock-agent", "the feed named the wrong agent: {w:?}");
    assert_eq!(w.run_id, "run-42", "the feed named the wrong run: {w:?}");

    // The anti-vacuity half: the plain INSERT is still unattributed, so this is not a runtime that
    // stamps everything.
    let unattributed = rows.len() - attributed.len();
    assert!(
        unattributed >= 1,
        "every row change carried a writer, including the plain INSERT made outside any agent \
         session — attribution that is always present is not attribution"
    );
}

/// **A runtime's attribution outlives the process — the other half of E79.**
///
/// Every `AgentRuntime` constructor builds a `MemProvenanceStore`. That is right for a test and
/// wrong for a database: the row keeps its author STAMP across a restart (it is in the heap), while
/// the table mapping that stamp to an agent lives in this process and does not. So `who_wrote_row`
/// and `ferro_row_authors` answered correctly all session and then answered nothing — the worst
/// shape, because the rows still look attributed.
///
/// `with_durable_provenance` is a builder rather than a constructor argument because the
/// constructors take page stores and a database's *name* is not something they are given; the layer
/// that owns the path applies it (`cli.rs` does).
///
/// **The anti-vacuity half is the whole test.** "The durable store round-trips" would pass against a
/// runtime that never used it, so the same sequence is run on a default (in-memory) runtime and must
/// LOSE the attribution. One assertion without the other proves nothing about the wiring.
#[test]
fn provenance_survives_a_reopen_only_when_the_runtime_was_built_durable() {
    use ferrodb::agent_sql::runtime::AgentRuntime;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attribution");
    let rid = RecordId { page_id: 9, slot_num: 4 };
    let run = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");

    // Durable: intern, stamp, drop the whole runtime, reopen at the same path.
    let id = {
        let rt = AgentRuntime::new().with_durable_provenance(&path).expect("open durable store");
        let id = rt.provenance().intern(&run).expect("intern");
        rt.provenance().stamp(rid, id).expect("stamp");
        id
    };

    let reopened = AgentRuntime::new().with_durable_provenance(&path).expect("reopen durable store");
    assert_eq!(
        reopened.provenance().attribute(rid).expect("attribute"),
        id,
        "the row's author was lost across a reopen, so `who_wrote_row` answers nothing for a row \
         that still carries its stamp"
    );
    let back = reopened.provenance().lookup(id).expect("lookup");
    assert_eq!(back.agent_id, "restock-agent", "the run came back as a different actor: {back:?}");
    assert_eq!(back.run_id, "run-42");

    // Anti-vacuity: the default runtime is in-memory, and must NOT survive the same sequence.
    let mem_path = dir.path().join("unused");
    let mem_id = {
        let rt = AgentRuntime::new();
        let id = rt.provenance().intern(&run).expect("intern");
        rt.provenance().stamp(rid, id).expect("stamp");
        id
    };
    let fresh = AgentRuntime::new();
    assert!(
        fresh.provenance().attribute(rid).map(|p| p == mem_id).unwrap_or(false) == false,
        "a default in-memory runtime reported an attribution from a runtime that no longer exists, \
         so this test cannot tell a durable store from a shared global"
    );
    let _ = mem_path;
}
