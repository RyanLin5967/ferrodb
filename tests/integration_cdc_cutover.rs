//! E20 — the snapshot-to-stream cutover, with **no gap and no overlap**.
//!
//! `integration_cdc_snapshot.rs` already asserts the weaker half of this: nothing is lost across the
//! join. That is at-least-once, and it is the contract [`snapshot_table`] promises — it brackets a
//! read it cannot see inside, so the best it can do is hand off early and re-deliver.
//!
//! This file asserts the strong version against `snapshot_table_exact`: every row appears **exactly
//! once** across snapshot and stream. No id missing, no id twice.
//!
//! # The scenario, and why each part of it is there
//!
//! One arrangement exercises both ways a cutover goes wrong, so a fix for either alone fails here:
//!
//! - **A transaction already in flight when the snapshot is taken.** MVCC does not show a reader
//!   another transaction's uncommitted work, so the snapshot excludes it — and its records sit
//!   *below* the LSN at which the scan started. A stream resuming there meets its `Commit` having
//!   never read its changes and emits nothing at all for it. That is the **gap**, and it is silent:
//!   a feed missing a row looks exactly like a feed that never had one.
//! - **Transactions that commit while the scan runs.** Pulling the resume point back far enough to
//!   catch the first case drags in commits the snapshot *did* contain. Delivering those again is
//!   the **overlap**.
//!
//! The two sets interleave in the log, which is the whole reason no single LSN separates them and
//! the cutover has to be decided per transaction.
//!
//! The companion test at the bottom runs the same scenario through the at-least-once path and
//! asserts it does in fact duplicate and drop. Without it, the exactly-once test above would pass
//! just as well against a scenario that never put any pressure on the join.

use std::collections::BTreeMap;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::replication::snapshot::{snapshot_table, snapshot_table_exact};
use ferrodb::replication::stream::{FeedStreamer, Subscription};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
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
    Db { _dir: dir, catalog, wal, bp, txn }
}

fn exec(
    sql: &str,
    catalog: &mut Catalog,
    bp: &Arc<BufferPoolManager>,
    txn: &Arc<TxnManager>,
    session: &mut Session,
) -> Outcome {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
    run(stmts.remove(0), catalog, bp.clone(), txn.clone(), session)
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

/// How many times each row id appears in a feed — **with** multiplicity, because the whole question
/// here is whether anything arrives twice.
///
/// Schema events are not rows and are skipped by name rather than by hoping their JSON does not
/// look like a row's. Only the `after` image is read, which is enough: this scenario is inserts.
fn id_counts(feed: &str) -> BTreeMap<u64, usize> {
    let mut counts = BTreeMap::new();
    for line in feed.lines() {
        if line.contains("\"op\":\"CREATE_TABLE\"") || line.contains("\"op\":\"DROP_TABLE\"") {
            continue;
        }
        let key = "\"after\":{\"id\":";
        let Some(i) = line.find(key) else {
            panic!("a feed line carried no row id, so the feed cannot be checked: {line}");
        };
        let rest = &line[i + key.len()..];
        let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        let id: u64 = rest[..end].parse().unwrap_or_else(|e| panic!("bad id in {line}: {e}"));
        *counts.entry(id).or_insert(0) += 1;
    }
    counts
}

fn ids(feed: &str) -> Vec<u64> {
    id_counts(feed).keys().copied().collect()
}

/// The same count, restricted to one table's events.
fn id_counts_for(feed: &str, table: &str) -> BTreeMap<u64, usize> {
    let want = format!("{{\"table\":\"{table}\"");
    let only: String = feed
        .lines()
        .filter(|l| l.starts_with(&want))
        .map(|l| format!("{l}\n"))
        .collect();
    id_counts(&only)
}

/// Drain a streamer to the end of the durable log, returning the feed it wrote.
///
/// Through a [`Subscription`] rather than raw pumps, because that is the path a real consumer
/// takes: it claims the log at the resume point so a checkpoint cannot discard what it is about to
/// read. Subscribing is also the first thing that would fail if a handoff pointed below the log's
/// base, and this asserts it does not.
///
/// Stops when a pump neither emits anything **nor moves the cursor**. `emitted == 0` alone is not
/// the end: a batch whose every event was suppressed as already-snapshotted emits nothing and still
/// advances, and treating that as the end would stop the feed at the first such batch.
fn drain(streamer: &FeedStreamer, wal: &Arc<WalManager>, from: u64) -> (String, usize) {
    let mut sub = Subscription::new(wal, from)
        .unwrap_or_else(|e| panic!("the handoff at {from} is not subscribable: {e}"));
    let mut out: Vec<u8> = Vec::new();
    let mut suppressed = 0;
    for round in 0..200 {
        let before = sub.cursor();
        let p = sub.pump(streamer, &mut out).expect("pump");
        suppressed += p.suppressed;
        assert!(p.is_clean(), "the stream dropped records it could not decode: {p:?}");
        if p.emitted == 0 && p.cursor == before {
            break;
        }
        assert!(round < 199, "the stream did not terminate");
    }
    (String::from_utf8(out).unwrap(), suppressed)
}

/// Rows 1..=13 exist before the cutover, a transaction holding row 100 is open across it, rows
/// 14..=15 commit during the scan, and row 16 arrives afterwards. Every one of them must reach the
/// consumer exactly once.
#[test]
fn snapshot_and_stream_deliver_every_row_exactly_once() {
    let mut d = db("cutover_exact");
    let (bp, txn) = (d.bp.clone(), d.txn.clone());
    let catalog = &mut d.catalog;

    // Two connections: one doing ordinary work, one holding a transaction open across the cutover.
    let mut app = Session::new();
    let mut holder = Session::new();

    exec("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", catalog, &bp, &txn, &mut app);
    for i in 1..=10 {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), catalog, &bp, &txn, &mut app);
    }

    // **The in-flight transaction.** Its `Begin` and its row are written now; its `Commit` lands
    // after the snapshot. It is the case a resume point taken "just before the scan" steps over.
    exec("BEGIN;", catalog, &bp, &txn, &mut holder);
    exec("INSERT INTO inventory VALUES (100, 1000);", catalog, &bp, &txn, &mut holder);

    // Committed while that transaction is open, so their commits sit *above* its `Begin`. The
    // snapshot contains them, and a stream resuming at that `Begin` will read them again.
    for i in 11..=13 {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), catalog, &bp, &txn, &mut app);
    }

    let mut snap_out: Vec<u8> = Vec::new();
    let snap = snapshot_table_exact("inventory", &txn, &mut snap_out, |reader| {
        // **Committed during the scan**, before the read runs. Under a snapshot pinned when the
        // reader opened, these are invisible to it and belong to the stream.
        for i in 14..=15 {
            exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), catalog, &bp, &txn, &mut app);
        }

        // The read, under the transaction the snapshot opened. That is what ties the rows written
        // out to the transaction set the cutover is decided by.
        app.current = Some(reader);
        let rows = match exec("SELECT * FROM inventory;", catalog, &bp, &txn, &mut app) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        app.current = None;
        Ok((vec!["id".into(), "qty".into()], rows))
    })
    .expect("exact snapshot");

    // The held transaction commits after the cutover, and one more row arrives.
    exec("COMMIT;", catalog, &bp, &txn, &mut holder);
    exec("INSERT INTO inventory VALUES (16, 160);", catalog, &bp, &txn, &mut app);
    d.wal.flush().unwrap();

    let snapshot_feed = String::from_utf8(snap_out).unwrap();

    // The snapshot is exactly the committed state as of the reader's transaction: 1..=13 and
    // nothing else. Asserted rather than assumed, because if the read were *not* pinned the
    // exactly-once result below could come from the snapshot having swallowed 14 and 15 instead.
    assert_eq!(
        ids(&snapshot_feed),
        (1..=13).collect::<Vec<u64>>(),
        "the snapshot is not the state its own transaction saw: {snapshot_feed}"
    );
    assert!(
        snap.included.includes(1) || snap.included.high_water > 1,
        "the snapshot boundary is empty, so nothing could be filtered by it"
    );

    let streamer = FeedStreamer::new(LogicalDecoder::new(catalog))
        .resuming_after_snapshot(snap.included.clone());
    let (stream_feed, suppressed) = drain(&streamer, &d.wal, snap.resume_lsn);

    assert!(
        suppressed > 0,
        "nothing was suppressed, so the resume point never reached back over commits the snapshot \
         already had and this scenario did not exercise the overlap at all"
    );

    // **The assertion.** Every row exactly once across the two feeds.
    let mut counts = id_counts(&snapshot_feed);
    for (id, n) in id_counts(&stream_feed) {
        *counts.entry(id).or_insert(0) += n;
    }

    let mut expected: Vec<u64> = (1..=16).collect();
    expected.push(100);
    for id in &expected {
        match counts.get(id) {
            None => panic!(
                "id {id} reached neither feed: the cutover dropped it, which a real consumer could \
                 never detect. Saw: {counts:?}"
            ),
            Some(1) => {}
            Some(n) => panic!(
                "id {id} was delivered {n} times across snapshot+stream. Saw: {counts:?}"
            ),
        }
    }
    assert_eq!(
        counts.keys().copied().collect::<Vec<u64>>(),
        expected,
        "the feeds carried rows that were never written: {counts:?}"
    );

    // And the two directions the assertion above proves, named individually so a failure says which
    // half broke rather than only that something did.
    assert_eq!(id_counts(&stream_feed).get(&100), Some(&1), "the in-flight transaction's row never \
        arrived: the resume point was taken after its records. Stream: {stream_feed}");
    assert_eq!(id_counts(&stream_feed).get(&12), None, "a row the snapshot already contained was \
        re-delivered by the stream. Stream: {stream_feed}");
}

/// **The same scenario, through the at-least-once path, does both things wrong.**
///
/// This is not a complaint about [`snapshot_table`]: hand-off-before-the-scan is the documented
/// contract there and duplication is what it trades for safety. It is here because it is the only
/// way to know the test above is testing anything — a scenario that does not break at-least-once
/// would pass under exactly-once for free.
#[test]
fn the_at_least_once_handoff_both_duplicates_and_drops_in_this_scenario() {
    let mut d = db("cutover_atleastonce");
    let (bp, txn) = (d.bp.clone(), d.txn.clone());
    let catalog = &mut d.catalog;
    let mut app = Session::new();
    let mut holder = Session::new();

    exec("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", catalog, &bp, &txn, &mut app);
    for i in 1..=10 {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), catalog, &bp, &txn, &mut app);
    }
    exec("BEGIN;", catalog, &bp, &txn, &mut holder);
    exec("INSERT INTO inventory VALUES (100, 1000);", catalog, &bp, &txn, &mut holder);
    for i in 11..=13 {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), catalog, &bp, &txn, &mut app);
    }

    let mut snap_out: Vec<u8> = Vec::new();
    let snap = snapshot_table("inventory", &d.wal, &mut snap_out, || {
        for i in 14..=15 {
            exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), catalog, &bp, &txn, &mut app);
        }
        // No transaction to read under, so this read sees a snapshot taken *now* — after the rows
        // above committed and after the handoff LSN was captured.
        let rows = match exec("SELECT * FROM inventory;", catalog, &bp, &txn, &mut app) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        Ok((vec!["id".into(), "qty".into()], rows))
    })
    .expect("snapshot");

    exec("COMMIT;", catalog, &bp, &txn, &mut holder);
    exec("INSERT INTO inventory VALUES (16, 160);", catalog, &bp, &txn, &mut app);
    d.wal.flush().unwrap();

    let snapshot_feed = String::from_utf8(snap_out).unwrap();
    let streamer = FeedStreamer::new(LogicalDecoder::new(catalog));
    let (stream_feed, suppressed) = drain(&streamer, &d.wal, snap.lsn);
    assert_eq!(suppressed, 0, "a streamer with no snapshot boundary suppressed something");

    let mut counts = id_counts(&snapshot_feed);
    for (id, n) in id_counts(&stream_feed) {
        *counts.entry(id).or_insert(0) += n;
    }

    // The overlap: rows that committed between the handoff LSN and the read are in both feeds.
    assert_eq!(
        counts.get(&14),
        Some(&2),
        "expected the at-least-once handoff to re-deliver row 14: {counts:?}"
    );
    assert_eq!(counts.get(&15), Some(&2), "{counts:?}");

    // The gap: the in-flight transaction's records are below the handoff LSN, so the stream sees
    // its `Commit` with nothing staged and emits nothing for it. This is the failure the exact path
    // fixes by pulling the resume point back to that transaction's `Begin`.
    assert_eq!(
        counts.get(&100),
        None,
        "expected the at-least-once handoff to lose the in-flight transaction's row: {counts:?}"
    );
}

/// **Two tables, one reader, one boundary.**
///
/// `snapshot_table_exact` opens a reader per call, so two calls give two boundaries — fine when
/// each table has its own stream, wrong when one stream carries both. The boundary is
/// transaction-level and says nothing about tables, so the consistent version is available; it is
/// just a different shape, and `src/replication/snapshot.rs` documents it. This test is here
/// because a documented alternative nobody has run is a claim, not a feature.
#[test]
fn one_reader_snapshots_two_tables_against_a_single_boundary() {
    use ferrodb::catalog::column::Value;

    let mut d = db("cutover_two_tables");
    let (bp, txn) = (d.bp.clone(), d.txn.clone());
    let catalog = &mut d.catalog;
    let mut app = Session::new();
    let mut holder = Session::new();

    exec("CREATE TABLE a (id INTEGER NOT NULL, qty INTEGER);", catalog, &bp, &txn, &mut app);
    exec("CREATE TABLE b (id INTEGER NOT NULL, qty INTEGER);", catalog, &bp, &txn, &mut app);
    for i in 1..=4 {
        exec(&format!("INSERT INTO a VALUES ({i}, {});", i * 10), catalog, &bp, &txn, &mut app);
        exec(&format!("INSERT INTO b VALUES ({i}, {});", i * 10), catalog, &bp, &txn, &mut app);
    }

    // One transaction spanning both tables, open across the cutover. The resume point has to reach
    // back to its `Begin`, which drags the commits below back into the stream's range.
    exec("BEGIN;", catalog, &bp, &txn, &mut holder);
    exec("INSERT INTO a VALUES (100, 1000);", catalog, &bp, &txn, &mut holder);
    exec("INSERT INTO b VALUES (100, 1000);", catalog, &bp, &txn, &mut holder);

    // Committed while that transaction is open, so these sit *above* the resume point and *inside*
    // the snapshot: exactly the events the single boundary has to suppress.
    exec("INSERT INTO a VALUES (5, 50);", catalog, &bp, &txn, &mut app);
    exec("INSERT INTO b VALUES (5, 50);", catalog, &bp, &txn, &mut app);

    let handoff = txn.begin_snapshot_read().expect("snapshot read");

    // Committed while the reader is open: invisible to it, so the stream owes them.
    exec("INSERT INTO a VALUES (6, 60);", catalog, &bp, &txn, &mut app);
    exec("INSERT INTO b VALUES (6, 60);", catalog, &bp, &txn, &mut app);

    // Both tables read under the one reader, so both describe the same instant.
    let read = |sql: &str, catalog: &mut Catalog, app: &mut Session| -> Vec<u64> {
        app.current = Some(handoff.txn_id);
        let rows = match exec(sql, catalog, &bp, &txn, app) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        app.current = None;
        rows.iter()
            .map(|r| match r[0] {
                Value::Integer(i) => i as u64,
                _ => panic!("id column is not an integer"),
            })
            .collect()
    };
    let snap_a = read("SELECT * FROM a;", catalog, &mut app);
    let snap_b = read("SELECT * FROM b;", catalog, &mut app);
    txn.end_read_only(handoff.txn_id).expect("close reader");

    assert_eq!(snap_a, (1..=5).collect::<Vec<u64>>(), "table a snapshot is not the reader's state");
    assert_eq!(snap_b, (1..=5).collect::<Vec<u64>>(), "table b snapshot is not the reader's state");

    exec("COMMIT;", catalog, &bp, &txn, &mut holder);
    exec("INSERT INTO a VALUES (7, 70);", catalog, &bp, &txn, &mut app);
    exec("INSERT INTO b VALUES (7, 70);", catalog, &bp, &txn, &mut app);
    d.wal.flush().unwrap();

    let streamer = FeedStreamer::new(LogicalDecoder::new(catalog))
        .resuming_after_snapshot(handoff.snapshot.clone());
    let (stream_feed, suppressed) = drain(&streamer, &d.wal, handoff.resume_lsn);
    assert!(suppressed > 0, "the single boundary suppressed nothing, so it was never exercised");

    for (table, snap) in [("a", &snap_a), ("b", &snap_b)] {
        let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
        for id in snap.iter() {
            *counts.entry(*id).or_insert(0) += 1;
        }
        for (id, n) in id_counts_for(&stream_feed, table) {
            *counts.entry(id).or_insert(0) += n;
        }
        let mut expected: Vec<u64> = (1..=7).collect();
        expected.push(100);
        assert_eq!(
            counts.keys().copied().collect::<Vec<u64>>(),
            expected,
            "table {table} did not receive exactly the rows written: {counts:?}"
        );
        for (id, n) in &counts {
            assert_eq!(*n, 1, "table {table} delivered id {id} {n} times: {counts:?}");
        }
    }
}
