//! E12 — snapshot then stream, with nothing lost at the join.
//!
//! A consumer connecting to a database that already holds rows must end up knowing about all of
//! them: the ones that predate it, from the snapshot, and the ones that arrive after, from the
//! stream. The join between those two is where a CDC source loses data, and it loses it silently —
//! a feed missing records is indistinguishable from one that had none.
//!
//! So the test writes rows before the snapshot, **during** the snapshot's read, and after it, then
//! asserts the union covers a contiguous range with no id missing. The write during the read is the
//! important one: it is the case that separates a handoff taken before the scan from one taken
//! after, and only one of those is safe.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::replication::snapshot::snapshot_table;
use ferrodb::replication::stream::FeedStreamer;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db(tag: &str) -> Db {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(dir.path().join(format!("{tag}.db"))).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, wal, bp, txn, session: Session::new() }
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

/// Ids appearing in a JSONL feed, from either a snapshot READ or a streamed change.
fn ids_in(feed: &str) -> Vec<u64> {
    let mut ids = Vec::new();
    for line in feed.lines() {
        // Read the id out of whichever image the line carries.
        let key = "\"id\":";
        if let Some(i) = line.find(key) {
            let rest = &line[i + key.len()..];
            let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
            if let Ok(v) = rest[..end].parse::<u64>() {
                ids.push(v);
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

#[test]
fn a_consumer_that_snapshots_then_streams_sees_every_row_that_ever_existed() {
    let mut d = db("handoff");
    exec("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);",
         &mut d.catalog, &d.bp, &d.txn, &mut d.session);

    // Rows that predate the consumer entirely.
    for i in 1..=10 {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10),
             &mut d.catalog, &d.bp, &d.txn, &mut d.session);
    }

    let mut snap_out: Vec<u8> = Vec::new();
    let (bp, txn) = (d.bp.clone(), d.txn.clone());
    let catalog = &mut d.catalog;
    let session = &mut d.session;

    let snap = snapshot_table("inventory", &d.wal, &mut snap_out, || {
        // The consistent read, through the engine's own MVCC visibility.
        let rows = match exec("SELECT * FROM inventory;", catalog, &bp, &txn, session) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        // **A write that races the scan.** This is the case the handoff rule exists for: it lands
        // after the handoff LSN was taken and may or may not be in `rows`, and either way the
        // stream must re-deliver it rather than skip it.
        exec("INSERT INTO inventory VALUES (11, 110);", catalog, &bp, &txn, session);
        Ok((vec!["id".into(), "qty".into()], rows))
    })
    .expect("snapshot");

    assert!(snap.rows >= 10, "the snapshot captured only {} rows", snap.rows);
    assert!(
        snap.concurrent_writes,
        "no write landed during the scan, so this test never exercised the handoff race"
    );

    // Rows that arrive strictly after the snapshot.
    for i in 12..=15 {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10),
             catalog, &bp, &txn, session);
    }
    d.wal.flush().unwrap();

    // Stream from the handoff.
    let streamer = FeedStreamer::new(LogicalDecoder::new(catalog));
    let mut stream_out: Vec<u8> = Vec::new();
    let mut cursor = snap.lsn;
    let mut emitted_through = 0u64;
    for _ in 0..50 {
        let p = streamer.pump(&d.wal, cursor, emitted_through, &mut stream_out).expect("pump");
        if p.emitted == 0 {
            break;
        }
        cursor = p.cursor;
        emitted_through = p.emitted_through;
    }

    let snapshot_feed = String::from_utf8(snap_out).unwrap();
    let stream_feed = String::from_utf8(stream_out).unwrap();

    assert!(
        snapshot_feed.contains("\"op\":\"READ\""),
        "the snapshot did not emit READ events: {snapshot_feed}"
    );
    assert!(
        !stream_feed.is_empty(),
        "the stream produced nothing after the handoff, so the join was never tested"
    );

    // **The assertion.** Every id from 1 to 15 must appear somewhere across the two feeds. A gap is
    // the failure a real consumer could never detect on its own.
    let combined = format!("{snapshot_feed}{stream_feed}");
    let ids = ids_in(&combined);
    for want in 1..=15u64 {
        assert!(
            ids.contains(&want),
            "id {want} is missing from snapshot+stream, so the handoff lost a row. Saw: {ids:?}"
        );
    }
}

/// The snapshot must reflect the table as it was, values included — not just the right count.
#[test]
fn snapshot_rows_carry_their_actual_values() {
    let mut d = db("values");
    exec("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR(16));",
         &mut d.catalog, &d.bp, &d.txn, &mut d.session);
    exec("INSERT INTO t VALUES (1, 'alice');", &mut d.catalog, &d.bp, &d.txn, &mut d.session);
    exec("INSERT INTO t VALUES (2, 'bob');", &mut d.catalog, &d.bp, &d.txn, &mut d.session);

    let mut out: Vec<u8> = Vec::new();
    let (bp, txn) = (d.bp.clone(), d.txn.clone());
    let catalog = &mut d.catalog;
    let session = &mut d.session;
    let snap = snapshot_table("t", &d.wal, &mut out, || {
        let rows = match exec("SELECT * FROM t;", catalog, &bp, &txn, session) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        Ok((vec!["id".into(), "name".into()], rows))
    })
    .unwrap();

    assert_eq!(snap.rows, 2);
    let feed = String::from_utf8(out).unwrap();
    assert!(feed.contains("\"name\":\"alice\""), "alice is missing: {feed}");
    assert!(feed.contains("\"name\":\"bob\""), "bob is missing: {feed}");
    // A quiet database: the handoff is exact and nothing will be re-delivered.
    assert!(!snap.concurrent_writes, "nothing wrote, yet an overlap was reported");
}

/// A value with JSON metacharacters must survive the snapshot path too, not just the stream path.
#[test]
fn snapshot_values_are_escaped() {
    let mut d = db("escape");
    exec("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(64));",
         &mut d.catalog, &d.bp, &d.txn, &mut d.session);
    exec("INSERT INTO t VALUES (1, 'he said \"hi\"');",
         &mut d.catalog, &d.bp, &d.txn, &mut d.session);

    let mut out: Vec<u8> = Vec::new();
    let (bp, txn) = (d.bp.clone(), d.txn.clone());
    let catalog = &mut d.catalog;
    let session = &mut d.session;
    snapshot_table("t", &d.wal, &mut out, || {
        let rows = match exec("SELECT * FROM t;", catalog, &bp, &txn, session) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        Ok((vec!["id".into(), "body".into()], rows))
    })
    .unwrap();

    let feed = String::from_utf8(out).unwrap();
    assert!(
        feed.contains("\\\"hi\\\""),
        "a quote reached the snapshot feed unescaped, which breaks the consumer's parser: {feed}"
    );
}
