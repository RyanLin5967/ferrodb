//! E17 — what a checkpoint does to a consumer that is not holding on.
//!
//! `checkpoint_pressure <dir> <commits>` — run with `FERRODB_CHECKPOINT_INTERVAL=1`.
//!
//! The change feed reads the WAL, and the WAL is thrown away at every checkpoint. That is normally
//! invisible, because a checkpoint happens once every 256 commits and a consumer keeping up reads
//! the records long before they are discarded. **Rare is not the same as safe**, and three separate
//! features in this project shipped with a test that passed only because it never crossed the
//! threshold.
//!
//! So this makes the rare thing constant — every commit truncates — and runs the same workload two
//! ways:
//!
//!   - **pinned**: the consumer holds a `Subscription`, which claims the log at its cursor.
//!   - **unpinned**: the consumer keeps a bare LSN, exactly as the earlier tests did.
//!
//! The difference is the whole argument for the pin existing. Both counts are printed so the caller
//! can compare them against the workload rather than take this program's word for it.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::logical::LogicalDecoder;
use std::io::Write;
use ferrodb::replication::stream::{FeedStreamer, Subscription};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn open(dir: &str, tag: &str) -> Db {
    let path = format!("{dir}/{tag}.db");
    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&path).expect("open db");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(format!("{path}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { catalog, wal, bp, txn, session: Session::new() }
}

fn sql(d: &mut Db, s: &str) {
    let tokens = Scanner::new(s.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "parse error in: {s}");
    run(stmts.remove(0), &mut d.catalog, d.bp.clone(), d.txn.clone(), &mut d.session)
        .unwrap_or_else(|e| panic!("{s} failed: {e}"));
}

/// Row events only. Schema events are deliberately excluded from this count: a checkpoint
/// re-establishes every table's DDL at the head of the new log, so at an interval of 1 the feed
/// carries a `CREATE_TABLE` per commit. That is correct — it is what keeps the log self-describing
/// from its own base — but it means "how many events arrived" is not the same question as "did
/// every row arrive", and the second one is the one that matters here.
fn count_inserts(buf: &[u8]) -> usize {
    String::from_utf8_lossy(buf)
        .lines()
        .filter(|l| l.contains("\"op\":\"INSERT\""))
        .count()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).cloned().unwrap_or_else(|| ".".into());
    let commits: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(30);

    // --- pinned: the consumer holds a claim on the log it has not read yet.
    let mut d = open(&dir, "pinned");
    sql(&mut d, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    let streamer = FeedStreamer::new(LogicalDecoder::new(&d.catalog));
    let mut sub = Subscription::from_start(&d.wal).expect("subscribe");
    let mut pinned_events = 0usize;
    let mut pinned_inserts = 0usize;
    let mut pinned_err: Option<String> = None;
    for i in 1..=commits {
        sql(&mut d, &format!("INSERT INTO t VALUES ({i}, {});", i * 10));
        let mut buf = Vec::new();
        match sub.pump(&streamer, &mut buf) {
            Ok(p) => {
                pinned_events += p.emitted;
                pinned_inserts += count_inserts(&buf);
            }
            Err(e) => {
                pinned_err = Some(e.to_string());
                break;
            }
        }
    }
    // Drain anything left behind.
    if pinned_err.is_none() {
        loop {
            let mut buf = Vec::new();
            match sub.pump(&streamer, &mut buf) {
                Ok(p) if p.emitted == 0 => break,
                Ok(p) => {
                pinned_events += p.emitted;
                pinned_inserts += count_inserts(&buf);
            }
                Err(e) => {
                    pinned_err = Some(e.to_string());
                    break;
                }
            }
        }
    }

    // --- unpinned: a bare cursor, which is what every consumer looked like before E16.
    let mut d2 = open(&dir, "unpinned");
    sql(&mut d2, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    let streamer2 = FeedStreamer::new(LogicalDecoder::new(&d2.catalog));
    let mut cursor = FeedStreamer::start_cursor(&d2.wal);
    // The unpinned consumer tracks delivery progress separately from its read position, exactly
    // as the pinned one does inside its Subscription.
    let mut unpinned_through = 0u64;
    let mut unpinned_events = 0usize;
    let mut unpinned_inserts = 0usize;
    let mut unpinned_err: Option<String> = None;
    for i in 1..=commits {
        sql(&mut d2, &format!("INSERT INTO t VALUES ({i}, {});", i * 10));
        let mut buf = Vec::new();
        match streamer2.pump(&d2.wal, cursor, unpinned_through, &mut buf) {
            Ok(p) => {
                cursor = p.cursor;
                unpinned_through = p.emitted_through;
                unpinned_events += p.emitted;
                unpinned_inserts += count_inserts(&buf);
            }
            Err(e) => {
                unpinned_err = Some(e.to_string());
                break;
            }
        }
    }

    println!("COMMITS {commits}");
    println!("PINNED_EVENTS {pinned_events}");
    println!("PINNED_INSERTS {pinned_inserts}");
    println!("UNPINNED_EVENTS {unpinned_events}");
    println!("UNPINNED_INSERTS {unpinned_inserts}");
    println!("PINNED_ERR {}", pinned_err.clone().unwrap_or_else(|| "none".into()));
    println!("UNPINNED_ERR {}", unpinned_err.clone().unwrap_or_else(|| "none".into()));
    eprintln!("interval={} pinned={pinned_events} unpinned={unpinned_events}",
              std::env::var("FERRODB_CHECKPOINT_INTERVAL").unwrap_or_else(|_| "default".into()));
}
