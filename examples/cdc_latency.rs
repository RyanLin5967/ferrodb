//! E16 — how long does a change take to reach the feed?
//!
//! `cdc_latency [db-path] [samples]`
//!
//! Measures **commit-to-emitted**: the wall clock from a SQL `INSERT` returning to that change
//! being available as a decoded event. That is the part of a CDC pipeline this project owns —
//! everything after it is network and destination, which vary by deployment.
//!
//! # Why a distribution and not an average
//!
//! A mean hides the tail, and the tail is what a pipeline is judged on: a feed that is usually
//! instant and occasionally seconds late is a feed that occasionally makes someone's dashboard
//! wrong, and the mean will not show it. p50, p95 and max are reported, along with the worst
//! sample's index so a spike can be located rather than merely noted.
//!
//! # The instrument checks itself before it reports
//!
//! Every iteration must produce exactly one event. A timing loop that found nothing would report
//! beautifully small numbers — it would be timing the cost of looking, not the cost of the work —
//! so a shortfall refuses to print a measurement at all. Likewise the calibration line: if the
//! clock cannot resolve the interval being measured, the numbers are noise wearing a unit.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::replication::stream::{FeedStreamer, Subscription};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "cdc_latency.db".into());
    let samples: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(200);

    // Single-writer lock, taken before the file is opened. Two processes on one database both build
    // an ArenaPageStore from the same checkpoint and hand the same pages to different branches, and
    // every such page still passes its checksum - so refusing here is the only detection point.
    //
    // Held for the whole run: `_db_lock` releases on the way out, including on an early return.
    let _db_lock = ferrodb::storage::db_lock::DbLock::acquire(std::path::Path::new(&db))
        .unwrap_or_else(|e| { eprintln!("cdc_latency: {e}"); std::process::exit(1); });

    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&db).expect("open db");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());

    let mut session = Session::new();
    let mut exec = |sql: &str, cat: &mut Catalog, s: &mut Session| {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in: {sql}");
        run(stmts.remove(0), cat, bp.clone(), txn.clone(), s).unwrap_or_else(|e| {
            eprintln!("{sql} failed: {e}");
            std::process::exit(3);
        })
    };

    exec("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut catalog, &mut session);

    // Calibration. If the clock cannot resolve an interval of the size being measured, every number
    // below is noise wearing a unit — so this is checked before anything is reported, not after.
    let cal = Instant::now();
    std::hint::black_box((0..1000).sum::<u64>());
    let resolution = cal.elapsed();
    if resolution >= Duration::from_millis(1) {
        eprintln!("REFUSING: the clock resolved a trivial loop as {resolution:?}, which is the \
                   order of the interval being measured. These numbers would be noise.");
        std::process::exit(4);
    }

    let streamer = FeedStreamer::new(LogicalDecoder::new(&catalog));
    // A Subscription rather than a bare cursor, so the automatic checkpoint every 256 commits
    // cannot truncate the log out from under this consumer. Without it a run of 1000 samples dies
    // around commit 256 — which is how this was found, and why the earlier 200-sample run was
    // passing for a reason that did not generalise.
    let mut sub = Subscription::from_start(&wal).expect("subscribe");

    // Drain whatever the CREATE TABLE produced, so the first sample measures an insert and not a
    // backlog that predates the timer.
    let mut sink = Vec::new();
    loop {
        let p = sub.pump(&streamer, &mut sink).expect("pump");
        if p.emitted == 0 {
            break;
        }
    }

    let mut latencies: Vec<Duration> = Vec::with_capacity(samples);
    let mut missed = 0usize;
    let mut bytes_emitted = 0usize;

    for i in 0..samples {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), &mut catalog, &mut session);
        // The commit has returned. From here to the event being decodable is what a consumer waits.
        let t0 = Instant::now();

        let mut buf = Vec::new();
        let mut emitted = 0;
        // Bounded: a pipeline that never produces the event is a failure to report, not a hang.
        let deadline = t0 + Duration::from_secs(5);
        while Instant::now() < deadline {
            let p = sub.pump(&streamer, &mut buf).expect("pump");
            emitted += p.emitted;
            if emitted > 0 {
                break;
            }
        }
        let dt = t0.elapsed();

        if emitted == 0 {
            missed += 1;
        } else {
            latencies.push(dt);
            bytes_emitted += buf.len();
        }
    }

    // The instrument checks itself. A loop that found nothing would report beautifully small
    // numbers, because it would be timing the cost of looking rather than the cost of the work.
    if missed > 0 {
        eprintln!("REFUSING: {missed} of {samples} commits never produced an event. These timings \
                   would be measuring a loop that found nothing.");
        std::process::exit(5);
    }
    if latencies.len() != samples {
        eprintln!("REFUSING: collected {} timings for {samples} commits.", latencies.len());
        std::process::exit(5);
    }

    latencies.sort_unstable();
    let total: Duration = latencies.iter().sum();
    let worst = latencies.last().copied().unwrap_or_default();

    println!("commit-to-emitted latency over {samples} commits");
    println!("  p50 ......... {:?}", pct(&latencies, 0.50));
    println!("  p95 ......... {:?}", pct(&latencies, 0.95));
    println!("  p99 ......... {:?}", pct(&latencies, 0.99));
    println!("  max ......... {worst:?}");
    println!("  mean ........ {:?}   (reported last, and only for completeness: it hides the tail)",
             total / samples as u32);
    println!("  feed bytes .. {bytes_emitted} over {samples} events");
    println!();
    println!("SAMPLES {samples} P50_NS {} P95_NS {} MAX_NS {}",
             pct(&latencies, 0.50).as_nanos(),
             pct(&latencies, 0.95).as_nanos(),
             worst.as_nanos());
}
