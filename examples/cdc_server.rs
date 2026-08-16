//! E11 — a streaming CDC source over TCP. `cdc_server <db> <addr> <rows>`
//!
//! Writes a workload on one thread while serving the change feed on another, so a consumer sees
//! events arrive as transactions commit rather than as one batch at the end.
//!
//! Protocol, deliberately trivial so any language can speak it: the consumer connects and sends one
//! line — the cursor to resume from, or `0` to start at the beginning of the retained log. The
//! server then writes JSON Lines until the workload is finished and the consumer is caught up, at
//! which point it closes. Resuming is the same connection made again with the last
//! `commit_end_lsn` the consumer processed.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::replication::stream::FeedStreamer;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "cdc.db".into());
    let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:0".into());
    let rows: i32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(20);

    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&db).expect("open db");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());

    let mut session = Session::new();
    {
        let tokens = Scanner::new(
            "CREATE TABLE inventory (id INTEGER NOT NULL, item VARCHAR(32), qty INTEGER);"
                .chars().collect(),
            Vec::new(),
        ).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        run(p.parse().remove(0), &mut catalog, bp.clone(), txn.clone(), &mut session)
            .expect("create table");
    }

    // The decoder is built AFTER the table exists, so its dir_root mapping includes it. Built
    // before, it would resolve nothing and every change would be reported unresolved.
    let streamer = Arc::new(FeedStreamer::new(LogicalDecoder::new(&catalog)));
    let listener = TcpListener::bind(&addr).expect("bind");
    println!("LISTENING {}", listener.local_addr().unwrap());
    println!("START {}", FeedStreamer::start_cursor(&wal));
    std::io::stdout().flush().unwrap();

    let done = Arc::new(AtomicBool::new(false));

    let writer = {
        let (bp, txn, wal, done) = (bp.clone(), txn.clone(), wal.clone(), done.clone());
        std::thread::spawn(move || {
            let mut session = Session::new();
            let mut exec = |sql: &str, cat: &mut Catalog, s: &mut Session| {
                let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
                let mut p = Parser::new(tokens);
                let mut stmts = p.parse();
                assert!(p.errors.is_empty(), "parse error in: {sql}");
                run(stmts.remove(0), cat, bp.clone(), txn.clone(), s)
                    .unwrap_or_else(|e| panic!("{sql} failed: {e}"));
            };
            for i in 1..=rows {
                exec(&format!("INSERT INTO inventory VALUES ({i}, 'item{i}', {});", i * 10),
                     &mut catalog, &mut session);
                if i % 5 == 0 {
                    exec(&format!("UPDATE inventory SET qty = {} WHERE id = {i};", i * 100),
                         &mut catalog, &mut session);
                }
                wal.flush().expect("flush");
                std::thread::yield_now();
            }
            wal.flush().expect("final flush");
            done.store(true, Ordering::SeqCst);
        })
    };

    for stream in listener.incoming() {
        let mut stream = match stream { Ok(s) => s, Err(_) => continue };
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            continue;
        }
        let mut cursor: u64 = line.trim().parse().unwrap_or(0);
        // Delivery progress, distinct from the read position: the cursor is clamped back for open
        // transactions, so without this every committed transaction after one would be re-sent.
        let mut emitted_through: u64 = 0;
        if cursor == 0 {
            cursor = FeedStreamer::start_cursor(&wal);
        }

        loop {
            // Sampled BEFORE the pump, and that ordering is the whole of it. The writer flushes and
            // THEN sets `done`, so a pump that begins after `done` is observed is guaranteed to see
            // the final frontier. Reading `done` after the pump instead leaves a window: the pump
            // samples the frontier, the writer flushes its tail and sets `done`, and the server
            // then breaks on "emitted 0 and finished" having never sent those last events. That is
            // the same check-then-act shape as the cursor rule, and it made this test pass alone
            // and fail under a loaded full suite.
            let finished_before_pump = done.load(Ordering::SeqCst);
            let pumped = match streamer.pump(&wal, cursor, emitted_through, &mut stream) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("pump failed: {e}");
                    break;
                }
            };
            cursor = pumped.cursor;
            emitted_through = pumped.emitted_through;
            if pumped.emitted == 0 {
                // Caught up. Finish only when the workload is finished too, so a consumer is not
                // disconnected merely for being faster than the writer.
                //
                // The condition is "nothing left to emit", NOT "cursor has reached the frontier".
                // Those are not the same and the difference hangs the server forever: the cursor
                // tracks COMMITS, while the frontier is a byte position that includes records
                // producing no events — a `TxnEnd` sits above the final commit permanently, so
                // `cursor >= frontier` is never satisfied and a consumer waiting for EOF waits for
                // ever. Caught by the Go consumer, which reads until close rather than stopping at
                // a client-side limit the way the earlier tests did.
                if finished_before_pump {
                    break;
                }
                std::thread::yield_now();
            }
        }
        eprintln!("consumer disconnected at cursor {cursor}");
        drop(stream);
        if done.load(Ordering::SeqCst) {
            break;
        }
    }
    let _ = writer.join();
}
