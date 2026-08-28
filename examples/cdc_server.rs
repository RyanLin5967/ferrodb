//! E11 — a streaming CDC source over TCP. `cdc_server <db> <addr> <rows> [publication-file]`
//!
//! Writes a workload on one thread while serving the change feed on another, so a consumer sees
//! events arrive as transactions commit rather than as one batch at the end.
//!
//! Protocol, deliberately trivial so any language can speak it: the consumer connects and sends one
//! line — the cursor to resume from, or `0` to start at the beginning of the retained log. The
//! server then writes JSON Lines until the workload is finished and the consumer is caught up, at
//! which point it closes. Resuming is the same connection made again with the last
//! `commit_end_lsn` the consumer processed.
//!
//! # A publication stops the serve rather than skipping an event — B7
//!
//! With a publication file, the feed carries only the columns it names. If it refuses an event, this
//! server prints the refusal and closes the connection instead of continuing to poll: a refusal does
//! not advance the cursor, so the same batch would be re-decoded and re-refused for as long as the
//! process ran — a busy loop that emits nothing while reporting no error. Closing hands the operator
//! the reason, and the consumer's cursor is still exactly where the refused commit begins.
//!
//! # Reporting the port it actually bound — I16
//!
//! `<addr>` may be `127.0.0.1:0`, in which case the kernel picks the port and this server says
//! which one on the `LISTENING` line. If `FERRODB_LISTEN_FILE` is set it writes the same address
//! to that path as well, by atomic rename, immediately after binding. That second channel exists
//! for the one harness that closes this process's stdout on purpose; see the comment at the bind.

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
use ferrodb::replication::publication::Publication;
use ferrodb::replication::stream::FeedStreamer;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "cdc.db".into());
    let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:0".into());
    let rows: i32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(20);
    // No publication argument means no policy, said out loud. A file that will not parse is a hard
    // exit: serving everything because the policy could not be read is the one outcome an operator
    // who passed a policy file cannot recover from.
    let publication = match args.get(4) {
        None => Publication::unrestricted(),
        Some(path) => Publication::load(std::path::Path::new(path))
            .unwrap_or_else(|e| { eprintln!("cdc_server: {e}"); std::process::exit(1); }),
    };

    // Single-writer lock, taken before the file is opened. Two processes on one database both build
    // an ArenaPageStore from the same checkpoint and hand the same pages to different branches, and
    // every such page still passes its checksum - so refusing here is the only detection point.
    //
    // Held for the whole run: `_db_lock` releases on the way out, including on an early return.
    let _db_lock = ferrodb::storage::db_lock::DbLock::acquire(std::path::Path::new(&db))
        .unwrap_or_else(|e| { eprintln!("cdc_server: {e}"); std::process::exit(1); });

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
    let streamer = Arc::new(FeedStreamer::new(LogicalDecoder::new(&catalog), publication));
    let listener = TcpListener::bind(&addr).expect("bind");

    // **Address discovery that cannot race — `FERRODB_LISTEN_FILE`.**
    //
    // A harness that needs a free port has two options and only one of them is sound. It can bind
    // `127.0.0.1:0` itself, read the number off the listener, DROP the listener and hand the bare
    // number to a server it spawns afterwards - or it can let the server bind `:0` and report what
    // it got. The first leaves the port owned by nobody for the whole of the server's startup, and
    // that is not a microsecond window: this process takes the single-writer lock, opens the
    // database, builds a buffer pool, creates a catalog and runs a CREATE TABLE before it reaches
    // the line above. Anything else on the machine can take the port in that gap, and on one
    // running ten worktrees of this repo at once, something did - six independent lanes hit `the
    // server never accepted a connection` on the same test.
    //
    // Every other harness in this repo takes the second option already, by reading the `LISTENING`
    // line printed below. The one that cannot is
    // `integration_cdc_go_consumer::the_server_survives_a_consumer_that_stops_reading_its_stdout`,
    // whose entire purpose is to CLOSE this process's stdout before its first write. It needs the
    // same answer through a channel that is not the pipe it is sabotaging, so it sets this variable
    // and reads the address out of a file.
    //
    // Three things this deliberately does, each because the alternative fails quietly:
    //
    // - **Published here, before anything is printed.** Not after, because a harness that pipes
    //   stdout and never drains it fills the pipe buffer and blocks the writer - and an address
    //   that arrives only if the log is being read is exactly the dependency this exists to remove.
    // - **Written to a sibling path and renamed.** `rename` is atomic within a directory, so a
    //   reader polling for the file sees either nothing or a complete address, never a half-written
    //   line it would then parse into the wrong port.
    // - **A failed publish is a hard exit - but it drops the database lock on the way out.**
    //   Carrying on would leave the harness waiting on an address that is never coming, so it would
    //   spend its entire timeout and then report the wrong cause, which is the failure mode that
    //   made this bug take six lanes to place. `std::process::exit` runs no destructors, though, and
    //   `_db_lock` releases only by dropping: exiting straight from here leaves `<db>.lock` behind,
    //   and `DbLock` refuses a stale lock BY DESIGN rather than reclaiming it - so every later open
    //   of that database, by any binary, is refused until somebody deletes the file by hand.
    //   Measured, not reasoned: without the `drop` below, a publish to a non-existent directory left
    //   `x.db.lock` holding pid 52237 and the next open was refused by a process that no longer
    //   existed. Note the `bind` above `.expect()`s, which unwinds and therefore does release it;
    //   this `drop` is what keeps the two failure paths agreeing.
    if let Some(path) = std::env::var_os("FERRODB_LISTEN_FILE") {
        let path = std::path::PathBuf::from(path);
        let tmp = path.with_file_name(format!(
            "{}.tmp",
            path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()
        ));
        let bound = listener.local_addr().expect("local_addr of a bound listener");
        if let Err(e) =
            std::fs::write(&tmp, format!("{bound}\n")).and_then(|()| std::fs::rename(&tmp, &path))
        {
            eprintln!(
                "cdc_server: bound {bound} but could not publish it to {}: {e}",
                path.display()
            );
            drop(_db_lock);
            std::process::exit(1);
        }
    }
    // **Writes that tolerate a closed pipe.** `println!` PANICS on EPIPE — proven, not assumed:
    // closing this process's stdout before its first write kills it with
    // `failed printing to stdout: Broken pipe (os error 32)` and exit 101, which is exactly the
    // status CI reported (`unix_wait_status(25856)`, 25856 >> 8 = 101).
    //
    // The window is small and real: a harness that reads the LISTENING line and then drops its
    // reader closes the pipe between these two lines. A server has no business dying because
    // nobody is reading its log, so these report failure by being ignored rather than by aborting
    // the process mid-serve.
    let mut out = std::io::stdout();
    let _ = writeln!(out, "LISTENING {}", listener.local_addr().unwrap());
    let _ = out.flush();
    let _ = writeln!(out, "START {}", FeedStreamer::start_cursor(&wal));
    let _ = out.flush();

    let done = Arc::new(AtomicBool::new(false));

    let writer = {
        let (bp, txn, wal, done) = (bp.clone(), txn.clone(), wal.clone(), done.clone());
        std::thread::spawn(move || {
            let mut session = Session::new();
            let exec = |sql: &str, cat: &mut Catalog, s: &mut Session| {
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
            // I20: a shape mismatch is not a refusal and not a silence. It means this pump held a
            // schema that does not describe the bytes it read, which since B11 is a thing a running
            // server can reach — and it produced no event, no error and, until `Pumped` carried the
            // count, no number either. Said out loud rather than left for someone to notice as a
            // permanently-null column at the destination.
            if pumped.undecodable > 0 || pumped.unresolved > 0 {
                eprintln!(
                    "cdc_server: WARNING at cursor {cursor}: {} record(s) did not match the schema \
                     this decoder holds and {} named no known table; the feed is INCOMPLETE",
                    pumped.undecodable, pumped.unresolved
                );
            }
            // Before the caught-up test, because a refusal also emits nothing and would otherwise be
            // read as "caught up" or spun on for ever.
            if let Some(refusal) = &pumped.refusal {
                eprintln!(
                    "cdc_server: the feed cannot advance past cursor {cursor}: {refusal}\n\
                     cdc_server: {} event(s) were held back, none lost; closing the connection",
                    pumped.refused
                );
                break;
            }
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
