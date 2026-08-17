//! E4 — a primary that does real SQL work and serves replication over TCP.
//!
//! `repl_primary <db-path> <addr> <rows>`
//!
//! Runs genuine `CREATE TABLE` / `INSERT` statements, so the WAL it ships is the log of real work
//! going through the heap and the transaction manager — not synthetic records manufactured for the
//! occasion. A replica that converges on this has replicated a database, not a test fixture.

use std::io::{BufReader, Write};
use std::net::TcpListener;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::sync::AckTracker;
use ferrodb::replication::{read_handshake, write_handshake, Message, ReplicationSource};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "primary.db".into());
    let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:0".into());
    let rows: i32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(50);

    // Single-writer lock, taken before the file is opened. Two processes on one database both build
    // an ArenaPageStore from the same checkpoint and hand the same pages to different branches, and
    // every such page still passes its checksum - so refusing here is the only detection point.
    //
    // Held for the whole run: `_db_lock` releases on the way out, including on an early return.
    let _db_lock = ferrodb::storage::db_lock::DbLock::acquire(std::path::Path::new(&db))
        .unwrap_or_else(|e| { eprintln!("repl_primary: {e}"); std::process::exit(1); });

    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&db).expect("open db");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());

    let mut session = Session::new();
    let exec = |sql: &str, cat: &mut Catalog, s: &mut Session| {
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

    // E8: a base backup, either taken after the work or taken *while it is still happening*.
    //
    // The hot mode is the one that tests the design rather than the happy path. A backup of a
    // quiescent database has an empty [start_lsn, end_lsn] window, so it proves the base image
    // works and proves nothing at all about the window — and the window is the entire reason the
    // copy is allowed to run without stopping the world. Taking it on a thread while inserts
    // continue means pages really are copied at different points in the log, and a replica that
    // converges anyway has exercised the per-page redo-idempotence argument.
    let backup_dir = format!("{db}.backup");
    let hot = args.get(4).map(|s| s == "hot").unwrap_or(false);

    let handle = if hot {
        let (bp2, wal2, dir2) = (bp.clone(), wal.clone(), backup_dir.clone());
        Some(std::thread::spawn(move || {
            ferrodb::replication::backup::take(&bp2, &wal2, dir2.as_ref()).expect("hot base backup")
        }))
    } else {
        None
    };

    for i in 1..=rows {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), &mut catalog, &mut session);
    }
    bp.flush_all().expect("flush pages");
    wal.flush().expect("flush wal");

    // Without this a replica can only start when the primary has never checkpointed, because a
    // checkpoint truncates the WAL out from under it. `BACKUP_START` is where the replica must
    // begin and is generally NOT `START`: it is inside the surviving log, not at its base.
    let label = match handle {
        Some(h) => h.join().expect("hot backup thread panicked"),
        None => ferrodb::replication::backup::take(&bp, &wal, backup_dir.as_ref())
            .expect("take base backup"),
    };

    let listener = TcpListener::bind(&addr).expect("bind");
    // Writes that tolerate a closed pipe. `println!` PANICS on EPIPE, and a harness that reads the
    // readiness line and then drops its reader closes this pipe underneath us. Proven on
    // `cdc_server`: closing stdout before the first write kills it with `failed printing to stdout:
    // Broken pipe (os error 32)` and exit 101, which is the status an intermittent CI failure
    // reported. A server has no business dying because nobody is reading its log.
    //
    // Six writes here, so the exposure is the widest of the three servers: a harness that stops at
    // LISTENING leaves five more writes to fail.
    let mut out = std::io::stdout();
    let _ = writeln!(out, "LISTENING {}", listener.local_addr().unwrap());
    let _ = writeln!(out, "DURABLE {}", ReplicationSource::new(&wal).durable_lsn());
    let _ = writeln!(out, "START {}", ReplicationSource::new(&wal).start_lsn());
    let _ = writeln!(out, "BACKUP {backup_dir}");
    let _ = writeln!(out, "BACKUP_START {}", label.start_lsn);
    let _ = writeln!(out, "BACKUP_END {}", label.end_lsn);
    let _ = out.flush();

    // E6: serving runs on its own thread so the main thread can WAIT for a replica ack. A primary
    // that only serves after it has finished committing can never demonstrate synchronous commit,
    // because there is nothing left to wait for.
    let acks = Arc::new(AckTracker::new());
    let sync_mode = args.get(4).map(|s| s == "sync").unwrap_or(false);
    let durable = ReplicationSource::new(&wal).durable_lsn();

    let serve = {
        let (wal, acks) = (wal.clone(), acks.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = match stream { Ok(s) => s, Err(_) => continue };
                let peer = stream
                    .peer_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|_| "unknown".into());
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                if read_handshake(&mut reader).is_err() || write_handshake(&mut stream).is_err() {
                    continue;
                }
                let src = ReplicationSource::new(&wal);
                loop {
                    let msg = match Message::read_from(&mut reader) { Ok(m) => m, Err(_) => break };
                    let from = match msg {
                        Message::Hello { from_lsn } => from_lsn,
                        _ => break,
                    };
                    // A Hello IS the ack: the replica records its position only after the pages it
                    // describes are durable, so "send me what follows N" asserts N is safe there.
                    acks.record(&peer, from);
                    match src.read_from(from, 64 * 1024) {
                        Ok((bytes, _next)) if bytes.is_empty() => {
                            let _ = Message::UpToDate { durable_lsn: src.durable_lsn() }
                                .write_to(&mut stream);
                        }
                        Ok((bytes, _next)) => {
                            let _ = Message::Records { start_lsn: from, bytes }.write_to(&mut stream);
                        }
                        Err(e) => {
                            let _ = Message::Error { message: e.to_string() }.write_to(&mut stream);
                            break;
                        }
                    }
                }
                acks.forget(&peer);
            }
        })
    };

    if sync_mode {
        // The deadline is the caller's choice, and it is short here on purpose: a test that waits
        // a minute to observe a refusal is a test nobody runs.
        let deadline = std::time::Duration::from_secs(
            std::env::var("FERRODB_SYNC_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
        );
        match acks.wait_for(durable, deadline) {
            Ok(()) => println!("SYNC_OK {durable}"),
            // Not a crash and not a silent downgrade: it says exactly what was and was not achieved.
            Err(e) => println!("SYNC_TIMEOUT {e}"),
        }
        std::io::stdout().flush().unwrap();
        // In sync mode the wait IS the point, so exit once it resolves rather than serving forever.
        std::process::exit(0);
    }

    let _ = serve.join();
}
