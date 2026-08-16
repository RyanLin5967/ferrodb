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
use ferrodb::replication::{read_handshake, write_handshake, Message, ReplicationSource};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "primary.db".into());
    let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:0".into());
    let rows: i32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(50);

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
    for i in 1..=rows {
        exec(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), &mut catalog, &mut session);
    }
    bp.flush_all().expect("flush pages");
    wal.flush().expect("flush wal");

    let listener = TcpListener::bind(&addr).expect("bind");
    println!("LISTENING {}", listener.local_addr().unwrap());
    println!("DURABLE {}", ReplicationSource::new(&wal).durable_lsn());
    println!("START {}", ReplicationSource::new(&wal).start_lsn());
    std::io::stdout().flush().unwrap();

    for stream in listener.incoming() {
        let mut stream = match stream { Ok(s) => s, Err(_) => continue };
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
            match src.read_from(from, 64 * 1024) {
                Ok((bytes, _next)) if bytes.is_empty() => {
                    let _ = Message::UpToDate { durable_lsn: src.durable_lsn() }.write_to(&mut stream);
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
    }
}
