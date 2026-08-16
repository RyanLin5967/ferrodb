//! E4 — a replica that follows a primary over TCP until it is caught up.
//!
//! `repl_replica <db-path> <primary-addr> <start-lsn>`
//!
//! Prints `APPLIED <lsn>` when it reaches the primary's durable frontier, so a test can wait on an
//! observable event rather than sleeping.

use std::io::BufReader;
use std::net::TcpStream;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::replication::{read_handshake, write_handshake, Message, ReplicaApplier};
use ferrodb::storage::disk_manager::DiskManager;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "replica.db".into());
    let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:5433".into());
    let start_lsn: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);

    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&db).expect("open db");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let applier = ReplicaApplier::new(Arc::clone(&bp), start_lsn);

    let mut stream = TcpStream::connect(&addr).expect("connect to primary");
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    write_handshake(&mut stream).expect("handshake out");
    read_handshake(&mut reader).expect("handshake in");

    loop {
        Message::Hello { from_lsn: applier.applied_lsn() }
            .write_to(&mut stream)
            .expect("hello");
        match Message::read_from(&mut reader).expect("reply") {
            Message::Records { start_lsn, bytes } => {
                applier.apply(start_lsn, &bytes).expect("apply");
            }
            Message::UpToDate { durable_lsn } => {
                bp.flush_all().expect("flush replica pages");
                println!("APPLIED {} DURABLE {}", applier.applied_lsn(), durable_lsn);
                return;
            }
            Message::Error { message } => {
                eprintln!("primary error: {message}");
                std::process::exit(4);
            }
            other => {
                eprintln!("unexpected message: {other:?}");
                std::process::exit(5);
            }
        }
    }
}
