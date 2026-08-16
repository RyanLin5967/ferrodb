//! E4/E8 — a replica that follows a primary over TCP until it is caught up.
//!
//! `repl_replica <db-path> <primary-addr> <start-lsn> [backup-dir]`
//!
//! Prints `APPLIED <lsn>` when it reaches the primary's durable frontier, so a test can wait on an
//! observable event rather than sleeping.
//!
//! With `backup-dir` it restores a **base backup** first and starts from the LSN in that backup's
//! label, ignoring `<start-lsn>`. That is the only mode that works once the primary has
//! checkpointed, because a checkpoint truncates the WAL: without a base image there is no state
//! for the surviving records to be applied *to*.

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
    let mut start_lsn: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let backup_dir = args.get(4).cloned();

    // Restore BEFORE opening the file: the restore writes the file, and DiskManager reads its
    // length on open to seed the allocator, so opening first would seed from an empty file.
    if let Some(dir) = &backup_dir {
        let label = ferrodb::replication::backup::restore(dir.as_ref(), db.as_ref())
            .unwrap_or_else(|e| {
                eprintln!("restore failed: {e}");
                std::process::exit(6);
            });
        start_lsn = label.start_lsn;
        println!("RESTORED pages {} start {} end {}", label.page_count, label.start_lsn, label.end_lsn);
    }

    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true).create(true);
    // Truncating a just-restored file would throw the base image away, which is the one mistake
    // that makes a base backup look like it does nothing.
    if backup_dir.is_none() {
        opts.truncate(true);
    }
    let file = opts.open(&db).expect("open db");
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
