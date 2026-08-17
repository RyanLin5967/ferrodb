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
//!
//! # E5 — resuming after a restart
//!
//! Progress is written to `<db>.replstate`. On a later start with no `backup-dir` the replica
//! resumes from that file rather than from scratch.
//!
//! **The ordering is the whole of the correctness argument, and it mirrors the primary\'s rule.**
//! The primary must never ship a record it has not durably written; the replica must never record
//! progress it has not durably applied. So each batch goes: apply, flush the pages, *then* write
//! the state file. A crash in that window leaves the state file BEHIND the pages, never ahead, and
//! re-applying is idempotent, so behind is repaired and ahead would be silent data loss — a replica
//! claiming an LSN whose pages never reached disk.
//!
//! `FERRODB_REPLICA_ABORT_AFTER_BATCHES=n` aborts the process after n applied batches, so a test
//! can kill it mid-stream at a known point rather than by racing it.

use std::io::{BufReader, Write};
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

    // Resume from where a previous run got to. Checked after the backup branch on purpose: a fresh
    // restore must start at the label, and a restart must not silently rewind to an older backup's
    // position.
    let state_path = format!("{db}.replstate");
    let mut resuming = false;
    if backup_dir.is_none() {
        if let Ok(text) = std::fs::read_to_string(&state_path) {
            if let Ok(lsn) = text.trim().parse::<u64>() {
                start_lsn = lsn;
                resuming = true;
                println!("RESUMED {lsn}");
            }
        }
    }

    // Single-writer lock, taken before the file is opened. Two processes on one database both build
    // an ArenaPageStore from the same checkpoint and hand the same pages to different branches, and
    // every such page still passes its checksum - so refusing here is the only detection point.
    //
    // Held for the whole run: `_db_lock` releases on the way out, including on an early return.
    let _db_lock = ferrodb::storage::db_lock::DbLock::acquire(std::path::Path::new(&db))
        .unwrap_or_else(|e| { eprintln!("repl_replica: {e}"); std::process::exit(1); });

    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true).create(true);
    // Truncate ONLY when there is genuinely nothing to keep.
    //
    // This condition used to read `backup_dir.is_none()`, which is a guard over the PATH TAKEN
    // rather than over the OUTCOME — the exact shape behind most of the defects in this repo. A
    // resume takes no backup directory either, so the restart wiped the very database it was
    // resuming and then died with `slot 14 is past this page's 0 slot(s)`. Asking "is there state
    // worth keeping" instead of "which argument did I get" cannot go wrong the same way.
    if backup_dir.is_none() && !resuming {
        opts.truncate(true);
    }

    let file = opts.open(&db).expect("open db");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let applier = ReplicaApplier::new(Arc::clone(&bp), start_lsn);

    // Record progress only after the pages it describes are durable. Never the other way round.
    let record = |lsn: u64| {
        use std::io::Write as _;
        let tmp = format!("{state_path}.tmp");
        let mut f = std::fs::File::create(&tmp).expect("create replstate");
        write!(f, "{lsn}").expect("write replstate");
        f.sync_all().expect("fsync replstate");
        // Rename so a reader never sees a half-written number.
        std::fs::rename(&tmp, &state_path).expect("rename replstate");
    };

    let abort_after: Option<u32> = std::env::var("FERRODB_REPLICA_ABORT_AFTER_BATCHES")
        .ok()
        .and_then(|s| s.parse().ok());
    let mut batches = 0u32;

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
                bp.flush_all().expect("flush replica pages");
                record(applier.applied_lsn());
                batches += 1;
                if Some(batches) == abort_after {
                    // Die the way a killed process dies: no unwinding, no flush, no goodbye.
                    println!("ABORTING after {batches} batches at {}", applier.applied_lsn());
                    std::io::stdout().flush().unwrap();
                    std::process::abort();
                }
            }
            Message::UpToDate { durable_lsn } => {
                bp.flush_all().expect("flush replica pages");
                record(applier.applied_lsn());
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
