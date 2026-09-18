//! Crash victim for the LAZY-DURABILITY falsifier. Not a benchmark.
//!
//! The claim under test (`SCALE-DESIGN.md` D6, option 2) is that a branch which never wrote need
//! never have been durable at all, so a speculative fork costs **no disk**. The sharp falsifier
//! the entry names is a `kill -9` between fork and first write: it must leave **nothing** — no
//! orphan id, no parked page, no index entry.
//!
//! So this victim forks a fixed count from trunk, **writes nothing**, prints every acknowledged
//! id, announces QUIESCED and then sleeps for ever. The test SIGKILLs it and requires the catalog
//! file to be byte-identical to what it was before the process started.
//!
//! ⛔ IT MUST GO QUIET, for the same reason `fork_kill9` must. Any later durable operation — by
//! this branch or any other — flushes the whole buffer pool, and would carry these forks to disk
//! as a side effect. Quiescence is what makes the exposed window the whole run rather than the
//! last few milliseconds of it.
//!
//! `fork_no_write_kill9 <catalog-path> <threads> <forks-per-thread>`
use std::io::Write;
use std::sync::Arc;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

fn main() {
    let path = std::env::args().nth(1).expect("catalog path");
    let threads: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    let per: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(64);

    let cat = Arc::new(
        TableBranchCatalog::open_sidecar(std::path::Path::new(&path), 1).expect("open catalog"),
    );
    let lease = LeaseDeadline(u64::MAX);

    let mut handles = Vec::new();
    for _ in 0..threads {
        let cat = Arc::clone(&cat);
        handles.push(std::thread::spawn(move || {
            for _ in 0..per {
                match cat.fork(BranchId::TRUNK, lease) {
                    // NOTHING is written against the child. This is the speculative agent fork:
                    // the branch is handed out and then abandoned.
                    Ok(child) => {
                        let mut out = std::io::stdout().lock();
                        let _ = writeln!(out, "{}", child.branch_id.id);
                        let _ = out.flush();
                    }
                    Err(e) => {
                        eprintln!("fork failed: {e}");
                        return;
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "QUIESCED");
        let _ = out.flush();
    }
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
