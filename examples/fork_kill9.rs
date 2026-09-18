//! Crash victim for the group-commit durability test. Not a benchmark.
//!
//! Group commit moved the fsync out from under the logical lock and made it SHARED. The risk it
//! introduces is precise: if a leader advances the durable watermark past work whose pages were
//! never written, a caller is told its fork succeeded and a crash loses it. That is worse than the
//! slow version it replaced, and no throughput number can detect it.
//!
//! So: fork in several threads, and print each branch id ONLY after `fork` returned Ok, flushing
//! immediately. Everything on stdout is therefore a fork the catalog ACKNOWLEDGED. The test kills
//! this process with SIGKILL, reopens the catalog, and requires every acknowledged id to be there.
//!
//! ⛔ AND IT MUST THEN GO QUIET. The first version of this victim forked in an infinite loop, and
//! the test could not detect a deliberately broken commit ordering in three runs: with a sync every
//! few milliseconds, a LATER group flushed the pages of almost every at-risk fork, so the bug was
//! real and invisible. Only forks acknowledged between the last flush and the kill are exposed.
//! So this forks a FIXED count and then sleeps forever, issuing no further syncs — nothing can
//! rescue the final group, which is precisely the group under test.
//!
//!   fork_kill9 <catalog-path> <threads> <forks-per-thread>
use std::io::Write;
use std::sync::Arc;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

fn main() {
    let path = std::env::args().nth(1).expect("catalog path");
    let threads: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    let per: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(64);

    let cat = Arc::new(TableBranchCatalog::open_sidecar(std::path::Path::new(&path), 1)
        .expect("open catalog"));
    let lease = LeaseDeadline(u64::MAX);

    let mut handles = Vec::new();
    for _ in 0..threads {
        let cat = Arc::clone(&cat);
        handles.push(std::thread::spawn(move || {
            for _ in 0..per {
                match cat.fork(BranchId::TRUNK, lease) {
                    Ok(child) => {
                        // ACKNOWLEDGED. Write it down before doing anything else, and flush, so
                        // the record of what was promised survives the kill even though the
                        // process does not. A buffered line would make the test unable to tell a
                        // lost fork from a lost printf.
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
    // DONE, and now deliberately idle. No further fsync will happen, so the last group's
    // acknowledgements are on their own. Announce it so the test kills at the right moment rather
    // than guessing from a timer.
    {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "QUIESCED");
        let _ = out.flush();
    }
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
