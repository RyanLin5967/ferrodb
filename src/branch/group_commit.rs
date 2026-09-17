//! One fsync shared by every forker waiting on it.
//!
//! WHY. `TableBranchCatalog::fork` used to hold `self.logical` across `commit()`, and `commit()`
//! fsyncs. So concurrent forkers serialized on the mutex and each paid a private disk round-trip.
//! Measured, `bench/fork_concurrency_before.txt`: **274.9 forks/sec at 1 thread, 254.0 at 64** —
//! x0.92, flat, with the 64-thread arm slightly slower because of the convoy. Adding sixty-four
//! forkers bought nothing.
//!
//! This is **leader/follower group commit** and it is not novel: MySQL's binlog group commit
//! (Kristian Nielsen), RocksDB's write-batch leader, and the observation itself going back to
//! ARIES. It is named here because the standing rule in this project is that the known answer gets
//! reached for first and reported as engineering, not dressed up.
//!
//! ⛔ THE ORDERING IS THE WHOLE CORRECTNESS ARGUMENT, AND IT IS ONE LINE.
//! A ticket is taken **last, under the catalog's logical lock, after every mutation has landed in
//! the buffer pool**. That is what makes `requested == N` a statement that N operations' pages are
//! all present, so an fsync issued afterwards necessarily covers them. If a ticket were taken on
//! entry instead, `requested` could name work whose pages had not been written, the leader would
//! advance `durable` past it, and a caller would be told its fork was durable when a crash would
//! lose it. Everything else here is bookkeeping; this is the part to not get wrong.
//!
//! What must NOT move out of the lock: every B+tree mutation. `BPlusTreeManager` is not safe for
//! concurrent compound mutations, and a child that exists but is not listed in its parent is a GC
//! correctness hole. Only `flush_all + sync` is shareable.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

use crate::error::FerroError;

#[derive(Default)]
struct GroupState {
    /// Tickets handed out. Incremented under the caller's logical lock, after its mutations.
    requested: u64,
    /// Highest ticket covered by a COMPLETED, successful fsync.
    durable: u64,
    /// A leader is inside `sync()` right now. Followers wait rather than issuing a second one.
    syncing: bool,
}

/// Shared commit point for one catalog.
#[derive(Default)]
pub(crate) struct CommitGroup {
    state: Mutex<GroupState>,
    wake: Condvar,
    /// fsyncs actually issued. Not a statistic for its own sake: `forks / syncs` is the only
    /// direct evidence that batching is happening, and once the throughput curve plateaus it is
    /// what distinguishes "the fsync is still the bottleneck" from "the bottleneck moved".
    /// A counter, deliberately, rather than an env var that skips the fsync — a durability bypass
    /// living in production code is a footgun, and the question can be answered without one.
    syncs: AtomicU64,
}

impl CommitGroup {
    /// How many fsyncs this catalog has issued.
    pub(crate) fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// Take a ticket. **Call this under the logical lock, after the last mutation**, never on entry.
    pub(crate) fn ticket(&self) -> u64 {
        let mut st = self.state.lock().unwrap();
        st.requested += 1;
        st.requested
    }

    /// Block until a successful fsync has covered `seq`. **Call this after RELEASING the logical
    /// lock** — holding it here would reintroduce exactly the serialization this removes.
    ///
    /// Exactly one waiter becomes the leader and runs `sync`; the rest sleep on the condvar and
    /// wake up already durable. A leader whose sync FAILS notifies before propagating the error,
    /// so the followers re-evaluate and one of them retries rather than parking for ever.
    pub(crate) fn wait_durable<F>(&self, seq: u64, sync: F) -> Result<(), FerroError>
    where
        F: Fn() -> Result<(), FerroError>,
    {
        let mut st = self.state.lock().unwrap();
        loop {
            if st.durable >= seq {
                return Ok(());
            }
            if st.syncing {
                st = self.wake.wait(st).unwrap();
                continue;
            }
            st.syncing = true;
            // Read the target BEFORE unlocking. A ticket handed out while we are inside `sync()`
            // is NOT covered by it -- that forker's pages may not have been written when the sync
            // began -- so claiming it would be the unsafe direction. Reading it here means we only
            // ever claim work that was already staged, and a later ticket simply waits for the
            // next round.
            let target = st.requested;
            drop(st);

            self.syncs.fetch_add(1, Ordering::Relaxed);
            let result = sync();

            let mut done = self.state.lock().unwrap();
            done.syncing = false;
            match result {
                Ok(()) => {
                    if target > done.durable {
                        done.durable = target;
                    }
                    self.wake.notify_all();
                    st = done;
                }
                Err(e) => {
                    // Notify FIRST. A follower asleep on the condvar has no other way to learn the
                    // leader failed, and a sync that never happens is indistinguishable from one
                    // still running.
                    self.wake.notify_all();
                    return Err(e);
                }
            }
        }
    }
}
