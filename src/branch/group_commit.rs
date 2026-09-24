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
use std::time::Duration;

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
                // BOUNDED, not `wait()`. A follower has exactly one way to learn anything: the
                // leader's notify. If that notify is ever missed -- a lost wakeup, a leader killed
                // between its sync and its notify, or a future edit that returns early -- an
                // unbounded wait parks this thread for the life of the process, and a database that
                // hangs forever is worse than one that does an extra fsync. On timeout the loop
                // re-checks `durable` and, if no leader is running, becomes one. A spurious
                // re-check costs at most one redundant sync.
                //
                // It is also what makes the failure TESTABLE: with `syncing` deliberately left set
                // on a failed sync, the unit test below fails in milliseconds instead of hanging,
                // and a guard that detects a bug by deadlocking is a bad guard in CI.
                let (guard, _timeout) = self.wake.wait_timeout(st, Duration::from_millis(50)).unwrap();
                st = guard;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::Duration;

    /// The rule that makes group commit safe: **a sync claims only the tickets that existed when it
    /// STARTED.** A ticket handed out while the leader is already inside `sync()` belongs to work
    /// whose pages may not have been written when the flush began, so claiming it would acknowledge
    /// a write that a crash could lose.
    ///
    /// This is tested here, deterministically, and NOT through the `kill -9` integration test.
    /// That test was tried against the corresponding mutant (ticket taken before the mutations
    /// instead of after) and **passed eight runs out of eight**: the race window is sub-millisecond
    /// and depends on buffer-pool iteration order, so a crash test cannot discriminate it. Testing
    /// the invariant where it is actually expressible is the difference between a guard and a
    /// reassuring green line.
    #[test]
    fn a_ticket_taken_during_a_sync_is_not_claimed_by_that_sync() {
        let g = Arc::new(CommitGroup::default());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));

        // Leader takes ticket 1 and blocks INSIDE the sync.
        let seq1 = g.ticket();
        assert_eq!(seq1, 1);
        let leader = {
            let g = Arc::clone(&g);
            let rx = Arc::clone(&release_rx);
            std::thread::spawn(move || {
                g.wait_durable(seq1, || {
                    entered_tx.send(()).unwrap();
                    let _ = rx.lock().unwrap().recv_timeout(Duration::from_secs(5));
                    Ok(())
                })
            })
        };
        entered_rx.recv_timeout(Duration::from_secs(5)).expect("leader entered sync");

        // Ticket 2 is taken WHILE that sync is running. Its pages are not covered by it.
        let seq2 = g.ticket();
        assert_eq!(seq2, 2);
        let follower = {
            let g = Arc::clone(&g);
            std::thread::spawn(move || g.wait_durable(seq2, || Ok(())))
        };

        release_tx.send(()).unwrap();
        leader.join().unwrap().expect("leader sync ok");
        follower.join().unwrap().expect("follower sync ok");

        // TWO syncs must have happened. If the first had claimed ticket 2, the follower would have
        // returned on that watermark and this would be 1 -- which is exactly the bug.
        assert_eq!(
            g.syncs(),
            2,
            "a ticket taken during a sync was claimed by it: the follower was told its write was \
             durable on the strength of a flush that began before the write existed"
        );
    }

    /// A failed sync must leave the group USABLE: `syncing` cleared, so the next caller can become
    /// leader and retry. Otherwise every later writer waits on a sync that will never happen.
    ///
    /// ⛔ THE TIMEOUT IS IN THE TEST, NOT IN THE PRODUCTION CODE, AND THAT IS DELIBERATE. The
    /// mutant here (clear `syncing` only on success) makes the retry spin rather than return, so
    /// the natural test HANGS -- and a guard that detects a bug by deadlocking is a bad guard,
    /// because in CI it burns the job timeout and reports nothing. The obvious production fix,
    /// letting a waiter steal leadership after a while, was REJECTED: the only way `syncing` stays
    /// set is a code bug (a panicking leader poisons the mutex instead), so that would be
    /// production complexity added to make a mutant convenient to test, which is the wrong trade.
    /// Bounding it here gets the fast failure without paying for it in the write path.
    #[test]
    fn a_failed_sync_leaves_the_group_usable() {
        let g = Arc::new(CommitGroup::default());
        let seq = g.ticket();
        let first = g.wait_durable(seq, || Err(FerroError::Branch("disk on fire".into())));
        assert!(first.is_err(), "a failing sync must propagate");

        let (tx, rx) = mpsc::channel();
        {
            let g = Arc::clone(&g);
            std::thread::spawn(move || {
                let r = g.wait_durable(seq, || Ok(()));
                let _ = tx.send(r.is_ok());
            });
        }
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(true) => {}
            Ok(false) => panic!("the retry became leader but its sync failed"),
            Err(_) => panic!(
                "a retry after a failed sync never completed: `syncing` was left set, so every \
                 subsequent writer waits on a sync that will never happen"
            ),
        }
        assert_eq!(g.syncs(), 2, "the retry must actually issue a second sync");
    }
}
