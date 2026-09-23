//! E6 — optional synchronous commit.
//!
//! Replication is asynchronous by default: the primary returns from commit as soon as its own WAL
//! is durable, and a replica catches up whenever it can. That is the right default, and it means a
//! crash can lose committed work no replica ever received.
//!
//! Synchronous commit changes the promise. With it on, commit does not return until at least one
//! replica has acknowledged the commit's LSN, so a crash of the primary cannot lose work a client
//! was told had committed.
//!
//! # No new protocol
//!
//! A replica already tells the primary where it is: every `Hello { from_lsn }` is an assertion that
//! everything below `from_lsn` has been applied *and flushed* — the replica writes its progress
//! only after the pages it describes are durable. So an ack is a message that already exists, and
//! [`AckTracker::record`] is fed from the primary's existing serve loop. Adding an `Ack` frame
//! would have been a second source of truth for the same fact, and the two would eventually
//! disagree.
//!
//! # The trade this makes, stated because it is not free
//!
//! **Synchronous commit with one replica and no consensus buys durability by spending
//! availability.** If the replica is slow, the primary is slow. If the replica is gone, the primary
//! cannot honour the promise at all, and there are exactly two things it can do:
//!
//! - **Block forever**, which converts a replica outage into a primary outage.
//! - **Give up and commit anyway**, which silently downgrades to asynchronous — the client was told
//!   its data was on two machines and it is on one.
//!
//! The second is worse, and it is worse in the specific way this project keeps arguing against: it
//! is a guarantee that reports success while not holding. So [`AckTracker::wait_for`] does neither
//! silently. It waits up to a caller-supplied deadline and then **returns an error naming what was
//! not achieved**, leaving the decision to escalate or degrade with the caller, who is the only one
//! who knows which the application can tolerate.
//!
//! PostgreSQL has this exact dilemma and resolves it the same way: `synchronous_commit = on` with a
//! single sync standby will block, and the documented remedy is operator action.
//!
//! **This is not consensus.** One ack from one replica is not a quorum, and a primary that cannot
//! reach its replica cannot tell "the replica is dead" from "the network is broken". Nothing here
//! makes it safe to promote a replica automatically.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::FerroError;

/// Tracks how far each replica has acknowledged, and lets a committer wait for a quorum of one.
#[derive(Default)]
/// What the primary knows about its replicas.
///
/// **Two questions, deliberately separated after D184.** `peers` answers "who is connected and
/// where are they" — a disconnect must change it. `retained` answers "what has been acknowledged"
/// — and a disconnect must NOT change that, because an acknowledgement means the data reached
/// that replica's disk and a replica going away does not un-write it. They lived in one map until
/// D184, so `forget` answered both the same way and a disconnect retracted an ack.
struct Acks {
    /// peer name -> highest LSN that peer has durably applied.
    peers: HashMap<String, u64>,
    /// Highest LSN ANY peer has ever acknowledged. Monotonic, and survives `forget`.
    retained: u64,
}

pub struct AckTracker {
    acks: Mutex<Acks>,
    changed: Condvar,
}

impl AckTracker {
    pub fn new() -> Self {
        Self { acks: Mutex::new(Acks { peers: HashMap::new(), retained: 0 }), changed: Condvar::new() }
    }

    /// Record a peer's position.
    ///
    /// Monotonic per peer: a report that goes backwards is ignored rather than trusted. A replica
    /// restarted from an older base backup would otherwise retract an acknowledgement the primary
    /// has already acted on, and a durability promise that can be withdrawn after the fact is not
    /// a promise. Going backwards is legitimate for the *replica* — it is how a resume works — but
    /// it must not un-commit anything here.
    pub fn record(&self, peer: &str, lsn: u64) {
        let mut acks = self.acks.lock().unwrap();
        let slot = acks.peers.entry(peer.to_string()).or_insert(0);
        if lsn > *slot {
            *slot = lsn;
            // The retained high-water mark rises with it and never falls. See [`Acks`].
            acks.retained = acks.retained.max(lsn);
            self.changed.notify_all();
        }
    }

    /// A peer has gone. Its ack no longer counts towards anything.
    ///
    /// **This deliberately does not shorten anyone's wait.** The first version of this woke
    /// waiters, on the reasoning that losing the only replica turns a "not yet" into a "not ever" —
    /// and a test caught that reasoning being wrong. A woken waiter recomputes the same maximum,
    /// finds it no better, and sleeps again, so the notify bought nothing; and failing fast on zero
    /// peers would be worse, because a replica reconnecting inside the deadline is exactly the case
    /// synchronous commit exists to ride out. Waiting out the deadline is the behaviour, and the
    /// deadline is the caller's to choose.
    /// ⚠ **D184: this removes the PEER, not its acknowledgement.** Until D184 it removed both,
    /// because one map answered both questions — so a replica that acked an LSN and then dropped
    /// retracted that ack, and a commit it had already satisfied was reported unacknowledged.
    /// That is the failure `record`'s own doc names one line up: *"a durability promise that can
    /// be withdrawn after the fact is not a promise"*. `record` guarded the going-backwards exit;
    /// this was the other exit into the same state. Pinned by
    /// `an_ack_survives_the_replica_disconnecting`.
    pub fn forget(&self, peer: &str) {
        self.acks.lock().unwrap().peers.remove(peer);
    }

    /// Highest LSN acknowledged by any peer, or 0 if there are none.
    pub fn max_acked(&self) -> u64 {
        self.acks.lock().unwrap().retained
    }

    /// Number of peers currently reporting.
    pub fn peer_count(&self) -> usize {
        self.acks.lock().unwrap().peers.len()
    }

    /// Block until some replica has acknowledged `lsn`, or the deadline passes.
    ///
    /// On timeout this returns an error rather than committing anyway. The message names the gap
    /// explicitly, because the operator's decision — escalate, or accept asynchronous durability —
    /// depends on how far behind the replica actually is, and "commit timed out" does not say.
    ///
    /// The condition is re-checked on every wake rather than assumed from the notify, so a spurious
    /// wakeup cannot be mistaken for an ack.
    pub fn wait_for(&self, lsn: u64, timeout: Duration) -> Result<(), FerroError> {
        let deadline = Instant::now() + timeout;
        let mut acks = self.acks.lock().unwrap();
        loop {
            let best = acks.retained;
            if best >= lsn {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(FerroError::Wal(format!(
                    "synchronous commit was not acknowledged: waited for lsn {lsn} but the furthest \
                     replica is at {best} ({} replica(s) connected). The commit is durable on the \
                     PRIMARY and is not confirmed on any replica, so this transaction has only \
                     asynchronous durability. Nothing was rolled back.",
                    acks.peers.len()
                )));
            }
            let (guard, _) = self.changed.wait_timeout(acks, deadline - now).unwrap();
            acks = guard;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn an_ack_that_reaches_the_target_releases_the_waiter() {
        let t = Arc::new(AckTracker::new());
        let t2 = Arc::clone(&t);
        let h = std::thread::spawn(move || t2.wait_for(500, Duration::from_secs(5)));

        // Below the target must NOT release it, or the test would pass against a `wait_for` that
        // returns on any ack at all.
        t.record("replica-1", 499);
        std::thread::sleep(Duration::from_millis(20));
        assert!(!h.is_finished(), "the waiter was released by an ack below its target");

        t.record("replica-1", 500);
        assert!(h.join().unwrap().is_ok(), "an ack at the target did not release the waiter");
    }

    /// An ack already recorded before the wait must satisfy it immediately.
    #[test]
    fn a_target_already_acked_does_not_wait() {
        let t = AckTracker::new();
        t.record("r", 900);
        let start = Instant::now();
        assert!(t.wait_for(800, Duration::from_secs(5)).is_ok());
        assert!(start.elapsed() < Duration::from_millis(500), "it waited despite already being acked");
    }

    /// **The trade, made explicit.** With no replica, commit does not silently succeed.
    #[test]
    fn with_no_replica_it_refuses_rather_than_degrading_silently() {
        let t = AckTracker::new();
        let err = t
            .wait_for(100, Duration::from_millis(50))
            .expect_err("synchronous commit reported success with no replica connected");
        let msg = format!("{err}");
        assert!(msg.contains("asynchronous durability"), "the message hides the downgrade: {msg}");
        assert!(msg.contains("Nothing was rolled back"), "it does not say what happened: {msg}");
        assert!(msg.contains("100"), "it does not say what was waited for: {msg}");
    }

    /// A replica that reports going backwards must not retract an acknowledgement.
    #[test]
    fn an_ack_never_goes_backwards() {
        let t = AckTracker::new();
        t.record("r", 1000);
        t.record("r", 10); // e.g. restarted from an older base backup
        assert_eq!(t.max_acked(), 1000, "a replica retracted an ack the primary may have acted on");
        assert!(t.wait_for(1000, Duration::from_millis(50)).is_ok());
    }

    /// The furthest replica decides, not the nearest.
    #[test]
    fn the_furthest_replica_satisfies_the_wait() {
        let t = AckTracker::new();
        t.record("slow", 10);
        t.record("fast", 2000);
        assert_eq!(t.max_acked(), 2000);
        assert!(t.wait_for(1500, Duration::from_millis(50)).is_ok());
        assert_eq!(t.peer_count(), 2);
    }

    /// **D184 — the MIRROR of `an_ack_never_goes_backwards`, aimed at the OTHER exit.**
    ///
    /// That test pins "a replica that reports going backwards must not retract an
    /// acknowledgement" using two `record` calls. But `record` is not the only way an ack can
    /// leave this map: `forget` removes the peer outright, and `wait_for` recomputes `best` from
    /// whatever is in the map at that moment. So the same invariant has a second exit, and only
    /// one of them is guarded.
    ///
    /// **This is the invariant under test, stated as a promise rather than as code:** an
    /// acknowledgement means the data is on that replica's disk. A replica going away does not
    /// un-write it. So an ack that has already been given must keep counting, and a commit it
    /// already satisfied must not later be reported as unacknowledged.
    ///
    /// ⚠ **PRE-REGISTERED BEFORE RUNNING, from the source and not from the run:** `forget` is
    /// `self.acks.lock().unwrap().remove(peer)`, so `max_acked()` should fall to 0 and this test
    /// should FAIL. If it passes, my reading of `forget` is wrong and this comment is the thing to
    /// correct.
    #[test]
    fn an_ack_survives_the_replica_disconnecting() {
        let t = AckTracker::new();
        t.record("r", 1000);
        assert_eq!(t.max_acked(), 1000, "fixture: the ack was never recorded");

        t.forget("r");

        // The peer really is gone -- that half is correct and is not what is under test.
        assert_eq!(t.peer_count(), 0, "fixture: the peer was not forgotten");

        assert_eq!(
            t.max_acked(),
            1000,
            "a disconnect retracted an acknowledgement the primary may have already acted on: the \
             data is on that replica's disk and going away does not un-write it"
        );
        assert!(
            t.wait_for(1000, Duration::from_millis(50)).is_ok(),
            "a commit that was already satisfied is now reported unacknowledged because its \
             replica disconnected afterwards"
        );
    }

    /// **A replica that drops and reconnects inside the deadline still satisfies the commit.**
    ///
    /// This test replaced one asserting the opposite. That one expected losing the last replica to
    /// end the wait early, and it failed — correctly. Waking a waiter teaches it nothing, because
    /// it recomputes the same maximum and sleeps again; and ending the wait early would abandon a
    /// commit that a reconnect one millisecond later would have satisfied. Riding out a brief
    /// outage is the entire value of a deadline, so the deadline is what decides.
    #[test]
    fn a_replica_that_reconnects_inside_the_deadline_still_satisfies_the_commit() {
        let t = Arc::new(AckTracker::new());
        t.record("only", 5);
        let t2 = Arc::clone(&t);
        let h = std::thread::spawn(move || t2.wait_for(100, Duration::from_secs(10)));

        // It goes away entirely...
        t.forget("only");
        assert_eq!(t.peer_count(), 0, "the peer was not forgotten");
        std::thread::sleep(Duration::from_millis(30));
        assert!(!h.is_finished(), "the commit gave up while its replica was merely absent");

        // ...and comes back, having caught up. The commit must be honoured, not abandoned.
        t.record("only", 100);
        assert!(
            h.join().unwrap().is_ok(),
            "a replica reconnected and acked inside the deadline, and the commit still failed"
        );
    }
}
