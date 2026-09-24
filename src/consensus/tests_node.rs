//! Tests for the driver. These cover what `node.rs` itself decides — the hard-state record, the
//! restore path, and the tick clock. What a *cluster* does is
//! `tests/integration_consensus_failover.rs`, which runs three real processes and kills one.

use super::*;
use crate::consensus::config::Config;
use std::net::TcpListener;

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").expect("an ephemeral port")
}

fn cmd(n: u64) -> Command {
    Command::WalBatch { start_lsn: n, bytes: vec![n as u8; 4] }
}

// ---------------------------------------------------------------- the hard-state record

#[test]
fn the_hard_state_record_round_trips_through_a_file() {
    let dir = tempfile::tempdir().unwrap();
    for h in [
        HardState::default(),
        HardState { term: 1, voted_for: None },
        HardState { term: 9_876_543_210, voted_for: Some(NodeId(7)) },
        HardState { term: u64::MAX, voted_for: Some(NodeId(u32::MAX)) },
    ] {
        store_hard_state(dir.path(), &h).unwrap();
        assert_eq!(load_hard_state(dir.path()).unwrap(), h);
    }
}

#[test]
fn a_missing_hard_state_record_is_a_fresh_node_not_an_error() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(load_hard_state(dir.path()).unwrap(), HardState::default());
}

#[test]
fn a_corrupt_hard_state_record_is_refused_rather_than_guessed() {
    // FORCED FIRE. The point of the checksum is that a torn record must not read as "term 0, voted
    // for nobody" — that answer is the amnesia the record exists to prevent, and it is also what a
    // trusting parse returns. So the corruption is made on purpose, byte by byte, and every one
    // must be refused.
    let dir = tempfile::tempdir().unwrap();
    let h = HardState { term: 42, voted_for: Some(NodeId(3)) };
    store_hard_state(dir.path(), &h).unwrap();
    let good = std::fs::read(dir.path().join("hardstate")).unwrap();
    assert_eq!(good.len(), HARD_LEN);

    for i in 0..good.len() {
        let mut bad = good.clone();
        bad[i] ^= 0xFF;
        assert!(
            hard_decode(&bad).is_err(),
            "flipping byte {i} of the hard-state record was accepted; a corrupt record that \
             decodes is a node that has silently forgotten its vote"
        );
    }
    // Anti-vacuity: the untouched record must still decode, or the loop above proves nothing.
    assert_eq!(hard_decode(&good).unwrap(), h);
}

#[test]
fn a_truncated_hard_state_record_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    store_hard_state(dir.path(), &HardState { term: 5, voted_for: Some(NodeId(1)) }).unwrap();
    let good = std::fs::read(dir.path().join("hardstate")).unwrap();
    for n in 0..good.len() {
        assert!(hard_decode(&good[..n]).is_err(), "a {n}-byte record decoded");
    }
}

// ---------------------------------------------------------------- restore

#[test]
fn a_restarted_node_remembers_its_term_and_its_vote() {
    // The whole reason `PersistHardState` is a separate action: a node that votes, crashes, and
    // comes back having forgotten can vote twice in one term, electing two leaders of that term.
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0);

    {
        let mut n = Node::start(
            NodeId(1),
            cfg.clone(),
            listener(),
            NodeOptions::new(dir.path(), BTreeMap::new(), 1),
            RecordingApplier::default(),
        )
        .unwrap();
        n.perform(Action::PersistHardState { term: 4, voted_for: Some(NodeId(2)) }).unwrap();
        n.shutdown();
    }

    let n = Node::start(
        NodeId(1),
        cfg,
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1),
        RecordingApplier::default(),
    )
    .unwrap();
    assert_eq!(n.term(), 4, "the restarted node forgot the term it had voted in");
    assert_eq!(n.sm.hard.voted_for, Some(NodeId(2)), "the restarted node forgot who it voted for");
    n.shutdown();
}

#[test]
fn a_restarted_node_gets_its_round_log_back() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0);
    let entries: Vec<Entry> =
        (1..=5).map(|r| Entry { term: 2, round: r, command: cmd(r) }).collect();

    {
        let mut n = Node::start(
            NodeId(1),
            cfg.clone(),
            listener(),
            NodeOptions::new(dir.path(), BTreeMap::new(), 1),
            RecordingApplier::default(),
        )
        .unwrap();
        n.perform(Action::Persist { entries: entries.clone() }).unwrap();
        assert_eq!(n.last_round(), 5);
        n.shutdown();
    }

    let n = Node::start(
        NodeId(1),
        cfg,
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1),
        RecordingApplier::default(),
    )
    .unwrap();
    assert_eq!(n.last_round(), 5, "the durable log did not survive the restart");
    // The state machine's own view, not just the file's: a node whose log is on disk but not in
    // `LogTail` answers `term_at` with None and refuses every append for ever.
    assert_eq!(n.sm.last_round, 5, "the state machine did not get the log back");
    assert_eq!(n.sm.last_term, 2);
    for e in &entries {
        assert_eq!(n.sm.term_at(e.round), Some(e.term), "round {} is missing", e.round);
    }
    // A local disk cannot establish that a quorum held these rounds, so commit stays at zero.
    // Restoring a commit index from local disk is how a node applies a round the cluster
    // afterwards truncated.
    assert_eq!(n.commit_round(), 0, "commit was restored from local disk, which no disk can know");
    n.shutdown();
}

// ---------------------------------------------------------------- the actions

#[test]
fn persist_makes_the_entries_durable_before_it_reports_them() {
    // `Event::Persisted` is queued by `perform`, and the assertion is that the bytes are already
    // readable from a *separate* handle on the same directory by then. An ack for a round that is
    // not on this node's disk turns a correlated power loss into acknowledged data loss.
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1),
        RecordingApplier::default(),
    )
    .unwrap();

    n.perform(Action::Persist { entries: vec![Entry { term: 1, round: 1, command: cmd(1) }] })
        .unwrap();

    assert!(
        matches!(n.pending.front(), Some(Event::Persisted { round: 1, .. })),
        "Persist did not feed Persisted back"
    );
    let reread = crate::consensus::log::RoundLog::open(dir.path()).unwrap();
    assert_eq!(reread.last_round(), 1, "the entry was reported persisted but is not on disk");
    n.shutdown();
}

#[test]
fn apply_hands_every_committed_round_to_the_applier_in_order_and_never_skips() {
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1),
        RecordingApplier::default(),
    )
    .unwrap();

    let entries: Vec<Entry> =
        (1..=4).map(|r| Entry { term: 1, round: r, command: cmd(r) }).collect();
    n.perform(Action::Persist { entries }).unwrap();
    n.pending.clear();

    n.perform(Action::Apply { through: 3 }).unwrap();
    assert_eq!(n.applied(), 3);
    assert_eq!(
        n.applier().applied.iter().map(|e| e.round).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the applier did not receive every round from 1 through 3, in order"
    );

    // Idempotent below the high-water mark: a repeated Apply for a round already applied must not
    // deliver it twice.
    n.perform(Action::Apply { through: 2 }).unwrap();
    assert_eq!(n.applier().applied.len(), 3, "a re-Apply below the high-water mark re-delivered");

    n.perform(Action::Apply { through: 4 }).unwrap();
    assert_eq!(n.applied(), 4);
    n.shutdown();
}

#[test]
fn a_tick_is_delivered_on_the_clock_and_missed_ticks_are_caught_up() {
    // A driver that delivers one tick per poll makes every timeout in the cluster a function of
    // how often the caller polls, so a busy node's leader is declared dead by everyone else while
    // it believes it is fine.
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1).tick_of(Duration::from_millis(1)),
        RecordingApplier::default(),
    )
    .unwrap();

    // Fall behind on purpose by 20 ticks, then poll once.
    n.next_tick = Instant::now() - Duration::from_millis(20);
    let before = n.sm.since_heard;
    n.poll(Duration::ZERO).unwrap();
    assert!(
        n.sm.since_heard >= before + 20 || n.role() != Role::Follower,
        "20 whole ticks elapsed but the state machine advanced from {before} to {}; a driver that \
         drops missed ticks silently slows every timeout in the cluster",
        n.sm.since_heard
    );
    n.shutdown();
}

#[test]
fn a_proposal_to_a_follower_is_refused_rather_than_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1),
        RecordingApplier::default(),
    )
    .unwrap();
    assert_eq!(n.role(), Role::Follower);
    n.propose(cmd(1)).unwrap();
    assert!(
        !n.take_refusals().is_empty(),
        "a follower accepted a proposal silently; a client cannot tell a dropped write from a \
         committed one"
    );
    n.shutdown();
}

use crate::consensus::snapshot as snap6;
use crate::consensus::{Body, Message};

// ---------------------------------------------------------------- F6: the driver's own account

/// A `Snapshot` big enough to need more than one chunk, so a second chunk has somewhere to go
/// wrong. Two pages of arena and branch bytes are enough to make the payloads distinguishable.
fn multi_chunk_snapshot(round: Round, term: Term) -> snap6::Snapshot {
    use crate::replication::backup::BackupLabel;
    let pages = 512u32;
    snap6::Snapshot::build(
        &snap6::SnapshotPoint {
            last_round: round,
            last_term: term,
            config: Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
            base_digest: 0,
        },
        1,
        1,
        BackupLabel { start_lsn: 1, end_lsn: 2, page_count: pages },
        &[7u8; 8],
        &[7u8; 4],
        &[],
        vec![7u8; pages as usize * crate::storage::disk_manager::PAGE_SIZE],
    )
    .expect("a well-formed payload was refused")
}

fn chunk_msg(snap: &snap6::Snapshot, offset: u64) -> Message {
    let bytes = snap.payload();
    let from = offset as usize;
    let to = (from + snap6::SNAPSHOT_CHUNK_BYTES).min(bytes.len());
    Message {
        from: NodeId(2),
        to: NodeId(1),
        term: 1,
        body: Body::InstallSnapshot {
            meta: snap.meta.clone(),
            offset,
            data: bytes[from..to].to_vec(),
            done: to == bytes.len(),
        },
    }
}

/// **FORCED FIRE.** A spool that is short of what the state machine digested is refused by name,
/// at the install, rather than installed.
///
/// This is the reconciliation that cannot happen in a healthy run, which is exactly why it needs to
/// be made to. The state machine digests the bytes it *saw* and this driver installs the bytes it
/// *wrote*; if a chunk the state machine accepted was one this driver could not place, the spool is
/// short — and a database assembled from a short spool would be installed with nothing to say
/// otherwise. Every page of the result passes its own checksum.
///
/// The check is at the install and not at the chunk, and that too is a correction: the first
/// version refused at the chunk, and a chunk re-sent by a leader on its next heartbeat — which
/// leaves the receive cursor at exactly the value an accept produces — took the whole node down.
#[test]
fn a_spool_short_of_what_was_digested_is_refused_rather_than_installed() {
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1)
            .with_snapshots(Box::new(CountingStore::default()), 0),
        RecordingApplier::default(),
    )
    .unwrap();

    let snap = multi_chunk_snapshot(4, 1);
    assert!(
        snap.meta.total_bytes > 2 * snap6::SNAPSHOT_CHUNK_BYTES as u64,
        "the fixture needs at least three chunks so one can be lost from the middle"
    );

    // Chunk 0: both accounts agree.
    n.pending.push_back(Event::Recv(chunk_msg(&snap, 0)));
    n.drain().expect("the first chunk was refused");
    let after_first = n.spooled;

    // Chunk 1: the driver's account is moved out from under it, so it cannot place the chunk. The
    // state machine still accepts and digests it. **This is not an error** — a chunk the driver
    // cannot place is indistinguishable from a duplicate the state machine refused, and taking the
    // node down for one of those was the defect.
    n.spooled = u64::MAX;
    n.pending.push_back(Event::Recv(chunk_msg(&snap, after_first)));
    n.drain().expect("a chunk the driver could not place took the node down");
    n.spooled = after_first; // the driver believes it is still where it was

    // The rest of the transfer, which the driver places normally, until the payload completes.
    let mut offset = n.sm.snapshot_incoming().expect("still in flight").received();
    let err = loop {
        n.pending.push_back(Event::Recv(chunk_msg(&snap, offset)));
        match n.drain() {
            Ok(()) => match n.sm.snapshot_incoming() {
                Some(c) if !c.is_complete() => offset = c.received(),
                _ => panic!("the transfer completed and a short spool was installed"),
            },
            Err(e) => break e,
        }
    };
    assert!(
        format!("{err}").contains("have drifted"),
        "refused, but not for the reason this guard exists: {err}"
    );
    assert_eq!(n.snapshots_installed(), 0, "a short spool was installed");
    n.shutdown();
}

/// Anti-vacuity for the guard above: the same transfer, with the driver keeping up, installs.
///
/// Without this the test above would pass just as well against a driver that refused every install.
#[test]
fn a_spool_that_kept_up_installs_rather_than_being_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1)
            .with_snapshots(Box::new(CountingStore::default()), 0),
        RecordingApplier::default(),
    )
    .unwrap();

    let snap = multi_chunk_snapshot(4, 1);
    let mut offset = 0u64;
    loop {
        n.pending.push_back(Event::Recv(chunk_msg(&snap, offset)));
        n.drain().expect("an ordinary chunk was refused");
        match n.sm.snapshot_incoming() {
            Some(c) => offset = c.received(),
            // The cursor is gone, which on this path means the install completed and was accepted.
            None => break,
        }
    }
    assert_eq!(n.snapshots_installed(), 1, "the transfer never installed");
    assert_eq!(n.snapshot_round(), 4, "the floor did not move to the installed round");
    assert!(
        !spool_path(&n.dir).exists(),
        "the spool survived an install the state machine accepted"
    );
    n.shutdown();
}

/// A store that accepts anything and reads the spool it is given, so a short one fails here too
/// rather than silently succeeding. What a REAL install does to real pages is
/// `tests/integration_cluster_snapshot.rs`.
#[derive(Default)]
struct CountingStore {
    installs: usize,
}

impl snap6::SnapshotStore for CountingStore {
    fn capture(&mut self, _at: &snap6::SnapshotPoint) -> Result<snap6::Snapshot, FerroError> {
        Err(FerroError::Internal("this store captures nothing".into()))
    }
    fn install(&mut self, _meta: &snap6::SnapshotMeta, path: &std::path::Path) -> Result<(), FerroError> {
        let bytes = std::fs::read(path).map_err(|e| FerroError::Io(e.to_string()))?;
        snap6::PayloadHeader::decode(&bytes).map_err(|e| e.into_ferro())?;
        self.installs += 1;
        Ok(())
    }
}

// ---------------------------------------------------------------- D73: the pumped clock

/// The wall clock's test above falls 20 ticks behind and requires all 20. This one falls behind by
/// the same amount and requires exactly one tick per poll: under `Clock::Pumped` nothing a harness
/// does between turns is time, and a single catch-up read of `Instant` would make it so again.
#[test]
fn a_pumped_node_takes_one_tick_per_poll_however_far_behind_the_wall_says_it_is() {
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1).tick_of(Duration::from_millis(1)).pumped(),
        RecordingApplier::default(),
    )
    .unwrap();

    n.next_tick = Instant::now() - Duration::from_millis(20);
    // Three polls, all below the shortest election timeout a node can draw (10 ticks), so a
    // follower that took one tick per poll is still a follower and its countdown moved by one.
    for k in 1..=3 {
        let before = n.sm.since_heard;
        n.poll(Duration::ZERO).unwrap();
        assert_eq!(
            (n.sm.since_heard, n.role()),
            (before + 1, Role::Follower),
            "poll {k} of a pumped node moved its election countdown from {before} to {} (role {}); \
             one poll is one tick, and anything more is the wall clock leaking back in",
            n.sm.since_heard,
            n.role()
        );
    }
    n.shutdown();
}

/// Wait until `n`'s transport has counted `k` frames received. The order under test is fixed by
/// these waits, not by timing; the bound is only there so a starved machine fails by name.
fn await_received<A: Applier>(n: &Node<A>, k: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while n.frames().received < k {
        assert!(
            Instant::now() < deadline,
            "the transport counted {} of {k} frames within 20 s",
            n.frames().received
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// **FORCED FIRE for the sort in `collect`.** The frame from node 3 is made to arrive first — its
/// arrival is waited for before node 2's is sent — and the queue must still hold node 2's first.
///
/// Arrival order across two peers is the scheduler's, so a `collect` that kept it would make a
/// fleet's replay of one election depend on which socket thread ran first. Forcing the order that
/// the sort must undo is what makes this able to fail: an arrival order that happened to match
/// sender order would pass against a `collect` with no sort at all.
#[test]
fn a_collected_turn_is_ordered_by_sender_not_by_arrival() {
    let dir = tempfile::tempdir().unwrap();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
        l,
        NodeOptions::new(dir.path(), BTreeMap::new(), 1).pumped(),
        RecordingApplier::default(),
    )
    .unwrap();
    let peer = |id: u32| {
        Transport::bind(
            NodeId(id),
            "127.0.0.1:0",
            BTreeMap::from([(NodeId(1), addr)]),
            TransportOptions::default(),
        )
        .expect("a peer transport")
    };
    let (t2, t3) = (peer(2), peer(3));
    let hello = |id: u32| Message {
        from: NodeId(id),
        to: NodeId(1),
        term: 0,
        body: Body::PreVoteResp { granted: false },
    };

    t3.send(&hello(3)).unwrap();
    await_received(&n, 1);
    t2.send(&hello(2)).unwrap();
    await_received(&n, 2);

    assert_eq!(n.collect(Duration::from_secs(20)).unwrap(), 2, "collect did not take both frames");
    let senders: Vec<NodeId> = n
        .pending
        .iter()
        .map(|e| match e {
            Event::Recv(m) => m.from,
            other => panic!("collect queued something that is not a message: {other:?}"),
        })
        .collect();
    assert_eq!(
        senders,
        vec![NodeId(2), NodeId(3)],
        "node 3's frame arrived first and was queued first; `collect` must order a turn by sender, \
         or a fleet's replay depends on which socket thread the scheduler ran first"
    );
    t2.shutdown();
    t3.shutdown();
    n.shutdown();
}

/// `collect` on a wall-clock node would be a second reader of an inbox `poll` already reads, and
/// each would hold part of a turn. Refused, with the reason.
#[test]
fn collect_is_refused_on_a_wall_clock_node() {
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1),
        RecordingApplier::default(),
    )
    .unwrap();
    let e = n.collect(Duration::ZERO).expect_err("a wall-clock node accepted `collect`");
    assert!(format!("{e}").contains("wall clock"), "refused, but not for this reason: {e}");
    n.shutdown();
}

/// Under the pumped clock the transport's idle close is off. How long a connection has been quiet
/// is wall time, and a turn-driven cluster leaves connections quiet for as long as its test likes;
/// closed, the next frame written into one dies uncounted and the harness refuses the turn.
///
/// **The wall-clock node is the control, and it runs first.** Same 50 ms deadline, same pause — and
/// its connection IS closed, so the pause is long enough and the pumped node's open connection is
/// the override, not the timing. Without the control this would pass against a transport whose
/// idle close never fired at all.
#[test]
fn a_pumped_node_does_not_close_a_connection_for_being_quiet() {
    for pumped in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let l = listener();
        let addr = l.local_addr().unwrap();
        let mut opts = NodeOptions::new(dir.path(), BTreeMap::new(), 1);
        opts.transport.idle_deadline = Duration::from_millis(50);
        if pumped {
            opts = opts.pumped();
        }
        let n = Node::start(
            NodeId(1),
            Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
            l,
            opts,
            RecordingApplier::default(),
        )
        .unwrap();
        let t2 = Transport::bind(
            NodeId(2),
            "127.0.0.1:0",
            BTreeMap::from([(NodeId(1), addr)]),
            TransportOptions::default(),
        )
        .expect("a peer transport");
        let hello = Message {
            from: NodeId(2),
            to: NodeId(1),
            term: 0,
            body: Body::PreVoteResp { granted: false },
        };

        t2.send(&hello).unwrap();
        await_received(&n, 1);
        // Twenty idle deadlines of silence.
        std::thread::sleep(Duration::from_secs(1));

        if pumped {
            assert_eq!(
                n.net.idle_closed(),
                0,
                "a pumped node closed a connection for a second of wall-clock silence"
            );
            t2.send(&hello).unwrap();
            await_received(&n, 2);
        } else {
            let deadline = Instant::now() + Duration::from_secs(20);
            while n.net.idle_closed() == 0 {
                assert!(
                    Instant::now() < deadline,
                    "the control did not fire: a wall-clock node with a 50 ms idle deadline kept a \
                     quiet connection open, so the pumped arm's open connection proves nothing"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        t2.shutdown();
        n.shutdown();
    }
}
