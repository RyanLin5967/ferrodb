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

/// **FORCED FIRE.** A chunk the state machine accepted and the driver cannot place is refused by
/// name, rather than written wherever the offset says.
///
/// This guard cannot fire in a healthy run, which is exactly why it needs to be made to. What it
/// prevents is invisible without it: the state machine digests the bytes it *saw* and the driver
/// installs the bytes it *wrote*, so a spool assembled from two different transfers passes the
/// payload's own digest — the state machine folded the same mixture — and every page of the
/// database it installs passes its own checksum. There is no later point at which that is
/// detectable.
#[test]
fn a_chunk_the_driver_cannot_place_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let mut n = Node::start(
        NodeId(1),
        Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0),
        listener(),
        NodeOptions::new(dir.path(), BTreeMap::new(), 1),
        RecordingApplier::default(),
    )
    .unwrap();

    let snap = multi_chunk_snapshot(4, 1);
    assert!(
        snap.meta.total_bytes > snap6::SNAPSHOT_CHUNK_BYTES as u64,
        "the fixture fits in one chunk, so there is no second chunk to misplace"
    );

    // Chunk 0 arrives and is accepted by both accounts.
    n.pending.push_back(Event::Recv(chunk_msg(&snap, 0)));
    n.drain().expect("the first chunk was refused");
    assert_eq!(n.spooled, snap6::SNAPSHOT_CHUNK_BYTES as u64);
    assert!(spool_path(&n.dir).exists(), "the accepted chunk was not spooled");

    // The two accounts drift. In the wild this is a bug in one of the two rules; here it is made
    // to happen, because a guard that has never fired is not a guard.
    n.spooled = 12_345;

    n.pending.push_back(Event::Recv(chunk_msg(&snap, snap6::SNAPSHOT_CHUNK_BYTES as u64)));
    let err = n.drain().expect_err(
        "the driver wrote a chunk it could not place. The spool is now a mixture of two accounts \
         of one transfer, and it will still pass the payload's digest.",
    );
    assert!(
        format!("{err}").contains("have drifted"),
        "refused, but not for the reason this guard exists: {err}"
    );

    // Anti-vacuity: with the accounts agreeing, the same chunk is accepted. Without this the test
    // above would pass just as well against a driver that refused every chunk.
    n.spooled = snap6::SNAPSHOT_CHUNK_BYTES as u64;
    n.pending.push_back(Event::Recv(chunk_msg(&snap, snap6::SNAPSHOT_CHUNK_BYTES as u64)));
    n.drain().expect("a chunk both accounts agree on was refused");
    assert_eq!(n.spooled, 2 * snap6::SNAPSHOT_CHUNK_BYTES as u64);

    n.shutdown();
}
