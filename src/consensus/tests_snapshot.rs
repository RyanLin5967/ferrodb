//! F6 — the state-transfer rules, each named after the rule it pins and the loss it prevents.
//!
//! Every test here was run against a deliberately broken copy of the rule it names before it was
//! believed; the mutants and what each printed are in `scratchpad/F6-mutants.txt`. A test that has
//! not been seen to fail is not evidence.
//!
//! Nothing here touches a disk. The protocol half of F6 is a pure function of its events, which is
//! the whole reason it can be tested at all — the driver's half (`node.rs`) and the storage half
//! (`PageStoreSnapshots`) are proven against a real filesystem and three real sockets in
//! `tests/integration_cluster_snapshot.rs`.

use super::*;
use crate::consensus::{Command, Entry, Event, HardState};
use crate::replication::backup::BackupLabel;
use crate::storage::disk_manager::PAGE_SIZE;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

fn cfg3() -> Config {
    Config::new([N1, N2, N3], 1, 1)
}

fn wal(mark: u8) -> Command {
    Command::WalBatch { start_lsn: 100 + mark as u64, bytes: vec![mark; 16] }
}

// ---------------------------------------------------------------------------------------------
// Harness. These build states the rest of `consensus/` would build; none computes an expected
// value by asking the code under test for it.
// ---------------------------------------------------------------------------------------------

/// Put `entries` into a node's log as rounds 1..n and mark them durable and applied, as a node that
/// has been running would hold them.
fn seed(c: &mut Consensus, entries: &[(Term, Command)]) {
    let restore = c.hard.term;
    let mut sink = Vec::new();
    for (t, cmd) in entries {
        c.hard.term = *t;
        c.append_own_entry(cmd.clone(), &mut sink);
    }
    c.hard.term = restore.max(c.last_term);
    c.durable = c.last_round;
    c.commit = c.last_round;
    c.applied = c.last_round;
}

fn promote(c: &mut Consensus, term: Term) {
    c.hard.term = term;
    c.hard.voted_for = Some(c.id());
    c.role = Role::Leader;
    c.leader = Some(c.id());
    c.init_leader_progress();
}

fn follower_of(c: &mut Consensus, term: Term, leader: NodeId) {
    c.hard.term = term;
    c.role = Role::Follower;
    c.leader = Some(leader);
}

/// A payload of `pages` pages, with a body whose bytes are a function of `mark`.
fn payload_of(at: &SnapshotPoint, pages: u32, mark: u8) -> Snapshot {
    Snapshot::build(
        at,
        7,
        1,
        BackupLabel { start_lsn: 10, end_lsn: 20, page_count: pages },
        &[mark; 8],
        &[mark; 4],
        vec![mark; pages as usize * PAGE_SIZE],
    )
    .expect("a well-formed payload was refused")
}

/// The point a leader with `entries` would snapshot at, taken from the state machine rather than
/// restated — a test that restates it cannot catch the state machine naming the wrong round.
fn point_of(c: &Consensus) -> SnapshotPoint {
    c.snapshot_point().expect("a node with applied rounds has a snapshot point")
}

fn install_msg(from: NodeId, to: NodeId, term: Term, snap: &Snapshot, offset: u64) -> Message {
    let bytes = snap.payload();
    let from_i = offset as usize;
    let to_i = (from_i + SNAPSHOT_CHUNK_BYTES).min(bytes.len());
    Message {
        from,
        to,
        term,
        body: Body::InstallSnapshot {
            meta: snap.meta.clone(),
            offset,
            data: bytes[from_i..to_i].to_vec(),
            done: to_i == bytes.len(),
        },
    }
}

fn sends(out: &[Action]) -> Vec<Message> {
    out.iter()
        .filter_map(|a| match a {
            Action::Send(m) => Some(m.clone()),
            _ => None,
        })
        .collect()
}

fn only_send(out: &[Action]) -> Message {
    let s = sends(out);
    assert_eq!(s.len(), 1, "expected exactly one message, got {s:#?}");
    s.into_iter().next().unwrap()
}

fn received_through(m: &Message) -> u64 {
    match &m.body {
        Body::InstallSnapshotResp { received_through } => *received_through,
        other => panic!("expected an InstallSnapshotResp, got {other:?}"),
    }
}

fn installed(m: &Message) -> &SnapshotMeta {
    match &m.body {
        Body::InstallSnapshot { meta, .. } => meta,
        other => panic!("expected an InstallSnapshot, got {other:?}"),
    }
}

/// Deliver a whole snapshot to `c`, chunk by chunk, and return every action it produced.
///
/// **Bounded, and the bound is a rule rather than a convenience.** A transfer that cannot converge
/// is a real failure mode of this protocol — a receiver that keeps answering the same resume point
/// while the sender keeps re-sending the same chunk makes no progress and never errors — and an
/// unbounded loop here turns that failure into a hung test, which reads like an environment
/// problem. The budget is far above the chunk count of any fixture in this file.
fn deliver_all(c: &mut Consensus, from: NodeId, term: Term, snap: &Snapshot) -> Vec<Action> {
    let budget = (snap.meta.total_bytes / SNAPSHOT_CHUNK_BYTES as u64 + 8) as usize;
    let mut out = Vec::new();
    let mut offset = 0u64;
    for step in 0..=budget {
        if offset >= snap.meta.total_bytes {
            return out;
        }
        assert!(
            step < budget,
            "the transfer did not converge: {budget} chunks delivered and the receiver still holds \
             {offset} of {} bytes. A transfer that makes no progress and reports no error is the \
             failure this bound exists to name.",
            snap.meta.total_bytes
        );
        let msg = install_msg(from, c.id(), term, snap, offset);
        let acts = c.step(Event::Recv(msg));
        offset = received_through(&only_send(&acts));
        out.extend(acts);
    }
    out
}

// ---------------------------------------------------------------------------------------------
// The payload format.
// ---------------------------------------------------------------------------------------------

/// A payload describes itself, and the description survives the round trip that a receiver — which
/// has nothing to compare it against — has to trust.
#[test]
fn a_payload_round_trips_and_its_header_describes_its_own_body() {
    let mut c = Consensus::new(N1, cfg3(), 7);
    seed(&mut c, &[(1, wal(1)), (1, wal(2))]);
    let at = point_of(&c);
    let snap = payload_of(&at, 3, 0xAB);

    assert_eq!(snap.meta.total_bytes, snap.payload().len() as u64);
    assert_eq!(snap.header.total_bytes(), snap.meta.total_bytes);

    let decoded = PayloadHeader::decode(snap.payload()).expect("its own header was refused");
    assert_eq!(decoded, snap.header, "the header did not survive its own encoding");
    assert_eq!(decoded.last_round, at.last_round);
    assert_eq!(decoded.last_term, at.last_term);
    assert_eq!(decoded.base_digest, at.base_digest);
    assert_eq!(decoded.image_len, 3 * PAGE_SIZE as u64);
    assert_eq!(decoded.arena_len, 8);
    assert_eq!(decoded.branches_len, 4);
}

/// Bytes that are not a ferrodb snapshot are refused for **not being one**, never parsed for a
/// field that happens to fit.
///
/// A snapshot payload is the one message that replaces a node's whole database. A parser that
/// guesses at a layout it does not recognise is a parser that installs a future format's fields as
/// this one's — and every field here is a length something is sized by.
#[test]
fn a_payload_header_that_is_not_one_is_refused_rather_than_guessed_at() {
    let mut c = Consensus::new(N1, cfg3(), 7);
    seed(&mut c, &[(1, wal(1))]);
    let good = payload_of(&point_of(&c), 1, 5);

    // Anti-vacuity first: the intact bytes decode, so every refusal below is about what was
    // changed and not about a parser that refuses everything.
    PayloadHeader::decode(good.payload()).expect("an intact header was refused");

    let mut wrong_magic = good.payload().to_vec();
    wrong_magic[0] = b'X';
    let e = PayloadHeader::decode(&wrong_magic).expect_err("foreign bytes were parsed as a payload");
    assert!(format!("{e}").contains("magic"), "refused, but not for the magic: {e}");

    let mut wrong_version = good.payload().to_vec();
    wrong_version[11] = 2;
    let e = PayloadHeader::decode(&wrong_version).expect_err("a v2 payload was read as a v1 one");
    assert!(format!("{e}").contains("version"), "refused, but not for the version: {e}");

    // A field flipped inside the header, with the header's own digest left stale.
    let mut damaged = good.payload().to_vec();
    damaged[60] ^= 0xFF;
    let e = PayloadHeader::decode(&damaged).expect_err("a damaged header was accepted");
    assert!(format!("{e}").contains("digest"), "refused, but not for the digest: {e}");

    let short = &good.payload()[..PayloadHeader::BYTES - 1];
    let e = PayloadHeader::decode(short).expect_err("a header shorter than one was accepted");
    assert!(format!("{e}").contains("header"), "refused, but not for the length: {e}");
}

/// A header that is intact and still describes an impossible payload is refused separately.
///
/// Damage and a bug in a sender are different failures with different causes, and a page count that
/// contradicts the image length is the second: `backup::restore` refuses exactly this mismatch, and
/// catching it in the header means nothing is spooled for it first.
#[test]
fn a_header_whose_page_count_contradicts_its_image_length_is_refused() {
    let mut c = Consensus::new(N1, cfg3(), 7);
    seed(&mut c, &[(1, wal(1))]);
    let at = point_of(&c);

    let e = Snapshot::build(
        &at,
        7,
        1,
        BackupLabel { start_lsn: 1, end_lsn: 2, page_count: 4 },
        &[],
        &[],
        vec![0u8; 3 * PAGE_SIZE],
    )
    .expect_err("an image of 3 pages was labelled 4 and accepted");
    assert!(format!("{e}").contains("pages"), "wrong reason: {e}");

    // Anti-vacuity: the same call with a page count that matches is accepted.
    Snapshot::build(
        &at,
        7,
        1,
        BackupLabel { start_lsn: 1, end_lsn: 2, page_count: 3 },
        &[],
        &[],
        vec![0u8; 3 * PAGE_SIZE],
    )
    .expect("a consistent payload was refused");
}

/// A zero-page image is not a snapshot of an empty database; it is a snapshot that collected
/// nothing, and installing it replaces a follower's state with nothing while reporting success.
///
/// `backup::take` refuses to *write* one for the same reason. This is the second half: refusing to
/// install one, in case a sender ever produces it another way.
#[test]
fn a_zero_page_image_is_refused_because_installing_it_would_look_like_success() {
    let mut c = Consensus::new(N1, cfg3(), 7);
    seed(&mut c, &[(1, wal(1))]);
    let e = Snapshot::build(
        &point_of(&c),
        7,
        1,
        BackupLabel { start_lsn: 1, end_lsn: 2, page_count: 0 },
        &[],
        &[],
        Vec::new(),
    )
    .expect_err("a zero-page snapshot was built");
    assert!(format!("{e}").contains("0 pages"), "wrong reason: {e}");
}

/// The first chunk always carries the whole header, and no chunk can overflow a frame.
///
/// Both are the reason a receiver can validate a payload before accepting a second chunk. Asserted
/// rather than left to a reader to notice, because either constant can be changed on its own.
#[test]
fn a_chunk_fits_a_frame_and_the_first_one_carries_the_whole_header() {
    assert!(
        SNAPSHOT_CHUNK_BYTES >= PayloadHeader::BYTES,
        "a chunk of {SNAPSHOT_CHUNK_BYTES} bytes cannot carry a {}-byte header, so a receiver \
         would have to accept bytes before it could tell what they are",
        PayloadHeader::BYTES
    );
    // The envelope around a chunk is the frame kind, the meta, the configuration and the length
    // prefixes. A megabyte of headroom is far more than any of them.
    assert!(
        SNAPSHOT_CHUNK_BYTES + (1 << 20) < crate::replication::MAX_FRAME_BYTES,
        "a chunk of {SNAPSHOT_CHUNK_BYTES} bytes plus its envelope does not fit the {}-byte frame \
         limit, so every transfer would be refused by the encoder",
        crate::replication::MAX_FRAME_BYTES
    );
}

// ---------------------------------------------------------------------------------------------
// What a receiver can decide from the envelope alone.
// ---------------------------------------------------------------------------------------------

/// **A snapshot must carry the configuration.** A receiver that installed one without it would hold
/// the database and no idea what a majority of the cluster is.
#[test]
fn a_snapshot_with_no_configuration_is_refused_because_its_receiver_could_not_count_a_majority() {
    let m = SnapshotMeta {
        last_round: 5,
        last_term: 2,
        config: Config::empty(),
        total_bytes: PayloadHeader::BYTES as u64 + 1,
    };
    let e = m.validate().expect_err("a snapshot with no voter set was accepted");
    assert!(format!("{e}").contains("empty voter set"), "wrong reason: {e}");

    // Anti-vacuity: the same meta with a configuration is accepted, so the refusal is about the
    // configuration and not about the rest of the meta.
    SnapshotMeta { config: cfg3(), ..m }.validate().expect("a well-formed meta was refused");
}

/// **An implausible `total_bytes` is refused before anything is allocated for it** — and on this
/// path nothing is ever allocated from it at all.
///
/// The receiver arms no cursor, keeps no bytes, and answers "I hold nothing". The second assertion
/// is the one that matters: a guard that refused *after* sizing a buffer would already have spent
/// the memory the guard exists to deny.
#[test]
fn a_snapshot_claiming_an_implausible_size_is_refused_before_anything_is_allocated_for_it() {
    let m = SnapshotMeta {
        last_round: 5,
        last_term: 2,
        config: cfg3(),
        total_bytes: u64::MAX,
    };
    let e = m.validate().expect_err("a snapshot claiming u64::MAX bytes was accepted");
    assert!(format!("{e}").contains("ceiling"), "wrong reason: {e}");

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 2, N1);
    let out = f.step(Event::Recv(Message {
        from: N1,
        to: N2,
        term: 2,
        body: Body::InstallSnapshot { meta: m, offset: 0, data: vec![0u8; 64], done: false },
    }));
    assert_eq!(received_through(&only_send(&out)), 0, "an impossible transfer was accepted");
    assert!(
        f.snapshot_incoming().is_none(),
        "a cursor was armed for a transfer that cannot exist, so the claim was acted on before it \
         was refused"
    );

    // A claim just *under* the ceiling passes the size check, so the ceiling is a comparison and
    // not a refusal of everything.
    SnapshotMeta { total_bytes: MAX_SNAPSHOT_BYTES, ..SnapshotMeta {
        last_round: 5,
        last_term: 2,
        config: cfg3(),
        total_bytes: 0,
    } }
    .validate()
    .expect("a snapshot at exactly the ceiling was refused");
}

/// Round 0 is "before the log begins", not a round. A snapshot of it covers nothing, and installing
/// one would move a receiver's floor to a round that does not exist.
#[test]
fn a_snapshot_at_round_zero_is_refused() {
    let m = SnapshotMeta {
        last_round: 0,
        last_term: 0,
        config: cfg3(),
        total_bytes: PayloadHeader::BYTES as u64,
    };
    let e = m.validate().expect_err("a snapshot of round 0 was accepted");
    assert!(format!("{e}").contains("round 0"), "wrong reason: {e}");
}

// ---------------------------------------------------------------------------------------------
// The sender.
// ---------------------------------------------------------------------------------------------

/// A peer whose next round is below this leader's floor is sent **state**, and never entries.
///
/// Sending entries at a round the leader no longer holds is a hole rather than a catch-up, and the
/// follower would accept it and hold a log with a gap no arithmetic could later distinguish from a
/// missing suffix.
#[test]
fn a_peer_below_the_floor_is_sent_state_and_never_entries() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2)), (1, wal(3)), (1, wal(4))]);
    promote(&mut l, 1);
    l.compact(3).expect("a checkpoint through an applied round was refused");

    let snap = Arc::new(payload_of(&point_of(&l), 1, 9));
    let mut out = Vec::new();
    l.progress.entry(N2).or_default().next = 2; // below the floor
    l.send_append_to(N2, &mut out);
    assert!(
        l.progress[&N2].needs_snapshot,
        "a peer asking for a round below the floor was not marked for state transfer"
    );
    assert!(sends(&out).is_empty(), "entries were sent for a round this leader does not hold");

    l.offer_snapshot_to(N2, Arc::clone(&snap)).expect("a valid snapshot was refused");
    let mut out = Vec::new();
    l.send_append_to(N2, &mut out);
    let m = only_send(&out);
    assert_eq!(installed(&m).last_round, snap.meta.last_round);
    assert_eq!(m.to, N2);
}

/// A leader will not serve a snapshot of state its own storage engine has not been given.
///
/// The payload is an image of that engine, so a snapshot above `applied` describes a round the
/// engine has never seen — and the receiver has nothing to check it against, which is the whole
/// reason it needs one.
#[test]
fn a_leader_refuses_to_serve_a_snapshot_above_what_its_engine_has_applied() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote(&mut l, 1);
    let good = payload_of(&point_of(&l), 1, 1);

    // Now pretend the engine is behind the log, which is what it is between an `Apply` and the
    // applier returning.
    l.applied = 1;
    let e = l
        .offer_snapshot_to(N2, Arc::new(good.clone()))
        .expect_err("a snapshot above `applied` was served");
    assert!(format!("{e}").contains("applied only through"), "wrong reason: {e}");

    // Anti-vacuity: with the engine caught up, the same snapshot is served.
    l.applied = 3;
    l.offer_snapshot_to(N2, Arc::new(good)).expect("a snapshot at `applied` was refused");
}

/// A snapshot whose term at its own last round disagrees with the leader's log is a snapshot of a
/// different history, and `(round, term)` is the pair every later log-matching check is decided by.
#[test]
fn a_leader_refuses_a_snapshot_whose_term_disagrees_with_its_own_log() {
    let mut l = Consensus::new(N1, cfg3(), 3);
    seed(&mut l, &[(1, wal(1)), (2, wal(2)), (3, wal(3))]);
    promote(&mut l, 3);

    let mut at = point_of(&l);
    at.last_term += 1;
    let wrong = payload_of(&at, 1, 1);
    let e = l.offer_snapshot_to(N2, Arc::new(wrong)).expect_err("a mismatched term was served");
    assert!(format!("{e}").contains("different history"), "wrong reason: {e}");

    l.offer_snapshot_to(N2, Arc::new(payload_of(&point_of(&l), 1, 1)))
        .expect("a snapshot whose term matches was refused");
}

/// **A snapshot must anchor the receiver's digest chain where this leader is.**
///
/// `AppendResp.digest` is a rolling hash chained from the digest at the log's floor. A receiver
/// re-seeded with the wrong anchor computes a different digest from its leader at *every* round
/// above the floor, and the leader latches it as diverged — a healthy node, permanently out of the
/// quorum, reported as byte-level corruption. The check has to be here because it is the only place
/// both values exist.
#[test]
fn a_leader_refuses_a_snapshot_whose_base_digest_is_not_its_own() {
    let mut l = Consensus::new(N1, cfg3(), 3);
    seed(&mut l, &[(1, wal(1)), (1, wal(2))]);
    promote(&mut l, 1);

    let mut at = point_of(&l);
    at.base_digest ^= 1;
    let e = l
        .offer_snapshot_to(N2, Arc::new(payload_of(&at, 1, 1)))
        .expect_err("a snapshot anchored somewhere this leader is not was served");
    assert!(format!("{e}").contains("base digest"), "wrong reason: {e}");

    l.offer_snapshot_to(N2, Arc::new(payload_of(&point_of(&l), 1, 1)))
        .expect("a correctly anchored snapshot was refused");
}

/// **A completed transfer moves `next` and never `matched`.**
///
/// `received_through` says the peer received the payload; it says nothing about whether the peer
/// made it durable. Quorum is counted over `matched`, so treating a byte cursor as a match would
/// count a replica of state no evidence puts on that peer's disk. The peer's next `AppendResp` is
/// that evidence — and if the install did not survive, the same append is refused and the transfer
/// simply happens again.
#[test]
fn a_completed_transfer_moves_next_and_never_matched() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote(&mut l, 1);
    let snap = Arc::new(payload_of(&point_of(&l), 1, 4));
    l.progress.entry(N2).or_default().needs_snapshot = true;
    l.offer_snapshot_to(N2, Arc::clone(&snap)).unwrap();

    let before = l.progress[&N2].matched;
    let mut out = Vec::new();
    l.on_install_snapshot_resp(N2, snap.meta.total_bytes, &mut out);

    assert_eq!(
        l.progress[&N2].matched, before,
        "a byte cursor was counted as a replica: quorum is counted over `matched`, and this leader \
         now believes a round is on a disk nothing has said it reached"
    );
    assert_eq!(
        l.progress[&N2].next,
        snap.meta.last_round + 1,
        "the leader did not continue from the round the snapshot covers"
    );
    assert!(!l.progress[&N2].needs_snapshot, "the peer is still marked for a transfer that finished");
    assert!(l.snapshot_in_flight(N2).is_none(), "the finished transfer was left armed");
}

/// **A receiver is the authority on its own resume point, even when it says it has less.**
///
/// The first version of this rule made the sender's cursor monotonic, on the reasoning that a
/// duplicated answer must not rewind a transfer. That rule deadlocks the case it matters in: a
/// follower that crashes mid-transfer comes back holding nothing and says so, and a monotonic
/// sender goes on sending chunks from the middle of a payload the follower can never complete —
/// a follower that is never repaired, with both nodes healthy and both behaving. A duplicated stale
/// answer costs one re-sent chunk and then converges.
#[test]
fn a_receiver_that_lost_a_transfer_rewinds_it_rather_than_deadlocking_it() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2))]);
    promote(&mut l, 1);
    // A payload big enough to need more than one chunk.
    let snap = Arc::new(payload_of(&point_of(&l), 512, 2));
    assert!(snap.meta.total_bytes > SNAPSHOT_CHUNK_BYTES as u64, "the fixture fits in one chunk");
    l.progress.entry(N2).or_default().needs_snapshot = true;
    l.offer_snapshot_to(N2, Arc::clone(&snap)).unwrap();

    let mut out = Vec::new();
    l.on_install_snapshot_resp(N2, SNAPSHOT_CHUNK_BYTES as u64, &mut out);
    assert_eq!(l.snapshot_in_flight(N2).unwrap().acked(), SNAPSHOT_CHUNK_BYTES as u64);

    // An answer carrying nothing new is not answered again — otherwise a duplicate is answered
    // with a duplicate, for ever.
    let mut quiet = Vec::new();
    l.on_install_snapshot_resp(N2, SNAPSHOT_CHUNK_BYTES as u64, &mut quiet);
    assert!(sends(&quiet).is_empty(), "a duplicate was answered with another chunk");
    assert_eq!(l.snapshot_in_flight(N2).unwrap().acked(), SNAPSHOT_CHUNK_BYTES as u64);

    // The receiver restarted and holds nothing. The sender must follow it back, and must resume
    // from the header — the only offset at which a payload can be validated.
    let mut back = Vec::new();
    l.on_install_snapshot_resp(N2, 0, &mut back);
    assert_eq!(
        l.snapshot_in_flight(N2).unwrap().acked(),
        0,
        "the sender kept its own cursor against a receiver that told it otherwise, so the transfer \
         can never complete"
    );
    match &only_send(&back).body {
        Body::InstallSnapshot { offset, .. } => assert_eq!(*offset, 0),
        other => panic!("expected a chunk from the start, got {other:?}"),
    }
}

/// **A peer mid-transfer must not cost this leader its own lease.**
///
/// A node receiving state answers no `Append`, so without this its silence counter climbs for the
/// whole transfer and a leader of three steps down on a cluster that is perfectly healthy and
/// merely busy — the lease firing through the mechanism written to make transfers possible.
#[test]
fn a_transfer_ack_resets_the_peers_silence_so_a_slow_install_does_not_cost_the_leader_its_lease() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2))]);
    promote(&mut l, 1);
    let snap = Arc::new(payload_of(&point_of(&l), 512, 2));
    l.progress.entry(N2).or_default().needs_snapshot = true;
    l.offer_snapshot_to(N2, Arc::clone(&snap)).unwrap();
    l.progress.entry(N2).or_default().silent = 99;

    let mut out = Vec::new();
    l.on_install_snapshot_resp(N2, SNAPSHOT_CHUNK_BYTES as u64, &mut out);
    assert_eq!(
        l.progress[&N2].silent, 0,
        "a peer that answered a snapshot chunk still counts as silent, so a long transfer expires \
         the leader's lease"
    );
}

/// A node the cluster has removed keeps running and keeps answering. Its answers must not
/// resurrect a `Progress` for a set it has left.
#[test]
fn an_answer_from_a_removed_node_does_not_resurrect_its_progress() {
    let mut l = Consensus::new(N1, Config::new([N1, N2], 1, 1), 1);
    seed(&mut l, &[(1, wal(1))]);
    promote(&mut l, 1);
    let mut out = Vec::new();
    l.on_install_snapshot_resp(N3, 1024, &mut out);
    assert!(!l.progress.contains_key(&N3), "a removed node's progress was recreated by its answer");
    assert!(sends(&out).is_empty());
}

// ---------------------------------------------------------------------------------------------
// The receiver.
// ---------------------------------------------------------------------------------------------

/// A chunk at the wrong offset is answered with the resume point and never buffered.
///
/// Buffering a hole means holding bytes at a position a peer chose, and the frozen
/// `InstallSnapshotResp` can describe exactly one cursor anyway — so a receiver that accepted holes
/// would be keeping state it has no way to report.
#[test]
fn a_chunk_at_the_wrong_offset_is_answered_with_the_resume_point_and_never_buffered() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2))]);
    promote(&mut l, 1);
    // Four chunks, so the skipped one is in the MIDDLE. A mutant of this rule survived a fixture
    // whose out-of-order chunk was the last one: the `done`-at-the-wrong-length check refused it
    // instead, and the test could not tell which guard had done the refusing.
    let snap = payload_of(&point_of(&l), 1024, 3);
    assert!(
        snap.meta.total_bytes > 3 * SNAPSHOT_CHUNK_BYTES as u64,
        "the fixture is too small for a chunk to be skipped without being the last one"
    );

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);

    let out = f.step(Event::Recv(install_msg(N1, N2, 1, &snap, 0)));
    let held = received_through(&only_send(&out));
    assert_eq!(held, SNAPSHOT_CHUNK_BYTES as u64);

    // Chunk 2, while this node holds only chunk 0 — a hole, and not the final chunk, so nothing
    // about the transfer's declared length can refuse it. Only the offset can.
    let ahead = install_msg(N1, N2, 1, &snap, held + SNAPSHOT_CHUNK_BYTES as u64);
    assert!(
        matches!(&ahead.body, Body::InstallSnapshot { done, .. } if !done),
        "the skipped-over chunk is the last one, so this fixture cannot isolate the offset rule"
    );
    let out = f.step(Event::Recv(ahead));
    assert_eq!(
        received_through(&only_send(&out)),
        held,
        "an out-of-order chunk moved the cursor, which leaves a hole in the payload that digests \
         to nothing only after the whole transfer has finished"
    );
    assert_eq!(f.snapshot_incoming().unwrap().received(), held);
}

/// A chunk that would push a transfer past its own declared length is refused whole.
///
/// Accepting a prefix of it would leave the digest folded over bytes nobody declared, and the
/// mismatch would surface at the end of the transfer as damage rather than here as a lie.
#[test]
fn a_chunk_that_would_run_past_the_declared_length_is_refused() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1))]);
    promote(&mut l, 1);
    let snap = payload_of(&point_of(&l), 1, 3);

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);

    // **Not final.** A mutant of this rule survived a fixture whose over-long chunk was also the
    // last one: the `done`-at-the-wrong-length check refused it, and the test could not tell which
    // guard had done the refusing. With `done: false`, the length rule is the only one that can.
    let mut over = install_msg(N1, N2, 1, &snap, 0);
    if let Body::InstallSnapshot { data, done, .. } = &mut over.body {
        data.extend_from_slice(&[0u8; 32]);
        *done = false;
    }
    let out = f.step(Event::Recv(over));
    assert_eq!(received_through(&only_send(&out)), 0, "a chunk longer than the payload was taken");
    assert!(
        f.snapshot_incoming().is_none(),
        "a chunk that runs past the declared length was accepted into the cursor"
    );
}

/// A transfer that says it is **done** at the wrong length is refused.
///
/// Its own test, because the guard above can otherwise stand in for this one and neither is then
/// tested. The two are different claims: one is a chunk that is too long, this is a sender and a
/// receiver that disagree about how much a whole snapshot is — and there is no reading of that
/// which installs safely.
#[test]
fn a_transfer_that_claims_to_be_done_at_the_wrong_length_is_refused() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1))]);
    promote(&mut l, 1);
    let snap = payload_of(&point_of(&l), 512, 3);

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);

    // The first chunk of a multi-chunk payload, flagged `done`.
    let mut early = install_msg(N1, N2, 1, &snap, 0);
    let len = match &mut early.body {
        Body::InstallSnapshot { data, done, .. } => {
            *done = true;
            data.len() as u64
        }
        other => panic!("expected an InstallSnapshot, got {other:?}"),
    };
    assert!(len < snap.meta.total_bytes, "the fixture fits in one chunk, so it cannot end early");

    let out = f.step(Event::Recv(early));
    assert!(
        f.pending_install_round().is_none(),
        "a transfer that ended {len} bytes into a {} byte payload was offered for install",
        snap.meta.total_bytes
    );
    // The chunk itself was fine, so it is kept and reported — see
    // `a_short_done_keeps_the_bytes_already_accepted_and_a_bad_digest_does_not` for why the two
    // completion faults are answered differently.
    assert_eq!(received_through(&only_send(&out)), len);

    // Anti-vacuity: the SAME first chunk without the flag leaves the node in the same place, so the
    // refusal above is about the claim of completeness and not about the chunk.
    let out = f.step(Event::Recv(install_msg(N1, N2, 1, &snap, 0)));
    assert_eq!(received_through(&only_send(&out)), len);
    assert!(f.pending_install_round().is_none());
}

/// A payload whose body does not digest to what its header claims is refused **before the driver is
/// told there is anything to install**.
#[test]
fn a_payload_whose_body_digest_does_not_match_is_refused_before_the_driver_is_told() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1))]);
    promote(&mut l, 1);
    let snap = payload_of(&point_of(&l), 1, 3);

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);

    let mut damaged = install_msg(N1, N2, 1, &snap, 0);
    if let Body::InstallSnapshot { data, .. } = &mut damaged.body {
        let n = data.len();
        data[n - 1] ^= 0xFF;
    }
    let out = f.step(Event::Recv(damaged));
    assert_eq!(received_through(&only_send(&out)), 0, "a damaged payload was accepted whole");
    assert!(f.pending_install_round().is_none(), "a damaged payload was offered for install");
    assert!(
        f.snapshot_incoming().is_none(),
        "a transfer whose bytes do not digest kept a prefix that cannot be trusted"
    );

    // Anti-vacuity: the same payload undamaged completes.
    let out = f.step(Event::Recv(install_msg(N1, N2, 1, &snap, 0)));
    assert_eq!(received_through(&only_send(&out)), snap.meta.total_bytes);
    assert_eq!(f.pending_install_round(), Some(snap.meta.last_round));
}

/// An envelope and a payload that disagree about what this is are refused, and neither is preferred.
///
/// Nothing on the receiving side can tell which of the two is lying, and the meta is what sizes the
/// transfer while the header is what sizes the install.
#[test]
fn an_envelope_and_a_payload_that_disagree_are_refused() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2))]);
    promote(&mut l, 1);
    let snap = payload_of(&point_of(&l), 1, 3);

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);

    let mut lying = install_msg(N1, N2, 1, &snap, 0);
    if let Body::InstallSnapshot { meta, .. } = &mut lying.body {
        meta.last_round += 1;
    }
    let out = f.step(Event::Recv(lying));
    assert_eq!(received_through(&only_send(&out)), 0, "an envelope contradicting its payload was taken");
    assert!(f.snapshot_incoming().is_none());
}

/// A snapshot at or below this node's own floor is acknowledged whole and **not installed**.
///
/// Installing it would move the floor backwards and replace state this node has with older state.
/// Answered as complete rather than refused, so the sender stops resending and moves to entries —
/// which is what it would have done had it known this node's floor.
#[test]
fn a_snapshot_older_than_this_nodes_floor_is_acknowledged_whole_and_not_installed() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote(&mut l, 1);
    let old = payload_of(
        &SnapshotPoint { last_round: 1, last_term: 1, config: cfg3(), base_digest: 7 },
        1,
        3,
    );

    let mut f = Consensus::new(N2, cfg3(), 3);
    seed(&mut f, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    follower_of(&mut f, 1, N1);
    f.compact(2).expect("a checkpoint through an applied round was refused");
    let floor_before = f.snapshot_round;

    let out = f.step(Event::Recv(install_msg(N1, N2, 1, &old, 0)));
    assert_eq!(received_through(&only_send(&out)), old.meta.total_bytes);
    assert_eq!(f.snapshot_round, floor_before, "the floor moved backwards onto an older snapshot");
    assert!(f.pending_install_round().is_none());
}

/// A snapshot of a round this node already holds in its own log is not installed: by the
/// log-matching property it already holds everything the snapshot covers.
#[test]
fn a_snapshot_of_a_round_this_node_already_holds_is_not_installed() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote(&mut l, 1);
    let at = SnapshotPoint { last_round: 2, last_term: 1, config: cfg3(), base_digest: 0 };
    let snap = payload_of(&at, 1, 3);

    let mut f = Consensus::new(N2, cfg3(), 3);
    seed(&mut f, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    follower_of(&mut f, 1, N1);

    let out = f.step(Event::Recv(install_msg(N1, N2, 1, &snap, 0)));
    assert_eq!(received_through(&only_send(&out)), snap.meta.total_bytes);
    // **The assertion has to be that nothing was accepted for install**, not that nothing has
    // moved: nothing moves until `Persisted` either way, so a mutant of this rule survived a test
    // that only checked the floor and the tail.
    assert!(
        f.pending_install_round().is_none(),
        "a snapshot of state this node already holds was accepted for install; the driver would \
         now replace this node's whole database with an older image of it"
    );
    assert!(f.snapshot_incoming().is_none(), "a cursor was armed for a transfer with nothing to do");
    assert_eq!(f.snapshot_round, 0, "a snapshot of state this node already had was installed");
    assert_eq!(f.last_round, 3, "the log was discarded for a snapshot that added nothing");
}

/// A different snapshot supersedes one in flight rather than being merged into it.
///
/// Two payloads interleaved by offset digest to nothing and are neither, so a new leader's transfer
/// restarts the cursor instead of continuing somebody else's.
#[test]
fn a_second_snapshot_supersedes_the_one_in_flight() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2))]);
    promote(&mut l, 1);
    let first = payload_of(&point_of(&l), 512, 1);
    let second = payload_of(&point_of(&l), 512, 2);
    assert_ne!(first.payload(), second.payload(), "the fixture's two payloads are the same bytes");

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);
    f.step(Event::Recv(install_msg(N1, N2, 1, &first, 0)));
    assert_eq!(f.snapshot_incoming().unwrap().received(), SNAPSHOT_CHUNK_BYTES as u64);

    // **The two metas are equal** — same round, same term, same configuration, same length — and
    // the payloads are different, which is what two captures of one round look like: a base backup
    // copies pages while writes land, so it is a smear of states rather than an instant. A receiver
    // that keyed on the meta would splice the second onto the first, fail the digest at the end,
    // and — since a refusal does not destroy a cursor — do it again for ever. The header is the
    // discriminator, because it carries the body digest.
    assert_eq!(first.meta, second.meta, "the fixture's two metas differ, so it tests nothing");
    assert_ne!(first.header, second.header);

    deliver_all(&mut f, N1, 1, &second);
    assert_eq!(
        f.pending_install_round(),
        Some(second.meta.last_round),
        "the transfer never converged: the two payloads were spliced by offset and the digest can \
         never pass"
    );
    assert_eq!(f.snapshot_incoming().unwrap().header(), &second.header);
}

/// **Nothing moves until the install is reported durable.**
///
/// Bytes in flight are not bytes on a disk. A state machine that moved its floor when the last
/// chunk arrived would answer for a history its storage engine had not yet accepted — rule 4's
/// failure in a different costume, and the one a snapshot makes unrecoverable because the log that
/// would prove otherwise is what the floor discards.
#[test]
fn nothing_moves_until_the_install_is_reported_durable() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote(&mut l, 1);
    let snap = payload_of(&point_of(&l), 1, 6);

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);
    deliver_all(&mut f, N1, 1, &snap);

    assert_eq!(f.pending_install_round(), Some(snap.meta.last_round), "the payload never completed");
    assert_eq!(f.snapshot_round, 0, "the floor moved on bytes that are not on a disk");
    assert_eq!(f.last_round, 0, "the log moved on bytes that are not on a disk");
    assert_eq!(f.durable, 0, "durability was claimed for an install that has not happened");

    // The driver reports the install. Only now does anything move.
    f.step(Event::Persisted { term: 1, round: snap.meta.last_round });
    assert_eq!(f.snapshot_round, snap.meta.last_round);
    assert_eq!(f.snapshot_term, snap.meta.last_term);
    assert_eq!(f.last_round, snap.meta.last_round);
    assert_eq!(f.durable, snap.meta.last_round);
    assert!(f.snapshot_incoming().is_none(), "the finished transfer was left pending");
}

/// **Completing an install sets the round watermark; `unjoined` is cleared by the next `Append`,
/// through F1's one rule.**
///
/// `DISTRIBUTED.md` §F6 says an install "ends by setting the follower's round watermark, which is
/// also what clears its `unjoined` flag (F1)" — and the emphasis is on setting the watermark.
/// Clearing the flag *here* would be the defect `mod.rs` names on the field: a snapshot covers the
/// sender's compaction floor, which is at or below its commit and normally below it, so "installed"
/// proves this node holds the snapshot's prefix and never that it holds what a quorum holds. A node
/// that campaigned on that would raise the term above everybody's while holding a prefix.
#[test]
fn completing_an_install_sets_the_watermark_and_leaves_unjoined_to_the_next_append() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote(&mut l, 1);
    let snap = payload_of(&point_of(&l), 1, 6);

    let mut j = Consensus::joining(N2, 3);
    follower_of(&mut j, 1, N1);
    deliver_all(&mut j, N1, 1, &snap);
    j.step(Event::Persisted { term: 1, round: snap.meta.last_round });

    assert!(!j.behind, "the snapshot's configuration was not applied");
    assert!(
        j.unjoined,
        "the install cleared `unjoined` on its own. It proves this node holds the snapshot's \
         prefix, never that it holds what a quorum holds — so the node may now campaign with a \
         term above everybody's while holding a prefix."
    );
    assert_eq!(j.durable, snap.meta.last_round, "the round watermark was not set");
    assert!(!j.may_campaign());

    // The leader's next `Append` carries a commit this node now holds, and F1's rule clears the
    // flag on that evidence. This is the anti-vacuity half: the flag is reachable, not stuck.
    j.step(Event::Recv(Message {
        from: N1,
        to: N2,
        term: 1,
        body: Body::Append {
            prev_round: snap.meta.last_round,
            prev_term: snap.meta.last_term,
            entries: Vec::new(),
            commit: snap.meta.last_round,
        },
    }));
    assert!(!j.unjoined, "an observable watermark this node holds did not clear the flag");
    assert!(j.may_campaign());
}

/// **An installed snapshot anchors the digest chain where the leader is.**
///
/// The other half of `a_leader_refuses_a_snapshot_whose_base_digest_is_not_its_own`: having been
/// given the right anchor, the receiver must use it. A receiver that anchored at zero would report
/// a digest the leader disagrees with at the very first round after the floor, and be latched out
/// of the quorum.
#[test]
fn an_installed_snapshot_anchors_the_digest_chain_where_the_leader_is() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote(&mut l, 1);
    l.append_own_entry(wal(4), &mut Vec::new());
    l.durable = l.last_round;
    let snap = payload_of(&point_of(&l), 1, 6);

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);
    deliver_all(&mut f, N1, 1, &snap);
    f.step(Event::Persisted { term: 1, round: snap.meta.last_round });

    assert_eq!(
        f.digest_at(snap.meta.last_round),
        l.digest_at(snap.meta.last_round),
        "the receiver's chain is anchored somewhere the leader is not"
    );

    // And the round *after* the floor, which is the one the leader will actually digest-check.
    let e = Entry { term: 1, round: snap.meta.last_round + 1, command: wal(4) };
    f.step(Event::Recv(Message {
        from: N1,
        to: N2,
        term: 1,
        body: Body::Append {
            prev_round: snap.meta.last_round,
            prev_term: snap.meta.last_term,
            entries: vec![e],
            commit: snap.meta.last_round,
        },
    }));
    f.step(Event::Persisted { term: 1, round: snap.meta.last_round + 1 });
    assert_eq!(
        f.digest_at(snap.meta.last_round + 1),
        l.digest_at(snap.meta.last_round + 1),
        "the first round after an installed snapshot digests differently on the two nodes, so the \
         leader's divergence detector will latch a healthy follower out of the quorum"
    );
}

/// An installed configuration goes through the one seam that refuses damage, and the refusal is
/// propagated rather than swallowed.
#[test]
fn an_install_whose_configuration_is_damage_latches_this_node_out_of_office() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2))]);
    promote(&mut l, 1);

    // A configuration claiming the identity the receiver's current one already holds, with a
    // different membership — the collision `note_config_in_log` exists to refuse.
    let colliding = Config::new([N1, N2], 1, 1);
    let mut at = point_of(&l);
    at.config = colliding;
    let snap = payload_of(&at, 1, 6);

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);
    deliver_all(&mut f, N1, 1, &snap);
    let out = f.step(Event::Persisted { term: 1, round: snap.meta.last_round });

    assert!(
        out.iter().any(|a| matches!(a, Action::Refuse { .. })),
        "damage in a snapshot's configuration was swallowed: {out:#?}"
    );
    assert!(f.behind, "a node that installed a damaged configuration may still campaign");
    assert_eq!(f.snapshot_round, 0, "the floor moved for an install that was refused");
    assert_eq!(*f.config(), cfg3(), "a colliding configuration was installed");
}

/// An install discards the whole log, and the floor moves with it.
///
/// A node being re-seeded holds rounds the leader serving it cannot vouch for — that is why it is
/// being re-seeded — so keeping any of them would keep a suffix nothing has agreed to.
#[test]
fn an_install_discards_the_whole_log_and_the_floor_moves_with_it() {
    let mut l = Consensus::new(N1, cfg3(), 5);
    seed(&mut l, &[(5, wal(1)), (5, wal(2)), (5, wal(3)), (5, wal(4)), (5, wal(5))]);
    promote(&mut l, 5);
    let snap = payload_of(&point_of(&l), 1, 6);

    // A follower holding a *longer* log from an older term, which is exactly the shape that must
    // not survive.
    let mut f = Consensus::new(N2, cfg3(), 2);
    seed(&mut f, &[(2, wal(9)), (2, wal(9)), (2, wal(9)), (2, wal(9)), (2, wal(9)), (2, wal(9))]);
    follower_of(&mut f, 2, N1);
    // A node holding a suffix that is about to be discarded was never told any of it was
    // committed — a committed round is on every leader that can be elected afterwards, so a
    // conflicting one cannot have been. `seed` sets a commit for the tests that want one; this
    // fixture must not claim it.
    f.commit = 0;
    f.applied = 0;
    assert_eq!(f.last_round, 6);

    deliver_all(&mut f, N1, 5, &snap);
    let out = f.step(Event::Persisted { term: 5, round: snap.meta.last_round });

    // **The applied cursor, judged by what it makes the driver do.** `advance_apply` runs after
    // the install and will happily raise `applied` from 0 to 5 on its own, so asserting the field
    // alone cannot see an install that left it behind — a mutant proved exactly that. What it
    // cannot cover for is the action: an `Apply { through: 5 }` on a node whose cursor is 0 sends
    // the driver walking rounds 1..5, and rounds 1..5 no longer exist anywhere on this node.
    assert!(
        !out.iter().any(|a| matches!(a, Action::Apply { .. })),
        "the install asked the storage engine to apply rounds the snapshot replaced, which no \
         longer exist on this node: {out:#?}"
    );
    assert_eq!(f.snapshot_round, 5);
    assert_eq!(f.last_round, 5, "a stale suffix survived an install");
    assert_eq!(f.term_at(5), Some(5));
    assert_eq!(f.term_at(6), None, "a round above the snapshot survived the install");
    assert_eq!(f.commit, 5);
    assert_eq!(f.applied, 5, "the applied cursor was left below the floor it can no longer read");
}

/// Receiving state adopts the sender as leader, so a transfer that takes many round trips is not an
/// election.
///
/// No `Append` arrives during a transfer, so without this the receiver's election timeout expires
/// mid-install and it campaigns against the leader that is repairing it.
#[test]
fn receiving_state_adopts_the_sender_as_leader_so_a_long_transfer_is_not_an_election() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1)), (1, wal(2))]);
    promote(&mut l, 1);
    let snap = payload_of(&point_of(&l), 512, 7);

    let mut f = Consensus::new(N2, cfg3(), 3);
    f.hard.term = 1;
    f.since_heard = 999;
    f.step(Event::Recv(install_msg(N1, N2, 1, &snap, 0)));
    assert_eq!(f.leader(), Some(N1), "the sender of state was not adopted as leader");
    assert_eq!(f.role(), Role::Follower);
    assert_eq!(f.since_heard, 0, "a chunk did not count as hearing from the leader");
}

// ---------------------------------------------------------------------------------------------
// Checkpointing this node's own log.
// ---------------------------------------------------------------------------------------------

/// A checkpoint above what the storage engine has applied is refused.
///
/// A floor above the engine is a claim about state that was never installed, and the rounds that
/// would prove otherwise are exactly what the checkpoint discards.
#[test]
fn a_checkpoint_above_what_the_engine_has_applied_is_refused() {
    let mut c = Consensus::new(N1, cfg3(), 1);
    seed(&mut c, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    c.applied = 2;

    let e = c.compact(3).expect_err("a checkpoint above `applied` was allowed");
    assert!(format!("{e}").contains("applied only through"), "wrong reason: {e}");
    assert_eq!(c.snapshot_round, 0, "a refused checkpoint moved the floor");

    c.compact(2).expect("a checkpoint at `applied` was refused");
    assert_eq!(c.snapshot_round, 2);
    assert_eq!(c.snapshot_term, 1);
}

/// A checkpoint keeps the digests of the rounds it keeps.
///
/// A rolling digest is an absolute value at a round, not an offset into a vector, so a compaction
/// must not change what this node reports for any round it still holds — or every follower and
/// leader that compacted at different moments would report a divergence that is not there.
#[test]
fn a_checkpoint_keeps_the_digests_of_the_rounds_it_keeps() {
    let mut c = Consensus::new(N1, cfg3(), 1);
    seed(&mut c, &[(1, wal(1)), (1, wal(2)), (1, wal(3)), (1, wal(4))]);
    let before: Vec<Option<u64>> = (1..=4).map(|r| c.digest_at(r)).collect();

    c.compact(2).unwrap();

    assert_eq!(c.digest_at(3), before[2], "a kept round's digest changed under a checkpoint");
    assert_eq!(c.digest_at(4), before[3], "a kept round's digest changed under a checkpoint");
    assert_eq!(c.digest_at(2), before[1], "the new floor's digest is not the one it had");
    assert_eq!(c.digest_at(1), None, "a discarded round is still answerable");
    assert_eq!(c.floor_digest(), before[1].unwrap());
}

/// A checkpoint is a no-op below the floor rather than an error: a retention policy runs every turn
/// and most turns have nothing to do.
#[test]
fn a_checkpoint_at_or_below_the_floor_is_a_no_op() {
    let mut c = Consensus::new(N1, cfg3(), 1);
    seed(&mut c, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    c.compact(2).unwrap();
    c.compact(2).expect("re-checkpointing at the floor was an error");
    c.compact(1).expect("checkpointing below the floor was an error");
    assert_eq!(c.snapshot_round, 2, "a no-op checkpoint moved the floor");
}

/// `restore` puts a restarted node back where its floor is, on both the commit and the applied
/// cursors.
///
/// The log *tail*'s committedness is a fact about a quorum that only a leader's `Append` can
/// re-establish, and restoring it from local disk is how a node applies a round the cluster later
/// truncated. **A snapshot floor is different in kind**: it exists only because a snapshot covering
/// it was taken at or below a leader's `applied`, so it is committed by construction — and leaving
/// `applied` behind it sends the next `Apply` walking rounds that no longer exist.
#[test]
fn a_restart_above_a_snapshot_floor_comes_back_at_the_floor_and_not_at_zero() {
    let mut c = Consensus::new(N1, cfg3(), 1);
    c.restore(HardState { term: 4, voted_for: Some(N1) }, 12, 3, 0xDEAD_BEEF, Vec::new());

    assert_eq!(c.snapshot_round, 12);
    assert_eq!(c.last_round, 12);
    assert_eq!(c.durable, 12);
    assert_eq!(c.commit, 12, "a restarted node forgot that its floor is committed state");
    assert_eq!(c.applied, 12, "the applied cursor was left below rounds this node cannot read");
    assert_eq!(
        c.digest_at(12),
        Some(0xDEAD_BEEF),
        "the floor's digest was not restored, so every round above it will disagree with the leader"
    );
}

/// **The completion rule, asked directly.**
///
/// Its two halves are unreachable independently through the receive path — a short transfer fails
/// both — so a mutant that removed the length check survived every behavioural test in this file.
/// A rule no test can distinguish is a rule nobody has shown matters, so the rule is a function and
/// this test asks it.
#[test]
fn a_completed_transfer_is_judged_on_its_length_and_then_on_its_bytes() {
    // The length, with a digest that matches — the case the receive path cannot construct, because
    // a short payload's digest never matches, and the reason the check was untestable inline.
    assert_eq!(
        super::completion_verdict(500, 1000, 0xABCD, 0xABCD),
        Err(super::CompletionFault::Length),
        "a transfer that ended at 500 bytes of 1000 was accepted"
    );

    // The bytes, at the right length. A DIFFERENT verdict, because the two are recovered
    // differently: a wrong length keeps the bytes already accepted and a wrong digest cannot.
    assert_eq!(
        super::completion_verdict(1000, 1000, 0xABCD, 0x1234),
        Err(super::CompletionFault::Digest),
        "a payload that digests differently from its own header was accepted"
    );

    // Anti-vacuity: right length, right bytes, accepted. Without this the two refusals above would
    // pass just as well against a function that refused everything.
    super::completion_verdict(1000, 1000, 0xABCD, 0xABCD).expect("a complete transfer was refused");
}

/// A `done` at the wrong **length** does not destroy the transfer; a wrong **digest** does.
///
/// The two are recovered differently and the difference is the whole reason they are separate
/// verdicts. A short `done` says nothing about the bytes — one frame, truncated in transit or
/// forged — and throwing away the megabytes already accepted for it is a denial of service anyone
/// who can reach the port could perform. A digest mismatch says some byte is wrong and cannot say
/// which, so no prefix of that transfer is worth keeping.
#[test]
fn a_short_done_keeps_the_bytes_already_accepted_and_a_bad_digest_does_not() {
    let mut l = Consensus::new(N1, cfg3(), 1);
    seed(&mut l, &[(1, wal(1))]);
    promote(&mut l, 1);
    let snap = payload_of(&point_of(&l), 512, 3);

    let mut f = Consensus::new(N2, cfg3(), 3);
    follower_of(&mut f, 1, N1);

    let mut early = install_msg(N1, N2, 1, &snap, 0);
    if let Body::InstallSnapshot { done, .. } = &mut early.body {
        *done = true;
    }
    let out = f.step(Event::Recv(early));
    let held = received_through(&only_send(&out));
    assert_eq!(
        held,
        SNAPSHOT_CHUNK_BYTES as u64,
        "a refused completion threw away the chunk it arrived with"
    );
    assert_eq!(
        f.snapshot_incoming().map(|c| c.received()),
        Some(SNAPSHOT_CHUNK_BYTES as u64),
        "a refused completion destroyed the transfer, so one bad frame costs a whole re-transfer"
    );

    // And the transfer still finishes from where it was, which is what makes the survival useful
    // rather than merely tidy.
    deliver_all(&mut f, N1, 1, &snap);
    assert_eq!(f.pending_install_round(), Some(snap.meta.last_round));

    // The other half: a full-length transfer whose bytes are wrong drops the cursor and answers
    // zero, because there is no prefix of it worth resuming from.
    let mut g = Consensus::new(N3, cfg3(), 5);
    follower_of(&mut g, 1, N1);
    let mut offset = 0u64;
    while offset + SNAPSHOT_CHUNK_BYTES as u64 <= snap.meta.total_bytes {
        let out = g.step(Event::Recv(install_msg(N1, N3, 1, &snap, offset)));
        offset = received_through(&only_send(&out));
    }
    let mut last = install_msg(N1, N3, 1, &snap, offset);
    if let Body::InstallSnapshot { data, .. } = &mut last.body {
        let n = data.len();
        data[n - 1] ^= 0xFF;
    }
    let out = g.step(Event::Recv(last));
    assert_eq!(received_through(&only_send(&out)), 0, "a payload with a wrong byte was resumable");
    assert!(
        g.snapshot_incoming().is_none(),
        "a transfer whose bytes do not digest kept a prefix that cannot be trusted"
    );
}
