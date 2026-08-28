//! F0b — the round log's rules, one named test each, and the crash sweep that makes the
//! durability claims measurements rather than assertions.
//!
//! Every test here is named for the **rule** it pins, not for the method it calls, so that a
//! failure names the property that stopped holding. Each rule was also broken on purpose and the
//! test watched to fail — the mutants and what each printed are recorded in
//! `scratchpad/F0b-log.md`.

use std::collections::BTreeMap;

use super::*;
use crate::catalog::column::DataType;
use crate::consensus::config::Config;
use crate::consensus::{BranchOp, Command, Entry, NodeId, Round, Term};
use crate::storage::sim::{Durability, FaultPlan, OpKind, SimFabric, WriteShape};
use crate::wal::log::{DdlOp, RecKind};

const A: &str = "rounds.a";
const B: &str = "rounds.b";

fn open_on(fabric: &std::sync::Arc<SimFabric>) -> Result<RoundLog, LogError> {
    RoundLog::with_storage(fabric.open(A), fabric.open(B))
}

/// A payload whose length varies with the round, so a scan that walks by arithmetic rather than by
/// frame length is caught.
fn cmd(round: Round) -> Command {
    Command::WalBatch {
        start_lsn: round * 100,
        bytes: vec![(round % 251) as u8; 8 + (round as usize % 7) * 3],
    }
}

fn entry(term: Term, round: Round) -> Entry {
    Entry { term, round, command: cmd(round) }
}

fn fill(log: &mut RoundLog, term: Term, rounds: std::ops::RangeInclusive<Round>) {
    let batch: Vec<Entry> = rounds.map(|r| entry(term, r)).collect();
    log.append(&batch).expect("appending a contiguous batch");
    log.sync().expect("syncing");
}

/// Walk an image the way [`scan_frames`] does, returning `(offset, len)` per frame. Tests use it to
/// aim damage at a specific frame rather than at a byte number nobody can interpret.
fn frames_in(img: &[u8]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut at = HEADER_SIZE;
    while at + 4 <= img.len() {
        let total = u32::from_be_bytes(img[at..at + 4].try_into().unwrap()) as usize;
        if total < MIN_FRAME || at + total > img.len() {
            break;
        }
        out.push((at, total));
        at += total;
    }
    out
}

fn recrc(img: &mut [u8], off: usize, len: usize) {
    let crc = crc32(&img[off..off + len - 4]);
    img[off + len - 4..off + len].copy_from_slice(&crc.to_be_bytes());
}

/// Which of the two files the log is live on, read out of the images rather than out of the
/// `RoundLog` — a test that asked the object would be asking the thing under test.
fn live_file(images: &BTreeMap<String, Vec<u8>>) -> &'static str {
    let generation_of = |name: &str| -> Option<u64> {
        let b = images.get(name)?;
        if b.len() < HEADER_SIZE {
            return None;
        }
        let mut h = [0u8; HEADER_SIZE];
        h.copy_from_slice(&b[..HEADER_SIZE]);
        Header::decode(&h).ok().flatten().map(|h| h.generation)
    };
    match (generation_of(A), generation_of(B)) {
        (Some(a), Some(b)) if b > a => B,
        (Some(_), _) => A,
        (None, Some(_)) => B,
        (None, None) => panic!("neither file carries a header"),
    }
}

// ---------------------------------------------------------------------------------------------
// Contiguity — the property every other part of §F0 is built on
// ---------------------------------------------------------------------------------------------

#[test]
fn rounds_are_contiguous_from_one_and_an_append_that_would_leave_a_hole_is_refused() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    assert_eq!(log.first_round(), 1, "a fresh log must begin at round 1");
    assert_eq!(log.last_round(), 0, "round 0 means 'before the log begins'");

    // A hole at the very start.
    assert_eq!(
        log.append(&[entry(1, 2)]),
        Err(LogError::NonContiguous { expected: 1, got: 2 }),
        "the log accepted round 2 as its first round, which is a hole nothing later can see"
    );

    fill(&mut log, 1, 1..=3);
    assert_eq!(log.last_round(), 3);

    // A hole after an existing tail.
    assert_eq!(
        log.append(&[entry(1, 5)]),
        Err(LogError::NonContiguous { expected: 4, got: 5 })
    );
    // A hole *inside* a batch: the first entry is fine, the second is not.
    assert_eq!(
        log.append(&[entry(1, 4), entry(1, 6)]),
        Err(LogError::NonContiguous { expected: 5, got: 6 })
    );
}

#[test]
fn a_refused_batch_leaves_the_log_exactly_as_it_was() {
    // The check runs over the whole batch before a byte is written. Validating as it went would
    // leave the good prefix of a rejected batch durable -- a hole created by the guard against one.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=3);

    assert!(log.append(&[entry(1, 4), entry(1, 4)]).is_err());
    assert_eq!(log.last_round(), 3, "a refused batch moved the log's end");

    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.last_round(), 3, "a refused batch left a frame on the disk");
}

#[test]
fn terms_never_decrease_along_the_log() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    log.append(&[entry(5, 1)]).unwrap();
    assert_eq!(
        log.append(&[entry(4, 2)]),
        Err(LogError::TermWentBackwards { last: 5, got: 4 }),
        "a term went backwards along the log, which means a stale frame was accepted as a new one"
    );
    log.append(&[entry(5, 2), entry(9, 3)]).unwrap();
    assert_eq!(log.last_term(), 9);
}

// ---------------------------------------------------------------------------------------------
// The recovery scan
// ---------------------------------------------------------------------------------------------

#[test]
fn a_torn_tail_is_trimmed_and_everything_before_it_survives() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 3, 1..=6);
    let mut images = fabric.durable_image();
    let live = live_file(&images);

    let img = images.get_mut(live).unwrap();
    let frames = frames_in(img);
    assert_eq!(frames.len(), 6, "the fixture did not write six frames");
    // One byte inside the last frame's payload: full length, wrong bytes. Only the CRC can see it.
    let (off, _len) = frames[5];
    img[off + 24] ^= 0xFF;

    let broken = SimFabric::from_images(images, None, Durability::WriteThrough);
    let after = open_on(&broken).unwrap();
    assert_eq!(after.last_round(), 5, "the scan trusted a frame whose crc32 does not hold");
    for r in 1..=5 {
        assert_eq!(after.entry(r).unwrap(), entry(3, r), "round {r} did not survive the tear");
    }
    assert!(matches!(after.entry(6), Err(LogError::NotFound { .. })));
}

#[test]
fn a_frame_whose_round_does_not_match_its_position_ends_the_scan() {
    // The analogue of the WAL's embedded-LSN check, and the reason a hole in this log is visible by
    // arithmetic. The crc is *recomputed* after the edit, so this test can only pass if the round
    // check exists -- a crc check alone would already have stopped.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 2, 1..=5);
    let mut images = fabric.durable_image();
    let live = live_file(&images);
    let img = images.get_mut(live).unwrap();
    let frames = frames_in(img);
    let (off, len) = frames[2]; // round 3
    img[off + 12..off + 20].copy_from_slice(&9u64.to_be_bytes());
    recrc(img, off, len);

    let broken = SimFabric::from_images(images, None, Durability::WriteThrough);
    let after = open_on(&broken).unwrap();
    assert_eq!(
        after.last_round(),
        2,
        "the scan walked past a frame claiming round 9 where round 3 belongs, so a hole in this log \
         is not visible by arithmetic after all"
    );
}

#[test]
fn a_frame_whose_term_goes_backwards_ends_the_scan() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    log.append(&[entry(4, 1), entry(4, 2), entry(4, 3)]).unwrap();
    log.sync().unwrap();
    let mut images = fabric.durable_image();
    let live = live_file(&images);
    let img = images.get_mut(live).unwrap();
    let frames = frames_in(img);
    let (off, len) = frames[2];
    img[off + 4..off + 12].copy_from_slice(&1u64.to_be_bytes());
    recrc(img, off, len);

    let broken = SimFabric::from_images(images, None, Durability::WriteThrough);
    let after = open_on(&broken).unwrap();
    assert_eq!(after.last_round(), 2, "the scan accepted a frame whose term went backwards");
}

#[test]
fn a_frame_length_that_runs_past_the_file_ends_the_scan_without_allocating_for_it() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=4);
    let mut images = fabric.durable_image();
    let live = live_file(&images);
    let img = images.get_mut(live).unwrap();
    let frames = frames_in(img);
    let (off, _) = frames[3];
    // Four bytes of corruption asking for four gigabytes. A scan that trusts the number before
    // bounding it is a denial of service triggered by a torn write.
    img[off..off + 4].copy_from_slice(&u32::MAX.to_be_bytes());

    let broken = SimFabric::from_images(images, None, Durability::WriteThrough);
    let after = open_on(&broken).unwrap();
    assert_eq!(after.last_round(), 3);
}

#[test]
fn a_zero_terminator_stops_the_scan_before_a_frame_left_over_from_an_earlier_life() {
    // The narrow case the terminator exists for: bytes from a longer previous life of this file,
    // sitting exactly where the next frame would start, with the right round and a valid crc. The
    // fixture builds precisely that and requires the scan to stop.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=5);
    let long = fabric.durable_image();
    let live = live_file(&long);
    let long_img = long.get(live).unwrap().clone();

    // A second, shorter log on the same file: rounds 1..=3 only.
    let fabric2 = SimFabric::clean(Durability::WriteThrough);
    let mut log2 = open_on(&fabric2).unwrap();
    fill(&mut log2, 1, 1..=3);
    let short = fabric2.durable_image();
    let live2 = live_file(&short);
    let mut short_img = short.get(live2).unwrap().clone();

    // Splice the longer log's tail back on, so rounds 4 and 5 sit immediately after round 3 exactly
    // as they did before, crc and all.
    let boundary = short_img.len();
    assert!(
        long_img.len() > boundary,
        "the fixture needs the five-round image to be longer than the three-round one"
    );
    short_img.extend_from_slice(&long_img[boundary..]);
    // ...except for the four terminator bytes the shorter log wrote, which the splice overwrote.
    // Put them back: that is the state a real file is in after an append.
    let terminator_at = boundary - 4;
    short_img[terminator_at..boundary].copy_from_slice(&[0u8; 4]);

    let spliced: BTreeMap<String, Vec<u8>> =
        [(live2.to_string(), short_img)].into_iter().collect();
    let f = SimFabric::from_images(spliced, None, Durability::WriteThrough);
    let after = open_on(&f).unwrap();
    assert_eq!(
        after.last_round(),
        3,
        "the scan walked past the terminator and resurrected rounds that had been truncated away"
    );
}

#[test]
fn an_append_leaves_a_zero_terminator_on_the_disk() {
    // The mechanism the test above depends on, asserted directly so its absence is named here
    // rather than showing up as a confusing failure over there.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=2);
    let images = fabric.durable_image();
    let live = live_file(&images);
    let img = &images[live];
    let frames = frames_in(img);
    let end = frames.last().map(|(o, l)| o + l).unwrap();
    assert!(img.len() >= end + 4, "there is no room for a terminator after the last frame");
    assert_eq!(&img[end..end + 4], &[0u8; 4], "the four bytes after the last frame are not zero");
}

// ---------------------------------------------------------------------------------------------
// The floor, and the error that is the point of it
// ---------------------------------------------------------------------------------------------

#[test]
fn a_read_below_the_snapshot_floor_refuses_with_compacted_and_names_the_floor() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 7, 1..=10);
    log.discard_prefix(5, 7).unwrap();

    assert_eq!(log.snapshot_round(), 5);
    assert_eq!(log.snapshot_term(), 7);
    assert_eq!(log.first_round(), 6);

    let e = log.entry(3).unwrap_err();
    assert_eq!(e, LogError::Compacted { asked: 3, floor: 5 });
    assert_eq!(
        e.needs_snapshot(),
        Some(5),
        "the refusal did not tell the caller to send a snapshot, which is the only reason it is a \
         distinguishable error"
    );
    assert_eq!(log.entry(5).unwrap_err(), LogError::Compacted { asked: 5, floor: 5 });
    assert_eq!(log.range(1, 10, 1 << 20).unwrap_err(), LogError::Compacted { asked: 1, floor: 5 });
    assert_eq!(log.term_at(4).unwrap_err(), LogError::Compacted { asked: 4, floor: 5 });
    assert_eq!(log.truncate_from(4).unwrap_err(), LogError::Compacted { asked: 4, floor: 5 });

    // The floor itself is answerable without being in the log: it is the `prev_round` of the first
    // entry above it, and a log-matching check needs its term.
    assert_eq!(log.term_at(5).unwrap(), 7);
    assert_eq!(log.term_at(0).unwrap(), 0);
    assert_eq!(log.entry(6).unwrap(), entry(7, 6));
}

#[test]
fn a_checkpoint_keeps_the_suffix_across_a_restart() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 7, 1..=10);
    log.discard_prefix(5, 7).unwrap();
    drop(log);

    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.snapshot_round(), 5);
    assert_eq!(after.snapshot_term(), 7);
    assert_eq!(after.last_round(), 10);
    assert_eq!(after.len(), 5);
    for r in 6..=10 {
        assert_eq!(after.entry(r).unwrap(), entry(7, r), "round {r} did not survive the checkpoint");
    }
    assert_eq!(after.entry(5).unwrap_err(), LogError::Compacted { asked: 5, floor: 5 });
}

#[test]
fn a_checkpoint_can_be_appended_to_and_the_new_rounds_continue_the_numbering() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 7, 1..=10);
    log.discard_prefix(10, 7).unwrap();
    assert!(log.is_empty());
    assert_eq!(log.first_round(), 11);
    assert_eq!(log.last_round(), 10, "an emptied log's end is its floor");
    fill(&mut log, 8, 11..=13);

    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.last_round(), 13);
    assert_eq!(after.entry(11).unwrap(), entry(8, 11));
}

#[test]
fn a_checkpoint_above_the_end_of_the_log_empties_it_and_moves_the_floor() {
    // The `InstallSnapshot` case: the snapshot covers rounds this node never held.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 2, 1..=10);
    log.discard_prefix(100, 9).unwrap();
    assert!(log.is_empty());
    assert_eq!(log.snapshot_round(), 100);
    assert_eq!(log.term_at(100).unwrap(), 9);
    assert_eq!(log.entry(50).unwrap_err(), LogError::Compacted { asked: 50, floor: 100 });
    assert_eq!(log.last_term(), 9, "the election restriction reads the snapshot's term when empty");

    log.append(&[entry(9, 101)]).unwrap();
    log.sync().unwrap();
    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.snapshot_round(), 100);
    assert_eq!(after.entry(101).unwrap(), entry(9, 101));
}

#[test]
fn a_checkpoint_whose_term_disagrees_with_the_log_is_refused() {
    // A snapshot whose last round is in the log but whose term is not the log's term is a snapshot
    // of a different history. Installing it would leave the floor asserting a (round, term) pair
    // this node never held -- the pair every later log-matching check is decided by.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 4, 1..=6);
    assert_eq!(
        log.discard_prefix(3, 9),
        Err(LogError::TermMismatch { round: 3, held: 4, claimed: 9 })
    );
    assert_eq!(log.snapshot_round(), 0, "a refused checkpoint moved the floor");

    log.discard_prefix(3, 4).unwrap();
    // Idempotent at the same round and term, and refused at the same round with a different one.
    log.discard_prefix(3, 4).unwrap();
    assert_eq!(
        log.discard_prefix(3, 5),
        Err(LogError::TermMismatch { round: 3, held: 4, claimed: 5 })
    );
    log.discard_prefix(2, 4).unwrap();
    assert_eq!(log.snapshot_round(), 3, "a checkpoint below the floor moved it backwards");
}

// ---------------------------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------------------------

#[test]
fn truncating_a_conflicting_suffix_removes_it_durably() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=10);

    // Beyond the end is a caller working from a log it does not have.
    assert!(matches!(log.truncate_from(12), Err(LogError::NotFound { asked: 12, .. })));
    // At the end is the ordinary case and is a no-op.
    log.truncate_from(11).unwrap();
    assert_eq!(log.last_round(), 10);

    log.truncate_from(6).unwrap();
    assert_eq!(log.last_round(), 5);
    assert_eq!(log.durable_round(), 5, "durability did not follow the truncation down");
    assert!(matches!(log.entry(6), Err(LogError::NotFound { .. })));

    // The leader's replacement suffix, in a later term.
    log.append(&[entry(4, 6), entry(4, 7)]).unwrap();
    log.sync().unwrap();

    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.last_round(), 7, "the truncation did not survive a restart");
    assert_eq!(after.entry(6).unwrap(), entry(4, 6));
    assert_eq!(after.entry(7).unwrap(), entry(4, 7));
}

#[test]
fn a_truncation_that_cannot_be_made_durable_poisons_the_log() {
    // A half-true truncation is the state this refuses to be in: the file may be long while this
    // handle believes it is short, so appending would write over a tail whose removal never
    // reached the device.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=6);
    let images = fabric.durable_image();

    let census = SimFabric::from_images(images.clone(), None, Durability::WriteThrough);
    let mut l = open_on(&census).unwrap();
    let mark = census.op_count();
    l.truncate_from(4).unwrap();
    let target = census
        .faultable_ops()
        .into_iter()
        .find(|i| *i >= mark)
        .expect("a truncation performs at least one faultable operation");

    let plan = FaultPlan::at_shaped(target, 7, WriteShape::Drop);
    let faulted = SimFabric::from_images(images, Some(plan), Durability::WriteThrough);
    let mut broken = open_on(&faulted).unwrap();
    let err = broken.truncate_from(4).unwrap_err();
    assert!(
        matches!(err, LogError::Poisoned(_)),
        "a truncation whose set_len failed returned {err:?} instead of poisoning the log"
    );
    assert!(
        matches!(broken.append(&[entry(2, 4)]), Err(LogError::Poisoned(_))),
        "a poisoned log accepted an append over a tail it could not prove it had removed"
    );
    assert!(matches!(broken.sync(), Err(LogError::Poisoned(_))));
    assert!(faulted.fired().is_some(), "no fault fired, so this test proved nothing");
}

// ---------------------------------------------------------------------------------------------
// Durability
// ---------------------------------------------------------------------------------------------

#[test]
fn writes_that_were_never_synced_do_not_survive_a_crash() {
    // The fake filesystem that discards unsynced writes. `durable_round` is the log's promise and
    // it must be the thing that survives -- a follower that acked past it turns a correlated power
    // loss into acknowledged data loss.
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=5);
    assert_eq!(log.durable_round(), 5);

    log.append(&(6..=10).map(|r| entry(1, r)).collect::<Vec<_>>()).unwrap();
    assert_eq!(log.last_round(), 10, "the entries are in the log");
    assert_eq!(log.durable_round(), 5, "an append moved the durable frontier without an fsync");
    // They are readable in this process, which is what a leader needs to ship them.
    assert_eq!(log.entry(9).unwrap(), entry(1, 9));

    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(
        after.last_round(),
        5,
        "a round that was never fsynced came back from a crash, so `durable_round` is not a promise"
    );
    assert_eq!(after.durable_round(), 5);
}

#[test]
fn a_sync_makes_everything_appended_so_far_durable() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut log = open_on(&fabric).unwrap();
    log.append(&(1..=4).map(|r| entry(2, r)).collect::<Vec<_>>()).unwrap();
    assert_eq!(log.sync().unwrap(), 4);
    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.last_round(), 4);
    for r in 1..=4 {
        assert_eq!(after.entry(r).unwrap(), entry(2, r));
    }
}

// ---------------------------------------------------------------------------------------------
// The two-file switch
// ---------------------------------------------------------------------------------------------

#[test]
fn a_checkpoint_syncs_its_data_before_it_writes_the_header_that_makes_it_live() {
    // The whole of the switch's crash-safety is this order. Asserted from the recorded operation
    // trace rather than from the outcome, because the outcome is right by luck under a fabric that
    // never faults.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 3, 1..=8);
    let live_before = live_file(&fabric.durable_image());
    let spare = if live_before == A { B } else { A };
    let mark = fabric.op_count();
    log.discard_prefix(4, 3).unwrap();

    let trace: Vec<_> = fabric.trace().into_iter().filter(|o| o.index >= mark).collect();
    let frame_writes: Vec<u64> = trace
        .iter()
        .filter(|o| o.file == spare && o.kind == OpKind::Pwrite && o.offset >= HEADER_SIZE as u64)
        .map(|o| o.index)
        .collect();
    let header_writes: Vec<u64> = trace
        .iter()
        .filter(|o| o.file == spare && o.kind == OpKind::Pwrite && o.offset == 0)
        .map(|o| o.index)
        .collect();
    let syncs: Vec<u64> = trace
        .iter()
        .filter(|o| {
            o.file == spare && matches!(o.kind, OpKind::SyncData | OpKind::SyncAll)
        })
        .map(|o| o.index)
        .collect();

    assert!(!frame_writes.is_empty(), "the checkpoint wrote no surviving frames");
    assert_eq!(header_writes.len(), 1, "the header was written {} times", header_writes.len());
    let last_frame = *frame_writes.iter().max().unwrap();
    let header = header_writes[0];
    let sync_between = syncs.iter().any(|s| *s > last_frame && *s < header);
    assert!(
        sync_between,
        "the header at op {header} was written with no fsync between it and the last frame write \
         at op {last_frame}; a crash there would make a file live whose entries are not on the \
         device. syncs on the spare: {syncs:?}"
    );
}

#[test]
fn a_crash_at_any_point_during_a_checkpoint_loses_nothing() {
    // The sweep. Every faultable operation of a checkpoint, in every shape a write can misbehave,
    // with the requirement that reopening finds either the old log or the new one and never a
    // mixture -- and that no round above whichever floor survived has gone missing.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 6, 1..=10);
    // A first checkpoint, so the spare the swept one writes into holds a previous generation's
    // bytes. Sweeping onto an empty spare would never exercise the reclaim.
    log.discard_prefix(3, 6).unwrap();
    drop(log);
    let base = fabric.restart().durable_image();

    let census = SimFabric::from_images(base.clone(), None, Durability::WriteThrough);
    let mut l = open_on(&census).unwrap();
    let mark = census.op_count();
    l.discard_prefix(6, 6).unwrap();
    let points: Vec<u64> = census.faultable_ops().into_iter().filter(|i| *i >= mark).collect();
    assert!(points.len() >= 4, "a checkpoint performed only {} faultable operations", points.len());

    let mut ran = 0usize;
    for at in points {
        for shape in [WriteShape::Drop, WriteShape::Tear, WriteShape::Corrupt] {
            let plan = FaultPlan::at_shaped(at, 0xF0B, shape);
            let f = SimFabric::from_images(base.clone(), Some(plan), Durability::WriteThrough);
            let mut broken = open_on(&f).unwrap();
            let _ = broken.discard_prefix(6, 6);
            let fired = f.fired();
            assert!(
                fired.is_some(),
                "seed 0xF0B at op {at} shape {shape:?}: no fault fired, so this point tested nothing"
            );
            drop(broken);

            let restarted = f.restart();
            let after = match open_on(&restarted) {
                Ok(a) => a,
                Err(e) => panic!(
                    "seed 0xF0B at op {at} shape {shape:?} ({:?}) left a log that will not open: {e}",
                    fired.unwrap()
                ),
            };
            let floor = after.snapshot_round();
            assert!(
                floor == 3 || floor == 6,
                "seed 0xF0B at op {at} shape {shape:?} ({:?}) left floor {floor}, which is neither \
                 the checkpoint that was in place nor the one being installed",
                fired.clone().unwrap()
            );
            assert_eq!(
                after.last_round(),
                10,
                "seed 0xF0B at op {at} shape {shape:?} ({:?}) lost rounds off the end",
                fired.clone().unwrap()
            );
            for r in floor + 1..=10 {
                assert_eq!(
                    after.entry(r).unwrap(),
                    entry(6, r),
                    "seed 0xF0B at op {at} shape {shape:?} ({:?}) lost or changed round {r}",
                    fired.clone().unwrap()
                );
            }
            ran += 1;
        }
    }
    assert!(ran >= 12, "the sweep ran {ran} points, which is not a sweep");
}

#[test]
fn a_live_header_that_cannot_be_read_is_refused_rather_than_reinitialized_over() {
    // The dangerous alternative is not an error, it is silence: a log whose header is unreadable
    // and whose frames are intact would come back as an empty log, and an empty log is a node that
    // has forgotten everything the cluster agreed to.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=4);
    let mut images = fabric.durable_image();
    let live = live_file(&images);
    images.get_mut(live).unwrap()[9] ^= 0xFF;

    let broken = SimFabric::from_images(images, None, Durability::WriteThrough);
    match open_on(&broken) {
        Err(LogError::Corrupt(m)) => {
            assert!(m.contains("refusing to reinitialize"), "unexpected refusal: {m}")
        }
        Err(e) => panic!("expected a corruption refusal, got {e:?}"),
        Ok(l) => panic!(
            "a log with an unreadable header opened as {} entries at floor {}, silently discarding \
             what was on the disk",
            l.len(),
            l.snapshot_round()
        ),
    }
}

#[test]
fn two_files_claiming_one_generation_are_refused() {
    // Impossible from this code, so it means a file was copied over another and there is no
    // principled way to choose. Guessing would mean serving a log the cluster did not agree to.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=3);
    let images = fabric.durable_image();
    let live = live_file(&images);
    let copy = images[live].clone();
    let both: BTreeMap<String, Vec<u8>> =
        [(A.to_string(), copy.clone()), (B.to_string(), copy)].into_iter().collect();

    let f = SimFabric::from_images(both, None, Durability::WriteThrough);
    match open_on(&f) {
        Err(LogError::Corrupt(m)) => assert!(m.contains("generation"), "unexpected refusal: {m}"),
        other => panic!("expected a refusal of two files at one generation, got {other:?}"),
    }
}

#[test]
fn a_log_written_at_a_newer_format_version_is_refused_rather_than_reinitialized() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=2);
    let mut images = fabric.durable_image();
    let live = live_file(&images);
    {
        let img = images.get_mut(live).unwrap();
        img[4..8].copy_from_slice(&(VERSION + 1).to_be_bytes());
        let crc = crc32(&img[0..32]);
        img[32..36].copy_from_slice(&crc.to_be_bytes());
    }
    let f = SimFabric::from_images(images, None, Durability::WriteThrough);
    match open_on(&f) {
        Err(LogError::Corrupt(m)) => assert!(m.contains("format version"), "unexpected: {m}"),
        other => panic!("a newer on-disk format was not refused: {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------------------------

#[test]
fn a_range_is_bounded_by_both_limits_and_still_returns_one_entry_when_it_must() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=10);

    assert_eq!(log.range(3, 4, 1 << 20).unwrap().len(), 4);
    assert_eq!(log.range(3, 100, 1 << 20).unwrap().len(), 8);
    // A budget of one byte cannot hold anything, and a leader that returned nothing would stall
    // with no error the moment one large entry reached the head of the log.
    assert_eq!(log.range(3, 100, 1).unwrap().len(), 1);
    assert_eq!(log.range(3, 0, 1 << 20).unwrap().len(), 0, "a zero count asked for nothing");
    // Caught up is not an error.
    assert!(log.range(11, 10, 1 << 20).unwrap().is_empty());
    assert!(matches!(log.range(12, 10, 1 << 20), Err(LogError::NotFound { asked: 12, .. })));
    assert!(matches!(log.range(0, 10, 1 << 20), Err(LogError::NotFound { asked: 0, .. })));

    let got = log.range(4, 3, 1 << 20).unwrap();
    assert_eq!(got, vec![entry(1, 4), entry(1, 5), entry(1, 6)]);
}

#[test]
fn a_frame_is_cross_checked_against_the_position_the_index_says_it_holds() {
    // The scan runs once at open; the index is carried across every append, truncation and
    // checkpoint after that. An index that drifted would serve the wrong entry under a correct
    // round -- the one failure a checksum cannot see, because the bytes are exactly what was
    // written.
    let e = entry(3, 5);
    let frame = encode_frame(&e).unwrap();
    assert_eq!(decode_frame(&frame, 5, 3).unwrap(), e);
    assert!(
        matches!(decode_frame(&frame, 6, 3), Err(LogError::Corrupt(_))),
        "a frame for round 5 was served as round 6"
    );
    assert!(
        matches!(decode_frame(&frame, 5, 4), Err(LogError::Corrupt(_))),
        "a frame written in term 3 was served as term 4"
    );
}

// ---------------------------------------------------------------------------------------------
// The encoding
// ---------------------------------------------------------------------------------------------

fn every_command() -> Vec<Command> {
    vec![
        Command::WalBatch { start_lsn: 0, bytes: Vec::new() },
        Command::WalBatch { start_lsn: u64::MAX, bytes: vec![0xAB; 4096] },
        Command::Catalog {
            op: DdlOp::CreateTable,
            table: "orders".into(),
            columns: vec![
                ("id".into(), DataType::BigInt, false),
                ("name".into(), DataType::Varchar(64), true),
                ("at".into(), DataType::Timestamp, false),
                ("amt".into(), DataType::Decimal, true),
                ("ok".into(), DataType::Boolean, false),
                ("f".into(), DataType::Float, true),
                ("i".into(), DataType::Integer, false),
            ],
        },
        Command::Catalog { op: DdlOp::DropTable, table: "gone".into(), columns: Vec::new() },
        Command::Catalog {
            op: DdlOp::AlterColumn(crate::wal::log::ColumnAlteration::Rename {
                from: "old".into(),
                to: "new".into(),
            }),
            table: "t".into(),
            columns: vec![("new".into(), DataType::Integer, false)],
        },
        Command::Catalog {
            op: DdlOp::AlterColumn(crate::wal::log::ColumnAlteration::Retype {
                column: "c".into(),
                from: DataType::Varchar(8),
            }),
            table: "t".into(),
            columns: vec![("c".into(), DataType::BigInt, true)],
        },
        Command::Catalog {
            op: DdlOp::AlterColumn(crate::wal::log::ColumnAlteration::Add { column: "x".into() }),
            table: "t".into(),
            columns: vec![("x".into(), DataType::Float, true)],
        },
        Command::Branch {
            op: BranchOp::Fork { child: 9, parent: 1, fork_epoch: 42, lease_millis: 60_000 },
        },
        Command::Branch { op: BranchOp::Merge { branch: 9, base_round: 77 } },
        Command::Branch { op: BranchOp::Abandon { branch: 9 } },
        Command::Branch { op: BranchOp::Reap { branch: 9, generation: 3 } },
        Command::ArenaGrant { node: NodeId(2), first_page: 66, page_count: 1024 },
        Command::TxnIdRange { node: NodeId(3), lo: 1000, hi: 2000 },
        Command::LeaseTick { unix_millis: 1_756_000_000_000 },
        Command::Checkpoint,
        Command::Membership { config: Config::empty() },
        Command::Membership {
            config: Config::new([NodeId(1), NodeId(2), NodeId(3)], 4, 9)
                .with_learners([NodeId(7), NodeId(8)]),
        },
        Command::NoOp,
    ]
}

#[test]
fn every_command_variant_survives_a_round_trip_through_the_log_and_a_restart() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut log = open_on(&fabric).unwrap();
    let cmds = every_command();
    let entries: Vec<Entry> = cmds
        .iter()
        .enumerate()
        .map(|(i, c)| Entry { term: 1 + i as u64 / 4, round: 1 + i as u64, command: c.clone() })
        .collect();
    log.append(&entries).unwrap();
    log.sync().unwrap();

    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.last_round(), entries.len() as u64);
    for e in &entries {
        assert_eq!(
            &after.entry(e.round).unwrap(),
            e,
            "round {} did not come back as it went in",
            e.round
        );
    }
}

#[test]
fn a_membership_command_carries_its_learners() {
    // A configuration that shipped only its voters would arrive as one whose learners had been
    // removed, and removing a learner is a membership change nobody proposed.
    let cfg = Config::new([NodeId(1), NodeId(2)], 3, 4).with_learners([NodeId(5)]);
    let mut buf = Vec::new();
    encode_command(&Command::Membership { config: cfg.clone() }, &mut buf);
    let back = decode_command(&buf).unwrap();
    match back {
        Command::Membership { config } => {
            assert_eq!(config, cfg);
            assert_eq!(config.learners(), &[NodeId(5)]);
            assert_eq!(config.quorum(), 2, "the learner enlarged the quorum after a round trip");
        }
        other => panic!("a membership command decoded as {other:?}"),
    }
}

#[test]
fn the_command_decoder_refuses_trailing_bytes() {
    // Two encodings of one command mean two nodes can hold byte-different logs that decode
    // identically, which defeats every byte comparison built on top of this.
    let mut buf = Vec::new();
    encode_command(&Command::NoOp, &mut buf);
    assert_eq!(decode_command(&buf).unwrap(), Command::NoOp);
    buf.push(0);
    assert!(
        matches!(decode_command(&buf), Err(LogError::Corrupt(_))),
        "a command with a trailing byte decoded successfully"
    );
}

#[test]
fn the_command_decoder_refuses_a_truncated_record_rather_than_panicking() {
    let mut buf = Vec::new();
    encode_command(
        &Command::Membership {
            config: Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 1),
        },
        &mut buf,
    );
    for cut in 1..buf.len() {
        assert!(
            decode_command(&buf[..cut]).is_err(),
            "a command truncated to {cut} of {} bytes decoded successfully",
            buf.len()
        );
    }
}

#[test]
fn a_node_list_claiming_more_entries_than_the_record_holds_is_refused_before_it_is_allocated_for() {
    let mut buf = Vec::new();
    buf.push(7); // Membership
    buf.extend_from_slice(&1u64.to_be_bytes()); // version
    buf.extend_from_slice(&1u64.to_be_bytes()); // term
    buf.extend_from_slice(&u32::MAX.to_be_bytes()); // four billion members
    assert!(matches!(decode_command(&buf), Err(LogError::Corrupt(_))));
}

#[test]
fn an_unknown_tag_is_refused_at_every_level() {
    for (label, bytes) in [
        ("command", vec![99u8]),
        ("branch op", vec![2u8, 99]),
        ("ddl op", vec![1u8, 99]),
        ("column alteration", vec![1u8, 2, 99]),
        ("column type", vec![1u8, 0, 0, 1, b't', 0, 1, 0, 1, b'c', 99, 0]),
    ] {
        assert!(
            matches!(decode_command(&bytes), Err(LogError::Corrupt(_))),
            "an unknown {label} tag was accepted"
        );
    }
}

/// The tag the WAL gives a column type, derived from the WAL's own encoder rather than from a
/// number copied into this test.
fn wal_type_tag(ty: &DataType, reference: &DataType) -> (u8, u8) {
    let ser = |t: &DataType| {
        let mut b = Vec::new();
        RecKind::Ddl {
            op: DdlOp::CreateTable,
            table: "t".into(),
            dir_root: 0,
            time_travel_root: 0,
            columns: vec![("c".into(), t.clone(), false)],
        }
        .serialize(&mut b);
        b
    };
    let a = ser(reference);
    let b = ser(ty);
    let at = a
        .iter()
        .zip(b.iter())
        .position(|(x, y)| x != y)
        .expect("two different column types serialized identically");
    (a[at], b[at])
}

#[test]
fn the_column_type_tags_agree_with_the_wals() {
    // `Command::Catalog` and `RecKind::Ddl` describe the same schema change through two encoders,
    // because `wal::log`'s is private and `mod.rs` is frozen. If they disagreed about a tag, a
    // Timestamp column replicated through consensus would arrive as a Decimal in the change feed.
    let reference = DataType::Float;
    let mine = |t: &DataType| {
        let mut v = Vec::new();
        write_data_type(&mut v, t);
        v[0]
    };
    for ty in [
        DataType::Integer,
        DataType::Boolean,
        DataType::Varchar(7),
        DataType::BigInt,
        DataType::Decimal,
        DataType::Timestamp,
    ] {
        let (ref_tag, tag) = wal_type_tag(&ty, &reference);
        assert_eq!(
            mine(&reference),
            ref_tag,
            "this module gives {reference:?} tag {} and the wal gives it {ref_tag}",
            mine(&reference)
        );
        assert_eq!(
            mine(&ty),
            tag,
            "this module gives {ty:?} tag {} and the wal gives it {tag}",
            mine(&ty)
        );
    }
}

#[test]
fn an_entry_too_large_for_the_transport_is_refused_at_append() {
    // An entry larger than one replication frame is one no follower can ever be sent, so a log that
    // accepted it would hold a round that can never reach a quorum.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    let big = Entry {
        term: 1,
        round: 1,
        command: Command::WalBatch { start_lsn: 0, bytes: vec![0u8; MAX_ENTRY_BYTES] },
    };
    match log.append(&[big]) {
        Err(LogError::TooLarge { bytes, limit }) => {
            assert_eq!(limit, MAX_ENTRY_BYTES);
            assert!(bytes > limit);
        }
        other => panic!("an entry over the transport's frame size was accepted: {other:?}"),
    }
    assert_eq!(log.last_round(), 0);

    // One that fits is accepted, so the limit is a limit and not a wall.
    let ok = Entry {
        term: 1,
        round: 1,
        command: Command::WalBatch { start_lsn: 0, bytes: vec![0u8; MAX_ENTRY_BYTES - 4096] },
    };
    log.append(&[ok]).unwrap();
    log.sync().unwrap();
    assert_eq!(log.last_round(), 1);
}

#[test]
fn the_entry_encoding_transport_will_reuse_round_trips() {
    // `transport.rs` puts entries on a wire with these two functions rather than a second encoder.
    for (i, c) in every_command().into_iter().enumerate() {
        let e = Entry { term: 2, round: 1 + i as u64, command: c };
        let mut buf = Vec::new();
        encode_entry(&e, &mut buf);
        assert_eq!(decode_entry(&buf).unwrap(), e);
        buf.push(0);
        assert!(decode_entry(&buf).is_err(), "a trailing byte was accepted after an entry");
    }
}

// ---------------------------------------------------------------------------------------------
// The production entry point
// ---------------------------------------------------------------------------------------------

#[test]
fn the_log_opens_on_real_files_and_recovers_from_them() {
    // Everything above runs on the simulated fabric. This runs the path a database actually takes,
    // so a divergence between `impl Storage for File` and the fabric cannot hide.
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cluster.rounds");
    {
        let mut log = RoundLog::open(&base).unwrap();
        fill(&mut log, 5, 1..=8);
        log.discard_prefix(4, 5).unwrap();
        log.append(&[entry(6, 9)]).unwrap();
        log.sync().unwrap();
    }
    let reopened = RoundLog::open(&base).unwrap();
    assert_eq!(reopened.snapshot_round(), 4);
    assert_eq!(reopened.last_round(), 9);
    assert_eq!(reopened.entry(9).unwrap(), entry(6, 9));
    assert_eq!(reopened.entry(2).unwrap_err(), LogError::Compacted { asked: 2, floor: 4 });
    assert!(base.with_extension("rounds.a").exists() || dir.path().join("cluster.rounds.a").exists());
}

#[test]
fn a_long_life_of_appends_truncations_and_checkpoints_stays_consistent() {
    // A model check: a scripted history against a `Vec` that says what the log should hold, with a
    // restart after every step so nothing is only true in memory.
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut fabric = fabric;
    let mut model: Vec<Entry> = Vec::new();
    let mut floor: Round = 0;
    let mut floor_term: Term = 0;
    let mut term: Term = 1;

    for step in 0..24u64 {
        let mut log = open_on(&fabric).unwrap();
        assert_eq!(log.snapshot_round(), floor, "step {step}: floor drifted");
        assert_eq!(
            log.last_round(),
            floor + model.len() as u64,
            "step {step}: the log's end drifted from the model"
        );
        for e in &model {
            assert_eq!(&log.entry(e.round).unwrap(), e, "step {step}: round {} drifted", e.round);
        }

        match step % 4 {
            0 | 1 => {
                let start = floor + model.len() as u64 + 1;
                let batch: Vec<Entry> = (start..start + 3).map(|r| entry(term, r)).collect();
                log.append(&batch).unwrap();
                model.extend(batch);
            }
            2 => {
                if model.len() > 2 {
                    term += 1;
                    let from = model[model.len() - 2].round;
                    log.truncate_from(from).unwrap();
                    model.retain(|e| e.round < from);
                }
            }
            _ => {
                if model.len() > 1 {
                    let through = model[0].round;
                    let t = model[0].term;
                    log.discard_prefix(through, t).unwrap();
                    model.retain(|e| e.round > through);
                    floor = through;
                    floor_term = t;
                }
            }
        }
        log.sync().unwrap();
        assert_eq!(log.snapshot_term(), floor_term, "step {step}: the floor's term drifted");
        drop(log);
        fabric = fabric.restart();
    }
    assert!(!model.is_empty(), "the history ended with nothing in the log, so it proved little");
}
