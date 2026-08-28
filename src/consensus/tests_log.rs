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

/// An entry whose encoded frame is the **same length** whatever its term, round or `v`. Needed
/// wherever a fixture has to put a different entry in exactly the bytes an old one occupied, which
/// is the shape a lost truncation leaves behind.
fn fixed(term: Term, round: Round, v: u8) -> Entry {
    Entry {
        term,
        round,
        command: Command::WalBatch { start_lsn: round, bytes: vec![v; 32] },
    }
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
    // The narrow case the terminator exists for, staged exactly: a truncation whose `set_len` never
    // reached the device, followed by a replacement frame of the same size. Round 5's old frame is
    // then sitting precisely where the scan would look for round 5 next, with a valid crc and a
    // term that does not go backwards -- so every other check in the scan passes it.
    //
    // The test proves its own fixture. The same image WITHOUT the four zero bytes must resurrect
    // round 5; if that half ever stops holding, the other half is measuring nothing. An earlier
    // version of this test spliced two images four bytes out of alignment and passed for that
    // reason, which is why the dangerous state is now built and *demonstrated* before it is denied.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    let original: Vec<Entry> = (1..=5).map(|r| fixed(1, r, 0xA1)).collect();
    log.append(&original).unwrap();
    log.sync().unwrap();

    let images = fabric.durable_image();
    let live = live_file(&images);
    let base = images[live].clone();
    let frames = frames_in(&base);
    assert_eq!(frames.len(), 5, "the fixture did not write five frames");
    let width = frames[0].1;
    assert!(frames.iter().all(|(_, n)| *n == width), "the fixture needs equal-length frames");

    // A replacement round 4: same term, same round, same encoded length, different bytes -- what an
    // append writes over a suffix whose removal was lost.
    let replacement = fixed(1, 4, 0x5E);
    let raw = encode_frame(&replacement).unwrap();
    assert_eq!(raw.len(), width, "the replacement is not the width of the frame it overwrites");
    let mut resurrecting = base.clone();
    resurrecting[frames[3].0..frames[3].0 + width].copy_from_slice(&raw);

    // Half one: without the terminator the old round 5 is reachable and the scan takes it.
    let f = SimFabric::from_images(
        [(live.to_string(), resurrecting.clone())].into_iter().collect(),
        None,
        Durability::WriteThrough,
    );
    let bad = open_on(&f).unwrap();
    assert_eq!(
        bad.last_round(),
        5,
        "the fixture does not stage a resurrection at all, so the other half of this test proves \
         nothing -- the leftover frame was rejected by some other check"
    );
    assert_eq!(bad.entry(4).unwrap(), replacement);

    // Half two: the four bytes an append writes after its last frame land on the leftover's length
    // field, and the scan stops.
    let mut terminated = resurrecting;
    terminated[frames[4].0..frames[4].0 + 4].copy_from_slice(&[0u8; 4]);
    let f2 = SimFabric::from_images(
        [(live.to_string(), terminated)].into_iter().collect(),
        None,
        Durability::WriteThrough,
    );
    let good = open_on(&f2).unwrap();
    assert_eq!(
        good.last_round(),
        4,
        "the scan walked past the terminator and resurrected a round that had been truncated away"
    );
    assert_eq!(good.entry(4).unwrap(), replacement);
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
    //
    // **Both durability models, and that is not thoroughness for its own sake.** Under
    // `WriteThrough` every write is durable the instant it returns, so an fsync deleted from the
    // checkpoint would change no outcome here and the sweep would call a missing flush green.
    // `SyncOnly` is the model in which the ordering of the two fsyncs is load-bearing.
    let mut total = 0usize;
    for durability in [Durability::WriteThrough, Durability::SyncOnly] {
        total += sweep_a_checkpoint(durability);
    }
    assert!(total >= 24, "the sweep ran {total} points across both models, which is not a sweep");
}

fn sweep_a_checkpoint(durability: Durability) -> usize {
    let fabric = SimFabric::clean(durability);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 6, 1..=10);
    // A first checkpoint, so the swept one runs on a log that has already switched files once --
    // which is what puts the *retirement* of the superseded file inside the swept window too.
    log.discard_prefix(3, 6).unwrap();
    drop(log);
    let base = fabric.restart().durable_image();

    let census = SimFabric::from_images(base.clone(), None, durability);
    let mut l = open_on(&census).unwrap();
    let mark = census.op_count();
    l.discard_prefix(6, 6).unwrap();
    let points: Vec<u64> = census.faultable_ops().into_iter().filter(|i| *i >= mark).collect();
    assert!(
        points.len() >= 4,
        "a checkpoint under {durability:?} performed only {} faultable operations",
        points.len()
    );

    let mut ran = 0usize;
    for at in points {
        for shape in [WriteShape::Drop, WriteShape::Tear, WriteShape::Corrupt] {
            let plan = FaultPlan::at_shaped(at, 0xF0B, shape);
            let f = SimFabric::from_images(base.clone(), Some(plan), durability);
            let mut broken = open_on(&f).unwrap();
            let _ = broken.discard_prefix(6, 6);
            let fired = f.fired();
            let where_ = format!("{durability:?} seed 0xF0B at op {at} shape {shape:?}");
            assert!(fired.is_some(), "{where_}: no fault fired, so this point tested nothing");
            drop(broken);

            let restarted = f.restart();
            let after = match open_on(&restarted) {
                Ok(a) => a,
                Err(e) => panic!("{where_} ({:?}) left a log that will not open: {e}", fired.unwrap()),
            };
            let floor = after.snapshot_round();
            assert!(
                floor == 3 || floor == 6,
                "{where_} ({:?}) left floor {floor}, which is neither the checkpoint that was in \
                 place nor the one being installed",
                fired.clone().unwrap()
            );
            assert_eq!(
                after.last_round(),
                10,
                "{where_} ({:?}) lost rounds off the end",
                fired.clone().unwrap()
            );
            for r in floor + 1..=10 {
                assert_eq!(
                    after.entry(r).unwrap(),
                    entry(6, r),
                    "{where_} ({:?}) lost or changed round {r}",
                    fired.clone().unwrap()
                );
            }
            ran += 1;
        }
    }
    ran
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
    encode_command(&Command::Membership { config: cfg.clone() }, &mut buf).unwrap();
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
    encode_command(&Command::NoOp, &mut buf).unwrap();
    assert_eq!(decode_command(&buf).unwrap(), Command::NoOp);
    buf.push(0);
    assert!(
        matches!(decode_command(&buf), Err(LogError::Corrupt(_))),
        "a command with a trailing byte decoded successfully"
    );
}

#[test]
fn the_command_decoder_refuses_a_truncated_record_rather_than_panicking() {
    // Every variant, at every cut. These bytes arrive from a disk or a socket, so a decoder that
    // indexes past the end of a short record is a denial of service triggered by a corrupt log --
    // and the Catalog arms are the ones that matter most, being the only ones with strings and a
    // counted loop.
    for c in every_command() {
        let mut buf = Vec::new();
        encode_command(&c, &mut buf).unwrap();
        assert_eq!(decode_command(&buf).unwrap(), c, "a command did not round trip");
        for cut in 1..buf.len() {
            assert!(
                decode_command(&buf[..cut]).is_err(),
                "{c:?} truncated to {cut} of {} bytes decoded successfully",
                buf.len()
            );
        }
    }
}

#[test]
fn a_node_list_claiming_more_entries_than_the_record_holds_is_refused_rather_than_allocated_for() {
    // Stated blind spot, because it is what shaped the code: no assertion available here can see
    // how much memory a decoder reserved on its way to an error. A mutant that deleted a
    // `Vec::with_capacity` bound placed beside the loop SURVIVED this test for exactly that reason.
    // The remedy was structural rather than a stronger assertion -- `read_ids` now proves the bytes
    // are present and takes its capacity from that slice, so there is no separable check to delete.
    // What this test can prove is the refusal, and that it is the *node list* that refused rather
    // than some later field tripping over the same bad bytes.
    let mut buf = Vec::new();
    buf.push(7); // Membership
    buf.extend_from_slice(&1u64.to_be_bytes()); // version
    buf.extend_from_slice(&1u64.to_be_bytes()); // term
    buf.extend_from_slice(&u32::MAX.to_be_bytes()); // four billion members
    match decode_command(&buf) {
        Err(LogError::Corrupt(m)) => assert!(m.contains("node list"), "unexpected refusal: {m}"),
        other => panic!("a node list claiming four billion entries decoded as {other:?}"),
    }

    // And a list one entry longer than the bytes behind it, which is the realistic shape of the
    // same fault: a record that ends mid-field.
    let mut short = Vec::new();
    short.push(7);
    short.extend_from_slice(&1u64.to_be_bytes());
    short.extend_from_slice(&1u64.to_be_bytes());
    short.extend_from_slice(&3u32.to_be_bytes());
    short.extend_from_slice(&1u32.to_be_bytes());
    short.extend_from_slice(&2u32.to_be_bytes());
    match decode_command(&short) {
        Err(LogError::Corrupt(m)) => assert!(m.contains("node list"), "unexpected refusal: {m}"),
        other => panic!("a node list that runs past its record decoded as {other:?}"),
    }
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

/// The bytes the WAL uses to encode a column type, derived from the WAL's own encoder rather than
/// from a number copied into this test.
///
/// Two records differing only in one column's type share a prefix and a suffix; the middle is
/// exactly the type's encoding. Comparing the whole middle rather than the first differing byte is
/// what makes a divergence in `Varchar`'s **width** visible -- comparing tags alone would let the
/// two files disagree about whether the width is a `u8` or a `u16` and call it agreement.
fn wal_type_bytes(ty: &DataType, reference: &DataType) -> (Vec<u8>, Vec<u8>) {
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
    let pre = a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count();
    let suf = a
        .iter()
        .rev()
        .zip(b.iter().rev())
        .take_while(|(x, y)| x == y)
        .count()
        .min(a.len() - pre)
        .min(b.len() - pre);
    (a[pre..a.len() - suf].to_vec(), b[pre..b.len() - suf].to_vec())
}

#[test]
fn the_column_type_tags_agree_with_the_wals() {
    // `Command::Catalog` and `RecKind::Ddl` describe the same schema change through two encoders,
    // because `wal::log`'s is private and `mod.rs` is frozen. If they disagreed, a Timestamp column
    // replicated through consensus would arrive as a Decimal in the change feed.
    let reference = DataType::Float;
    let mine = |t: &DataType| {
        let mut v = Vec::new();
        write_data_type(&mut v, t);
        v
    };
    for ty in [
        DataType::Integer,
        DataType::Boolean,
        DataType::Varchar(7),
        DataType::Varchar(65535),
        DataType::BigInt,
        DataType::Decimal,
        DataType::Timestamp,
    ] {
        let (ref_bytes, ty_bytes) = wal_type_bytes(&ty, &reference);
        assert_eq!(
            mine(&reference),
            ref_bytes,
            "this module encodes {reference:?} as {:?} and the wal encodes it as {ref_bytes:?}",
            mine(&reference)
        );
        assert_eq!(
            mine(&ty),
            ty_bytes,
            "this module encodes {ty:?} as {:?} and the wal encodes it as {ty_bytes:?}",
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
        encode_entry(&e, &mut buf).unwrap();
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

// ---------------------------------------------------------------------------------------------
// Rules the adversarial review found untested, and the two data-loss windows it found untried
// ---------------------------------------------------------------------------------------------

#[test]
fn the_live_file_is_the_one_at_the_greater_generation() {
    // The comparison that decides which file is the log. It is never exercised by an ordinary
    // reopen -- a retired file carries no header, so the `(Some, None)` arm answers -- which is
    // exactly why it needs a fixture that puts two valid headers side by side.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 2, 1..=6);
    let gen1 = fabric.durable_image();
    assert_eq!(live_file(&gen1), A, "the fixture assumed the first file is initialized live");
    log.discard_prefix(4, 2).unwrap();
    let gen2 = fabric.durable_image();
    assert_eq!(live_file(&gen2), B);

    // A's generation-1 image, which still holds rounds 1..=6 at floor 0, beside B's generation-2
    // image at floor 4. Both headers validate; only the generation separates them.
    let both: BTreeMap<String, Vec<u8>> =
        [(A.to_string(), gen1[A].clone()), (B.to_string(), gen2[B].clone())].into_iter().collect();
    let f = SimFabric::from_images(both, None, Durability::WriteThrough);
    let l = open_on(&f).unwrap();
    assert_eq!(l.live_generation(), 2, "the log opened on the older generation");
    assert_eq!(l.snapshot_round(), 4);
    assert_eq!(l.last_round(), 6);
    assert_eq!(l.entry(5).unwrap(), entry(2, 5));
    assert_eq!(l.entry(4).unwrap_err(), LogError::Compacted { asked: 4, floor: 4 });
}

#[test]
fn a_superseded_file_is_retired_so_a_damaged_header_cannot_rewind_the_log() {
    // The module header states this as a safety property: once the switch is durable the old file
    // is emptied, so an unreadable live header is a hard refusal rather than a quiet fall-back to a
    // generation that has forgotten every round appended since. Nothing enforced it until the
    // retirement was made durable -- under a device that only makes writes durable at an fsync, an
    // unsynced `set_len(0)` leaves the whole previous generation on the platter.
    for durability in [Durability::WriteThrough, Durability::SyncOnly] {
        let fabric = SimFabric::clean(durability);
        let mut log = open_on(&fabric).unwrap();
        fill(&mut log, 7, 1..=8);
        log.discard_prefix(4, 7).unwrap();
        // Rounds that exist ONLY in the new generation. These are what a fall-back would forget.
        fill(&mut log, 7, 9..=12);
        drop(log);

        let mut images = fabric.restart().durable_image();
        let live = live_file(&images);
        let stale = if live == A { B } else { A };
        assert_eq!(
            images.get(stale).map_or(0, |b| b.len()),
            0,
            "under {durability:?} the superseded file survived the switch, so it is still a \
             candidate the recovery could fall back to"
        );

        images.get_mut(live).unwrap()[9] ^= 0xFF;
        let f = SimFabric::from_images(images, None, durability);
        match open_on(&f) {
            Err(LogError::Corrupt(m)) => {
                assert!(m.contains("refusing to reinitialize"), "unexpected refusal: {m}")
            }
            Err(e) => panic!("expected a corruption refusal under {durability:?}, got {e:?}"),
            Ok(l) => panic!(
                "under {durability:?} a damaged live header fell back to {l:?}, silently forgetting \
                 rounds 9..=12"
            ),
        }
    }
}

#[test]
fn a_checkpoint_that_cannot_be_made_durable_poisons_rather_than_returning_an_ordinary_error() {
    // The window: `pwrite` of the generation+1 header returns Ok and reaches the device, then its
    // fsync fails. The switch has happened on disk and not in memory. A handle that returned a
    // plain error here and kept going would append rounds into the file it still believes is live,
    // `sync()` would return Ok, the node would ack them -- and the next open would pick the OTHER
    // file at the greater generation and come up holding none of them.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 3, 1..=8);
    let base = fabric.restart().durable_image();

    let census = SimFabric::from_images(base.clone(), None, Durability::WriteThrough);
    let mut l = open_on(&census).unwrap();
    let mark = census.op_count();
    l.discard_prefix(4, 3).unwrap();
    let spare = if live_file(&base) == A { B } else { A };
    let header_write = census
        .trace()
        .into_iter()
        .find(|o| {
            o.index >= mark
                && o.file == spare
                && o.kind == OpKind::Pwrite
                && o.offset == 0
                && o.len == HEADER_SIZE
        })
        .expect("a checkpoint writes the spare's header exactly once");

    // The fault fires at the first faultable op at or above the header write. That is the header
    // write itself, or -- because the plan cannot name a sync directly -- the fsync behind it.
    for at in [header_write.index, header_write.index + 1] {
        let f = SimFabric::from_images(
            base.clone(),
            Some(FaultPlan::at_shaped(at, 11, WriteShape::Drop)),
            Durability::WriteThrough,
        );
        let mut broken = open_on(&f).unwrap();
        let err = broken.discard_prefix(4, 3).unwrap_err();
        assert!(f.fired().is_some(), "op {at}: no fault fired, so this point tested nothing");
        assert!(
            matches!(err, LogError::Poisoned(_)),
            "a checkpoint that failed at or after the header write returned {err:?} instead of \
             poisoning; a caller that kept this handle would ack rounds the next open cannot find"
        );
        // `check_live` runs before any I/O, so this is the handle refusing and not the storage.
        assert!(matches!(broken.append(&[entry(3, 9)]), Err(LogError::Poisoned(_))));
        assert!(matches!(broken.sync(), Err(LogError::Poisoned(_))));
        assert!(matches!(broken.discard_prefix(6, 3), Err(LogError::Poisoned(_))));
    }
}

#[test]
fn a_failed_fsync_poisons_rather_than_letting_the_next_one_report_success() {
    // A writeback error is delivered to one fsync and then cleared, and the pages are marked clean.
    // So the obvious recovery -- call `sync()` again -- returns Ok about bytes that never reached
    // the device, and would move `durable_round` over exactly the rounds the failure dropped.
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=4);
    let base = fabric.restart().durable_image();

    let census = SimFabric::from_images(base.clone(), None, Durability::SyncOnly);
    let mut l = open_on(&census).unwrap();
    l.append(&[entry(1, 5)]).unwrap();
    let mark = census.op_count();
    l.sync().unwrap();
    let target = census
        .faultable_ops()
        .into_iter()
        .find(|i| *i >= mark)
        .expect("a sync performs at least one faultable operation");

    let f = SimFabric::from_images(
        base,
        Some(FaultPlan::at_shaped(target, 13, WriteShape::Drop)),
        Durability::SyncOnly,
    );
    let mut broken = open_on(&f).unwrap();
    broken.append(&[entry(1, 5)]).unwrap();
    let err = broken.sync().unwrap_err();
    assert!(f.fired().is_some(), "no fault fired, so this test proved nothing");
    assert!(
        matches!(err, LogError::Poisoned(_)),
        "a failed fsync returned {err:?}; a caller that simply retried would be told the rounds are \
         durable when they are not"
    );
    assert_eq!(broken.durable_round(), 4, "a failed fsync advanced the durable frontier");
    assert!(matches!(broken.sync(), Err(LogError::Poisoned(_))), "a retried fsync reported success");
}

#[test]
fn a_torn_first_header_write_does_not_brick_a_log_that_has_nothing_in_it() {
    // The refusal that protects a full log from being reinitialized over must not fire on a log
    // whose very first header write was caught by a crash. Frames begin at HEADER_SIZE, so a file
    // no longer than the header has never held an entry -- that is the boundary, and it is why the
    // check is `> HEADER_SIZE` rather than `> 0`.
    let census = SimFabric::clean(Durability::WriteThrough);
    let _ = open_on(&census).unwrap();
    let header_write = census
        .trace()
        .into_iter()
        .find(|o| o.kind == OpKind::Pwrite && o.len == HEADER_SIZE)
        .expect("opening a fresh log writes a header");

    let f = SimFabric::with_fault(
        FaultPlan::at_shaped(header_write.index, 17, WriteShape::Tear),
        Durability::WriteThrough,
    );
    assert!(open_on(&f).is_err(), "the torn write did not fail the open");
    let fired = f.fired().expect("no fault fired, so this test proved nothing");
    assert!(fired.kept > 0 && fired.kept < HEADER_SIZE, "the header write was not torn: {fired:?}");

    let restarted = f.restart();
    let survivors: usize = restarted.durable_image().values().map(|b| b.len()).sum();
    assert!(survivors > 0, "nothing survived, so the reopen is not the case this test is about");
    let l = open_on(&restarted).expect("a torn first header bricked a log that held nothing");
    assert_eq!(l.last_round(), 0);
    assert_eq!(l.first_round(), 1);
}

#[test]
fn the_scan_refuses_a_frame_longer_than_the_maximum_before_allocating_for_it() {
    // MAX_FRAME is what stands between a corrupt length field and an eight-megabyte allocation. In
    // a small file the companion clause `offset + total > file_len` answers first, so this calls
    // the scan with a file length it does NOT have -- leaving MAX_FRAME as the only clause that can
    // fire, which is the only way to know it works.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=2);
    let images = fabric.durable_image();
    let live = live_file(&images);
    let frames = frames_in(&images[live]);
    let mut damaged = images[live].clone();
    let (off, _) = frames[1];
    damaged[off..off + 4].copy_from_slice(&((MAX_FRAME + 1) as u32).to_be_bytes());

    let f = SimFabric::from_images(
        [(live.to_string(), damaged)].into_iter().collect(),
        None,
        Durability::WriteThrough,
    );
    let st = f.open(live);
    let scan = scan_frames(&*st, 0, 0, u64::MAX / 2).expect("the scan errored instead of stopping");
    assert_eq!(
        scan.frames.len(),
        1,
        "the scan accepted a frame claiming {} bytes, over the {MAX_FRAME}-byte maximum",
        MAX_FRAME + 1
    );
}

#[test]
fn a_header_with_the_wrong_magic_is_not_a_header() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=3);
    let mut images = fabric.durable_image();
    let live = live_file(&images);
    {
        let img = images.get_mut(live).unwrap();
        img[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        let crc = crc32(&img[0..32]);
        img[32..36].copy_from_slice(&crc.to_be_bytes());
    }
    let f = SimFabric::from_images(images, None, Durability::WriteThrough);
    match open_on(&f) {
        Err(LogError::Corrupt(m)) => {
            assert!(m.contains("refusing to reinitialize"), "unexpected refusal: {m}")
        }
        other => panic!("a file whose magic is another format's opened as {other:?}"),
    }
}

#[test]
fn a_checkpoint_streams_survivors_larger_than_one_copy_batch() {
    // The mid-loop flush in `discard_prefix` writes at an offset computed from a running total, and
    // an off-by-one there would land a megabyte of frames in the wrong place. Nothing smaller than
    // COPY_BATCH of survivors reaches that branch at all.
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut log = open_on(&fabric).unwrap();
    let big: Vec<Entry> = (1..=40)
        .map(|r| Entry {
            term: 1,
            round: r,
            command: Command::WalBatch { start_lsn: r, bytes: vec![(r % 251) as u8; 64 * 1024] },
        })
        .collect();
    log.append(&big).unwrap();
    log.sync().unwrap();
    assert!(
        35 * 64 * 1024 > COPY_BATCH,
        "the fixture's survivors do not exceed one batch, so the flush branch is not reached"
    );
    log.discard_prefix(5, 1).unwrap();

    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.snapshot_round(), 5);
    assert_eq!(after.last_round(), 40);
    for e in big.iter().filter(|e| e.round > 5) {
        assert_eq!(&after.entry(e.round).unwrap(), e, "round {} moved in the rewrite", e.round);
    }
}

#[test]
fn a_value_the_length_fields_cannot_express_is_refused_before_it_becomes_durable() {
    // `wal::log`'s `write_str` writes `s.len() as u16`. A 65536-byte name would be written with a
    // length prefix of ZERO, and the frame -- crc valid over exactly the bytes intended -- would be
    // durable and permanently undecodable. Nothing downstream can catch it, because everything
    // downstream checks the bytes against a checksum of themselves.
    let too_long = Command::Catalog {
        op: DdlOp::CreateTable,
        table: "a".repeat(u16::MAX as usize + 1),
        columns: Vec::new(),
    };
    let mut out = Vec::new();
    assert!(
        matches!(
            encode_command(&too_long, &mut out),
            Err(LogError::Unrepresentable { what: "table name", .. })
        ),
        "a table name too long for the length field was encoded anyway"
    );

    let too_many = Command::Catalog {
        op: DdlOp::CreateTable,
        table: "t".into(),
        columns: (0..=u16::MAX as usize)
            .map(|i| (format!("c{i}"), DataType::Integer, false))
            .collect(),
    };
    out.clear();
    assert!(matches!(
        encode_command(&too_many, &mut out),
        Err(LogError::Unrepresentable { what: "column count", .. })
    ));

    // And through the log, where the refusal has to happen before anything is written.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    assert!(matches!(
        log.append(&[Entry { term: 1, round: 1, command: too_long }]),
        Err(LogError::Unrepresentable { .. })
    ));
    assert_eq!(log.last_round(), 0, "a refused entry moved the log's end");

    // The boundary is a boundary and not a wall: the longest representable name round trips.
    let at_the_limit = Command::Catalog {
        op: DdlOp::CreateTable,
        table: "a".repeat(u16::MAX as usize),
        columns: Vec::new(),
    };
    log.append(&[Entry { term: 1, round: 1, command: at_the_limit.clone() }]).unwrap();
    log.sync().unwrap();
    let restarted = fabric.restart();
    let after = open_on(&restarted).unwrap();
    assert_eq!(after.entry(1).unwrap().command, at_the_limit);
}

#[test]
fn crossing_into_ferro_error_keeps_a_corruption_a_corruption() {
    // `into_ferro` is the one deliberate, greppable crossing out of `LogError`. The mapping it
    // performs is the thing that keeps a damaged log from being reported to an operator as an
    // ordinary write failure -- and a corruption nobody pages on is the failure mode `FerroError`'s
    // own doc comment says the `Corruption` variant exists to prevent.
    assert!(matches!(
        LogError::Corrupt("bad frame".into()).into_ferro(),
        FerroError::Corruption(_)
    ));
    assert!(matches!(
        LogError::Compacted { asked: 1, floor: 5 }.into_ferro(),
        FerroError::Wal(_)
    ));
    assert!(matches!(LogError::Io("device".into()).into_ferro(), FerroError::Wal(_)));
    assert!(matches!(LogError::Poisoned("x".into()).into_ferro(), FerroError::Wal(_)));
}

#[test]
fn a_device_error_reading_the_log_is_reported_as_io_and_not_as_corruption() {
    // `pread_all` and the bounds-checked decoders both speak `FerroError`, and mapping every one of
    // them to `Corrupt` sends an operator looking for a bad checksum that does not exist.
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 1, 1..=3);
    let images = fabric.durable_image();
    let live = live_file(&images);
    // Truncate the image so the last frame's bytes are simply not there. The index still points at
    // them, so the read fails at the device rather than at a checksum.
    let mut short = images[live].clone();
    let frames = frames_in(&short);
    short.truncate(frames[2].0 + 4);
    let f = SimFabric::from_images(
        [(live.to_string(), short)].into_iter().collect(),
        None,
        Durability::WriteThrough,
    );
    let st = f.open(live);
    let mut buf = vec![0u8; frames[2].1];
    match read_at(&*st, &mut buf, frames[2].0 as u64) {
        Err(LogError::Io(_)) => {}
        other => panic!("a read that ran off the end of the device was reported as {other:?}"),
    }
}

#[test]
fn the_generation_rises_by_one_at_every_checkpoint_and_survives_a_restart() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut fabric = fabric;
    let mut floor = 0u64;
    let mut expected = 1u64;
    for step in 0..5u64 {
        let mut log = open_on(&fabric).unwrap();
        assert_eq!(log.live_generation(), expected, "step {step}: the generation drifted");
        let start = floor + log.len() as u64 + 1;
        log.append(&(start..start + 3).map(|r| entry(1, r)).collect::<Vec<_>>()).unwrap();
        log.sync().unwrap();
        floor = start;
        log.discard_prefix(floor, 1).unwrap();
        expected += 1;
        assert_eq!(log.live_generation(), expected);
        drop(log);
        fabric = fabric.restart();
    }
}
