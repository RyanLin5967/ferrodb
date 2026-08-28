//! F3 — the transport's rules, one named test each.
//!
//! Every test below names a rule from `DISTRIBUTED.md` §F3 or from the module header of
//! `transport.rs`, and every one of them has been seen to **fail** with that rule deliberately
//! broken — the mutants and what each printed are recorded in `scratchpad/F3-transport.md`. A test
//! that has never failed is not evidence.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use super::*;
use crate::catalog::column::DataType;
use crate::consensus::config::Config;
use crate::consensus::snapshot::SnapshotMeta;
use crate::consensus::{Body, BranchOp, Command, Entry, Message, NodeId};
use crate::replication::{CONSENSUS_TAG, MAX_FRAME_BYTES, REPL_MAGIC, REPL_VERSION};
use crate::wal::log::{ColumnAlteration, DdlOp};

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

fn cfg() -> Config {
    Config::new([NodeId(1), NodeId(2), NodeId(3)], 7, 4).with_learners([NodeId(9)])
}

fn every_command() -> Vec<Command> {
    vec![
        Command::WalBatch { start_lsn: 0, bytes: Vec::new() },
        Command::WalBatch { start_lsn: u64::MAX, bytes: vec![0xAA; 300] },
        Command::Catalog {
            op: DdlOp::CreateTable,
            table: "accounts".into(),
            columns: vec![
                ("id".into(), DataType::Integer, false),
                ("name".into(), DataType::Varchar(64), true),
                ("balance".into(), DataType::Decimal, true),
                ("seen".into(), DataType::Timestamp, true),
                ("big".into(), DataType::BigInt, false),
                ("ok".into(), DataType::Boolean, true),
                ("rate".into(), DataType::Float, true),
            ],
        },
        Command::Catalog { op: DdlOp::DropTable, table: "gone".into(), columns: Vec::new() },
        Command::Catalog {
            op: DdlOp::AlterColumn(ColumnAlteration::Add { column: "note".into() }),
            table: "t".into(),
            columns: vec![("note".into(), DataType::Varchar(20), true)],
        },
        Command::Catalog {
            op: DdlOp::AlterColumn(ColumnAlteration::Rename { from: "a".into(), to: "b".into() }),
            table: "t".into(),
            columns: vec![("b".into(), DataType::Integer, false)],
        },
        Command::Catalog {
            op: DdlOp::AlterColumn(ColumnAlteration::Retype {
                column: "c".into(),
                from: DataType::Varchar(4),
            }),
            table: "t".into(),
            columns: vec![("c".into(), DataType::Varchar(40), true)],
        },
        Command::Branch {
            op: BranchOp::Fork { child: 5, parent: 1, fork_epoch: 88, lease_millis: 60_000 },
        },
        Command::Branch { op: BranchOp::Merge { branch: 5, base_round: 42 } },
        Command::Branch { op: BranchOp::Abandon { branch: 5 } },
        Command::Branch { op: BranchOp::Reap { branch: 5, generation: 3 } },
        Command::ArenaGrant { node: NodeId(2), first_page: 66, page_count: 1024 },
        Command::TxnIdRange { node: NodeId(3), lo: 1000, hi: 2000 },
        Command::LeaseTick { unix_millis: 1_756_000_000_000 },
        Command::Checkpoint,
        Command::Membership { config: cfg() },
        Command::Membership { config: Config::empty() },
        Command::NoOp,
    ]
}

/// Every `Body` variant, and for the ones with a list, both an empty list and a populated one.
fn every_body() -> Vec<Body> {
    let entries: Vec<Entry> = every_command()
        .into_iter()
        .enumerate()
        .map(|(i, command)| Entry { term: 3, round: i as u64 + 1, command })
        .collect();

    vec![
        Body::PreVote { last_term: 0, last_round: 0 },
        Body::PreVote { last_term: u64::MAX, last_round: u64::MAX },
        Body::PreVoteResp { granted: true },
        Body::PreVoteResp { granted: false },
        Body::RequestVote { last_term: 9, last_round: 100 },
        Body::RequestVoteResp { granted: true },
        Body::RequestVoteResp { granted: false },
        // An empty entry list is the heartbeat, and it must be a legitimate frame rather than an
        // edge case that happens to decode.
        Body::Append { prev_round: 0, prev_term: 0, entries: Vec::new(), commit: 0 },
        Body::Append { prev_round: 41, prev_term: 2, entries, commit: 40 },
        Body::AppendResp { success: true, matched: 40, hint: 0, digest: 0xDEAD_BEEF_CAFE_F00D },
        Body::AppendResp { success: false, matched: 0, hint: 12, digest: 0 },
        Body::InstallSnapshot {
            meta: SnapshotMeta::default(),
            offset: 0,
            data: Vec::new(),
            done: false,
        },
        Body::InstallSnapshot {
            meta: SnapshotMeta {
                last_round: 900,
                last_term: 7,
                config: cfg(),
                total_bytes: 12_345_678,
            },
            offset: 4096,
            data: vec![1, 2, 3, 4, 5],
            done: true,
        },
        Body::InstallSnapshotResp { received_through: 0 },
        Body::InstallSnapshotResp { received_through: 4096 },
    ]
}

fn every_message() -> Vec<Message> {
    every_body()
        .into_iter()
        .map(|body| Message { from: NodeId(1), to: NodeId(2), term: 11, body })
        .collect()
}

/// Decode a whole frame the way a socket reader does: strip the tag and the length, then decode.
fn decode_frame(frame: &[u8]) -> Result<Message, FerroError> {
    assert_eq!(frame[0], CONSENSUS_TAG, "not a consensus frame");
    let len = u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize;
    assert_eq!(frame.len(), 5 + len, "the length must cover the body only");
    decode(&frame[5..])
}

// ---------------------------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------------------------

#[test]
fn the_length_covers_the_body_only_and_nothing_else() {
    // Stated because pgwire in this same codebase uses the other convention — its length includes
    // itself and excludes the tag — and confusing the two is how a hand-rolled protocol appears to
    // work and then desyncs three frames in. `replication` pins the same rule for its own frames.
    let m = Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 11,
        body: Body::PreVoteResp { granted: true },
    };
    let f = encode(&m).unwrap();
    assert_eq!(f[0], CONSENSUS_TAG);
    let len = u32::from_be_bytes(f[1..5].try_into().unwrap()) as usize;
    assert_eq!(f.len(), 1 + 4 + len, "tag + length + body");
    // from(4) + to(4) + term(8) + kind(1) + granted(1)
    assert_eq!(len, 18, "the body is exactly its fields, with no padding and no header");
}

#[test]
fn the_consensus_tag_is_disjoint_from_every_replication_tag() {
    // Both protocols share one framing and, since F3, one handshake version. A consensus client
    // that dials a replication listener therefore gets *through* the handshake, and this
    // disjointness is what catches it one frame later instead of letting a body be misread.
    //
    // The replication tags are read off real encoded frames rather than off a constant, so this
    // tests the wire and not a second declaration of it.
    for m in [
        crate::replication::Message::Hello { from_lsn: 1 },
        crate::replication::Message::Records { start_lsn: 1, bytes: vec![1] },
        crate::replication::Message::UpToDate { durable_lsn: 1 },
        crate::replication::Message::Error { message: "x".into() },
    ] {
        let tag = m.encode()[0];
        assert_ne!(
            tag, CONSENSUS_TAG,
            "replication frame {m:?} uses tag {:?}, which consensus has also claimed; one of the \
             two protocols would silently decode the other's frames",
            tag as char
        );
    }

    // And the converse: a replication reader must refuse a consensus frame rather than misparse it.
    let f = encode(&every_message()[0]).unwrap();
    let e = crate::replication::Message::read_from(&mut std::io::Cursor::new(f)).unwrap_err();
    assert!(
        format!("{e}").contains("unknown replication frame"),
        "a replication reader accepted a consensus frame: {e}"
    );
}

#[test]
fn a_v1_peer_is_refused_at_the_handshake_rather_than_misparsed_later() {
    // THE §F3 RULE. Version 1 has no arm for the consensus tag, so without the bump a v1 peer
    // completes the handshake and fails several frames later with "unknown replication frame" —
    // an error naming the symptom at an arbitrary point in the stream instead of the
    // incompatibility at the point it could have been refused.
    assert_eq!(REPL_VERSION, 2, "consensus traffic requires the protocol version to be 2");

    let mut v1 = Vec::new();
    v1.extend_from_slice(&REPL_MAGIC.to_be_bytes());
    v1.extend_from_slice(&1u16.to_be_bytes());
    let e = crate::replication::read_handshake(&mut std::io::Cursor::new(v1)).unwrap_err();
    assert!(
        format!("{e}").contains("version 1"),
        "a v1 handshake was not refused by name: {e}"
    );

    // The refusal names the version rather than being a bare disconnect, so an operator running
    // two builds is told which two.
    assert!(format!("{e}").contains("speaks 2"), "the refusal does not say what this build speaks: {e}");
}

#[test]
fn a_truncated_frame_is_refused_rather_than_misread() {
    for m in every_message() {
        let full = encode(&m).unwrap();
        // Every proper prefix of the body must be refused. (The framing layer refuses short
        // headers separately, in the FrameReader tests.)
        let body = &full[5..];
        for cut in 0..body.len() {
            assert!(
                decode(&body[..cut]).is_err(),
                "a {cut}-of-{} byte body decoded instead of being refused, for {:?}",
                body.len(),
                m.body
            );
        }
    }
}

#[test]
fn trailing_bytes_after_the_last_field_are_refused() {
    // A decoder that stops at its last field and ignores the slack accepts an unbounded family of
    // byte strings for one message — the canonicality property gone — and is also how a field the
    // sender writes and the receiver forgets to read stays invisible.
    for m in every_message() {
        let full = encode(&m).unwrap();
        let mut body = full[5..].to_vec();
        body.push(0);
        let e = decode(&body).unwrap_err();
        assert!(
            format!("{e}").contains("left over"),
            "a body with one trailing byte was accepted for {:?}: {e}",
            m.body
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Round-tripping every variant
// ---------------------------------------------------------------------------------------------

#[test]
fn every_body_variant_round_trips_including_an_empty_entry_list() {
    let msgs = every_message();
    assert!(msgs.len() >= 15, "the variant sweep has stopped being a sweep");
    let mut seen_empty_entries = false;
    let mut seen_full_entries = false;
    for m in msgs {
        if let Body::Append { entries, .. } = &m.body {
            if entries.is_empty() {
                seen_empty_entries = true;
            } else {
                seen_full_entries = true;
            }
        }
        let frame = encode(&m).unwrap();
        let back = decode_frame(&frame).unwrap();
        assert_eq!(back, m, "round trip changed the message");
    }
    assert!(seen_empty_entries, "no empty entry list was exercised");
    assert!(seen_full_entries, "no populated entry list was exercised");
}

#[test]
fn every_command_variant_round_trips_inside_an_entry() {
    // Named separately from the body sweep so a `Command` variant added without a wire encoding
    // fails here, where the message says "command", rather than inside an Append.
    let cmds = every_command();
    assert!(cmds.len() >= 17, "the command sweep has stopped being a sweep");
    for c in cmds {
        let m = Message {
            from: NodeId(4),
            to: NodeId(5),
            term: 2,
            body: Body::Append {
                prev_round: 1,
                prev_term: 1,
                entries: vec![Entry { term: 2, round: 2, command: c.clone() }],
                commit: 1,
            },
        };
        assert_eq!(decode_frame(&encode(&m).unwrap()).unwrap(), m, "{c:?} did not round trip");
    }
}

#[test]
fn the_encoding_is_canonical_so_a_signature_over_a_re_encoding_still_matches() {
    // F7 takes a MAC over bytes. A receiver that re-encodes a message it accepted must reproduce
    // the bytes it authenticated, which is only true if exactly one byte string spells each
    // message. Every "refused rather than normalised" rule in this codec exists for this line.
    for m in every_message() {
        let once = encode(&m).unwrap();
        let back = decode(&once[5..]).unwrap();
        let twice = encode(&back).unwrap();
        assert_eq!(once, twice, "re-encoding {:?} produced different bytes", m.body);
    }
}

#[test]
fn a_maximal_frame_round_trips_and_one_byte_more_is_refused_at_the_encoder() {
    // A body of exactly MAX_FRAME_BYTES is legal and must survive both the encoder and the socket
    // reader; one byte more must be refused to its SENDER, because `(len as u32)` on an oversized
    // body is the silent truncation that hands a peer a frame it will misparse.
    //
    // Body layout for this shape:
    //   from 4 + to 4 + term 8 + kind 1 + prev_round 8 + prev_term 8 + commit 8 + count 4
    //   + entry(term 8 + round 8) + cmd tag 1 + start_lsn 8 + payload-len 4  =  74
    const OVERHEAD: usize = 74;
    let build = |payload: usize| Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::Append {
            prev_round: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 1,
                round: 1,
                command: Command::WalBatch { start_lsn: 0, bytes: vec![0x5A; payload] },
            }],
            commit: 0,
        },
    };

    let maximal = build(MAX_FRAME_BYTES - OVERHEAD);
    let frame = encode(&maximal).unwrap();
    assert_eq!(
        frame.len(),
        MAX_FRAME_BYTES + 5,
        "the overhead constant in this test no longer matches the encoder"
    );
    let len = u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize;
    assert_eq!(len, MAX_FRAME_BYTES, "a maximal body must be exactly at the limit");
    assert_eq!(decode_frame(&frame).unwrap(), maximal, "a maximal frame did not round trip");

    // ...and the socket reader accepts exactly the same maximum, so the two limits cannot drift.
    let mut fr = FrameReader::new();
    let mut cur = std::io::Cursor::new(frame);
    let got = loop {
        match fr.poll(&mut cur).unwrap() {
            Poll::Frame(t, b) => break (t, b),
            Poll::Pending => continue,
            Poll::Eof => panic!("the reader hit EOF inside a maximal frame"),
        }
    };
    assert_eq!(got.0, CONSENSUS_TAG);
    assert_eq!(decode(&got.1).unwrap(), maximal);

    let e = encode(&build(MAX_FRAME_BYTES - OVERHEAD + 1)).unwrap_err();
    assert!(
        format!("{e}").contains("over the") && format!("{e}").contains("frame limit"),
        "an oversized message was not refused by name: {e}"
    );
}

#[test]
fn a_huge_entry_list_is_refused_before_it_is_built_and_not_after() {
    // Two guards refuse an oversized message: the per-entry one inside the encoder, and the single
    // check on the finished buffer. Only the first bounds MEMORY, and "it was refused" cannot tell
    // them apart — so this asserts on the SIZE the refusal names, which can.
    //
    // 2,000,000 NoOp entries are 17 bytes each, so the finished body would be ~34 MB. The
    // incremental guard stops at the first entry that would cross 8 MiB and therefore reports a
    // number a few bytes past the limit; the final check would report all 34 MB. A reported size
    // within a whisker of the limit is only reachable if the buffer never grew past it.
    let m = Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::Append {
            prev_round: 0,
            prev_term: 0,
            entries: (0..2_000_000)
                .map(|r| Entry { term: 1, round: r, command: Command::NoOp })
                .collect(),
            commit: 0,
        },
    };
    let e = format!("{}", encode(&m).unwrap_err());
    assert!(e.contains("frame limit"), "got {e}");

    let reported: usize = e
        .split_whitespace()
        .find_map(|w| w.parse::<usize>().ok())
        .unwrap_or_else(|| panic!("the refusal names no size: {e}"));
    assert!(
        reported <= MAX_FRAME_BYTES + 64,
        "the refusal names {reported} bytes, so the whole {}-byte body was built before anything \
         said no; the per-entry guard did not fire and the frame limit is not bounding memory",
        2_000_000 * 17
    );
}

// ---------------------------------------------------------------------------------------------
// Hostile input
// ---------------------------------------------------------------------------------------------

#[test]
fn an_oversized_length_is_refused_without_allocating() {
    // A peer must not get to choose this process's memory usage. Checked before `vec![0u8; len]`,
    // in the same order `replication::Message::read_from` checks it.
    // Both the absurd case and the boundary case. The boundary is what makes this a test of THE
    // limit rather than of some limit: one byte over must be refused, and a reader that merely
    // guards against a wild u32 would pass the first and fail the second.
    for claim in [u32::MAX as usize, MAX_FRAME_BYTES + 1] {
        let mut bytes = vec![CONSENSUS_TAG];
        bytes.extend_from_slice(&(claim as u32).to_be_bytes());
        let mut fr = FrameReader::new();
        let mut cur = std::io::Cursor::new(bytes);
        let e = loop {
            match fr.poll(&mut cur) {
                Ok(Poll::Pending) => continue,
                Ok(other) => panic!("a length of {claim} produced {other:?} instead of a refusal"),
                Err(e) => break e,
            }
        };
        assert!(
            format!("{e}").contains("over the"),
            "a length of {claim} was not refused by the frame limit: {e}"
        );
    }
}

#[test]
fn a_flag_byte_that_is_not_zero_or_one_is_refused() {
    // `!= 0` would make 2 and 1 decode to the same message, so a MAC over a re-encoding would not
    // match the bytes that arrived.
    let m = Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::PreVoteResp { granted: true },
    };
    let mut body = encode(&m).unwrap()[5..].to_vec();
    let flag = body.len() - 1;
    for bad in [2u8, 3, 0xFF] {
        body[flag] = bad;
        let e = decode(&body).unwrap_err();
        assert!(
            format!("{e}").contains("flag byte"),
            "flag byte {bad} was accepted: {e}"
        );
    }
}

#[test]
fn an_unknown_kind_command_or_branch_tag_is_refused_by_name() {
    // Refused rather than skipped: a frame this build cannot read is a frame it must not claim to
    // have understood, and a command it cannot apply is a round it must not claim to hold.
    let kind = {
        let mut b = encode(&every_message()[0]).unwrap()[5..].to_vec();
        b[16] = 200; // the kind byte, immediately after from/to/term
        b
    };
    let e = decode(&kind).unwrap_err();
    assert!(format!("{e}").contains("unknown consensus message kind 200"), "got {e}");

    let m = Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::Append {
            prev_round: 0,
            prev_term: 0,
            entries: vec![Entry { term: 1, round: 1, command: Command::NoOp }],
            commit: 0,
        },
    };
    let mut b = encode(&m).unwrap()[5..].to_vec();
    let cmd_tag = b.len() - 1;
    b[cmd_tag] = 201;
    let e = decode(&b).unwrap_err();
    assert!(format!("{e}").contains("unknown consensus command tag 201"), "got {e}");

    let m = Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::Append {
            prev_round: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 1,
                round: 1,
                command: Command::Branch { op: BranchOp::Abandon { branch: 1 } },
            }],
            commit: 0,
        },
    };
    let mut b = encode(&m).unwrap()[5..].to_vec();
    let branch_tag = b.len() - 9;
    b[branch_tag] = 202;
    let e = decode(&b).unwrap_err();
    assert!(format!("{e}").contains("unknown branch op tag 202"), "got {e}");
}

#[test]
fn a_configuration_that_repeats_a_member_is_refused_and_cannot_inflate_the_quorum() {
    // One node counted twice is one vote counted twice, which is two leaders of one term arriving
    // through the front door. Refused rather than de-duplicated, because a decoder that silently
    // normalises produces bytes the sender did not sign.
    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_be_bytes()); // from
    b.extend_from_slice(&2u32.to_be_bytes()); // to
    b.extend_from_slice(&1u64.to_be_bytes()); // term
    b.push(4); // Append
    b.extend_from_slice(&0u64.to_be_bytes()); // prev_round
    b.extend_from_slice(&0u64.to_be_bytes()); // prev_term
    b.extend_from_slice(&0u64.to_be_bytes()); // commit
    b.extend_from_slice(&1u32.to_be_bytes()); // one entry
    b.extend_from_slice(&1u64.to_be_bytes()); // entry term
    b.extend_from_slice(&1u64.to_be_bytes()); // entry round
    b.push(7); // Membership
    let cfg_at = b.len();
    b.extend_from_slice(&1u64.to_be_bytes()); // config version
    b.extend_from_slice(&1u64.to_be_bytes()); // config term
    b.extend_from_slice(&3u32.to_be_bytes()); // three "members"...
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes()); // ...the same one twice
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes()); // no learners

    let e = decode(&b).unwrap_err();
    assert!(
        format!("{e}").contains("strictly ascending"),
        "a repeated member was accepted: {e}"
    );

    // The same bytes with the duplicate removed decode, so the refusal is about the duplicate and
    // not about the frame being malformed in some other way. This is the anti-vacuity half.
    let mut good = b[..cfg_at].to_vec();
    good.extend_from_slice(&1u64.to_be_bytes());
    good.extend_from_slice(&1u64.to_be_bytes());
    good.extend_from_slice(&2u32.to_be_bytes());
    good.extend_from_slice(&1u32.to_be_bytes());
    good.extend_from_slice(&2u32.to_be_bytes());
    good.extend_from_slice(&0u32.to_be_bytes());
    let m = decode(&good).expect("the de-duplicated version must decode");
    let Body::Append { entries, .. } = &m.body else { panic!("shape changed") };
    let Command::Membership { config } = &entries[0].command else { panic!("shape changed") };
    assert_eq!(config.len(), 2);
    assert_eq!(config.quorum(), 2, "a two-node cluster needs two votes");
}

#[test]
fn an_unsorted_member_list_is_refused() {
    // Sorted and de-duplicated are one check, because `Config::new` guarantees both and an
    // out-of-order list is therefore equally impossible from a correct sender. Accepting it would
    // also break canonicality: two orderings of one set would be two spellings of one message.
    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(4);
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(7);
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&3u32.to_be_bytes()); // 3 before 1
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes());
    let e = decode(&b).unwrap_err();
    assert!(format!("{e}").contains("strictly ascending"), "got {e}");
}

#[test]
fn a_node_listed_as_both_voter_and_learner_is_refused() {
    // `with_learners` drops such a learner, which is right for a builder and wrong for a decoder:
    // this node would then hold a different configuration from the one the leader replicated,
    // while both claim the same version.
    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(4);
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(7);
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes()); // one learner...
    b.extend_from_slice(&2u32.to_be_bytes()); // ...who is already a voter
    let e = decode(&b).unwrap_err();
    assert!(
        format!("{e}").contains("both a voter and a learner"),
        "got {e}"
    );
}

#[test]
fn a_catalog_command_carrying_a_non_ddl_record_is_refused_before_it_can_panic() {
    // `RecKind::deserialize`'s heap arms (tags 5, 6, 7) index their slices unchecked, so a short
    // record with one of those tags PANICS. On a disk that is a corrupt page; on a socket it is a
    // one-frame remote denial of service, and the tag check in `decode_catalog` is what stops a
    // peer choosing whether this process lives.
    for hostile_tag in [5u8, 6, 7, 0, 10, 250] {
        let mut b = Vec::new();
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&2u32.to_be_bytes());
        b.extend_from_slice(&1u64.to_be_bytes());
        b.push(4);
        b.extend_from_slice(&0u64.to_be_bytes());
        b.extend_from_slice(&0u64.to_be_bytes());
        b.extend_from_slice(&0u64.to_be_bytes());
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&1u64.to_be_bytes());
        b.extend_from_slice(&1u64.to_be_bytes());
        b.push(1); // Catalog
        // A record of two bytes: the hostile tag and one byte of nothing. Every heap arm wants at
        // least 15.
        b.extend_from_slice(&2u32.to_be_bytes());
        b.push(hostile_tag);
        b.push(0);
        let e = decode(&b).unwrap_err();
        assert!(
            format!("{e}").contains("not a Ddl record") || format!("{e}").contains("log record"),
            "record tag {hostile_tag} produced {e}"
        );
    }
}

#[test]
fn a_catalog_command_carrying_a_page_id_is_refused() {
    // `Command::Catalog` is deliberately logical: each node applies the DDL and computes its own
    // roots. A page id is node-local, so shipping the leader's would point this node's catalog at
    // one of ITS pages holding something else — the same mistake as agreeing on byte offsets.
    // The encoder writes zeroes; this is the check that turns that into a rule.
    let rec = crate::wal::log::RecKind::Ddl {
        op: DdlOp::CreateTable,
        table: "t".into(),
        dir_root: 42, // a real page id, exactly what must never cross
        time_travel_root: 0,
        columns: vec![("id".into(), DataType::Integer, false)],
    };
    let mut rec_bytes = Vec::new();
    rec.serialize(&mut rec_bytes);

    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(4);
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(1);
    b.extend_from_slice(&(rec_bytes.len() as u32).to_be_bytes());
    b.extend_from_slice(&rec_bytes);

    let e = decode(&b).unwrap_err();
    assert!(format!("{e}").contains("dir_root=42"), "a page id crossed the wire: {e}");

    // Anti-vacuity: the identical record with the roots zeroed decodes, so the refusal is about
    // the page id and not about the record being unreadable.
    let rec = crate::wal::log::RecKind::Ddl {
        op: DdlOp::CreateTable,
        table: "t".into(),
        dir_root: 0,
        time_travel_root: 0,
        columns: vec![("id".into(), DataType::Integer, false)],
    };
    let mut ok_bytes = Vec::new();
    rec.serialize(&mut ok_bytes);
    let mut good = b[..b.len() - rec_bytes.len() - 4].to_vec();
    good.extend_from_slice(&(ok_bytes.len() as u32).to_be_bytes());
    good.extend_from_slice(&ok_bytes);
    decode(&good).expect("the same record with no page ids must decode");
}

// ---------------------------------------------------------------------------------------------
// The resumable frame reader
// ---------------------------------------------------------------------------------------------

/// A reader that hands back one byte at a time and returns `WouldBlock` between every one —
/// exactly what a `TcpStream` with a read timeout does to a frame that arrives slowly.
struct Stutter {
    data: Vec<u8>,
    at: usize,
    stall: bool,
}

impl Read for Stutter {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stall = !self.stall;
        if self.stall {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "read timed out"));
        }
        if self.at >= self.data.len() {
            return Ok(0);
        }
        if buf.is_empty() {
            return Ok(0);
        }
        buf[0] = self.data[self.at];
        self.at += 1;
        Ok(1)
    }
}

#[test]
fn a_frame_split_across_read_timeouts_is_reassembled_and_not_desynchronised() {
    // THE RULE `read_exact` CANNOT MEET. `read_exact` does not report how much it consumed before
    // it failed, so a frame straddling a socket read timeout leaves bytes taken from the kernel and
    // unaccounted for — and every frame after it is parsed from the wrong offset, which a
    // length-prefixed protocol cannot detect because the next "tag" is whatever the middle of the
    // last frame held.
    //
    // Two frames back to back, delivered a byte at a time with a timeout between every byte. Both
    // must come out whole and in order.
    let a = every_message()[0].clone();
    let b = Message {
        from: NodeId(3),
        to: NodeId(1),
        term: 99,
        body: Body::AppendResp { success: true, matched: 7, hint: 0, digest: 5 },
    };
    let mut stream = encode(&a).unwrap();
    stream.extend_from_slice(&encode(&b).unwrap());

    let mut r = Stutter { data: stream, at: 0, stall: false };
    let mut fr = FrameReader::new();
    let mut got = Vec::new();
    for _ in 0..1_000_000 {
        match fr.poll(&mut r).unwrap() {
            Poll::Pending => continue,
            Poll::Eof => break,
            Poll::Frame(tag, body) => {
                assert_eq!(tag, CONSENSUS_TAG);
                got.push(decode(&body).unwrap());
            }
        }
    }
    assert_eq!(got, vec![a, b], "a frame delivered a byte at a time was not reassembled in order");
}

#[test]
fn a_peer_that_closes_inside_a_frame_is_refused_rather_than_completed() {
    // Closing between frames is the ordinary end of a connection; closing inside one is not, and
    // the difference must be visible or a half-frame gets completed from whatever arrives next.
    let full = encode(&every_message()[0]).unwrap();

    let mut fr = FrameReader::new();
    let mut empty = std::io::Cursor::new(Vec::new());
    assert!(matches!(fr.poll(&mut empty).unwrap(), Poll::Eof), "a clean close is not EOF");

    for cut in [1usize, 3, 5, 6, full.len() - 1] {
        let mut fr = FrameReader::new();
        let mut cur = std::io::Cursor::new(full[..cut].to_vec());
        let mut err = None;
        for _ in 0..1000 {
            match fr.poll(&mut cur) {
                Ok(Poll::Pending) => continue,
                Ok(Poll::Eof) => panic!("a {cut}-byte prefix reported a clean EOF"),
                Ok(Poll::Frame(..)) => panic!("a {cut}-byte prefix produced a whole frame"),
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert!(err.is_some(), "a {cut}-byte prefix neither errored nor terminated");
    }
}

// ---------------------------------------------------------------------------------------------
// The transport itself, over real sockets
// ---------------------------------------------------------------------------------------------

fn fast() -> TransportOptions {
    TransportOptions {
        queue_depth: 64,
        poll_interval: Duration::from_millis(5),
        reconnect_delay: Duration::from_millis(5),
        handshake_deadline: Duration::from_secs(5),
    }
}

/// Two listeners bound first, so both peer maps are complete before either transport starts.
/// Binding one transport and then the other would need an address that does not exist yet.
fn pair(opts: TransportOptions) -> (Transport, Transport) {
    let la = TcpListener::bind("127.0.0.1:0").unwrap();
    let lb = TcpListener::bind("127.0.0.1:0").unwrap();
    let aa = la.local_addr().unwrap();
    let ab = lb.local_addr().unwrap();
    let a = Transport::from_listener(
        NodeId(1),
        la,
        BTreeMap::from([(NodeId(2), ab)]),
        opts.clone(),
    )
    .unwrap();
    let b = Transport::from_listener(NodeId(2), lb, BTreeMap::from([(NodeId(1), aa)]), opts).unwrap();
    (a, b)
}

/// Wait for one message, failing loudly rather than returning `None` — a test that accepts "nothing
/// arrived" has not tested delivery.
fn expect_recv(t: &Transport, within: Duration) -> Message {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(m) = t.recv_timeout(Duration::from_millis(20)) {
            return m;
        }
    }
    panic!("no message arrived within {within:?}; received={} misrouted={}", t.received(), t.misrouted());
}

#[test]
fn every_body_variant_survives_a_real_socket_in_both_directions() {
    let (a, b) = pair(fast());
    for m in every_body() {
        let out = Message { from: NodeId(1), to: NodeId(2), term: 5, body: m.clone() };
        a.send(&out).unwrap();
        assert_eq!(expect_recv(&b, Duration::from_secs(10)), out, "A->B lost or changed {m:?}");

        let back = Message { from: NodeId(2), to: NodeId(1), term: 5, body: m.clone() };
        b.send(&back).unwrap();
        assert_eq!(expect_recv(&a, Duration::from_secs(10)), back, "B->A lost or changed {m:?}");
    }
}

#[test]
fn a_message_addressed_to_another_node_is_refused_rather_than_delivered() {
    // Not authentication — `from` is still only a claim and F7 owns that. This catches the
    // configuration mistake where two nodes were given one address, which otherwise shows up as
    // one node mysteriously voting twice.
    let (a, b) = pair(fast());
    let mis = Message {
        from: NodeId(1),
        to: NodeId(7), // not b
        term: 5,
        body: Body::PreVoteResp { granted: true },
    };
    // Injected past `send`, which would refuse an unconfigured addressee, by writing the frame to
    // b's listener directly — that is the shape a misconfiguration actually takes.
    let frame = encode(&mis).unwrap();
    let mut s = TcpStream::connect(b.local_addr()).unwrap();
    let mut hs = Vec::new();
    crate::replication::write_handshake(&mut hs).unwrap();
    s.write_all(&hs).unwrap();
    let mut theirs = [0u8; 6];
    s.read_exact(&mut theirs).unwrap();
    s.write_all(&frame).unwrap();
    s.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && b.misrouted() == 0 {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(b.misrouted(), 1, "a misrouted message was not counted");
    assert_eq!(b.received(), 0, "a misrouted message was delivered to the state machine");
    assert!(b.try_recv().is_none(), "a misrouted message reached the inbox");
    drop(a);
}

#[test]
fn a_v1_peer_is_told_which_version_this_node_speaks() {
    // The handshake bytes go out whatever the verdict, because that is what makes a v1 peer's own
    // `read_handshake` say "version 2; this build speaks 1" instead of "failed to fill whole
    // buffer". The incompatibility is named at the handshake, by the peer, in its own words.
    let (a, _b) = pair(fast());
    let mut s = TcpStream::connect(a.local_addr()).unwrap();
    let mut v1 = Vec::new();
    v1.extend_from_slice(&REPL_MAGIC.to_be_bytes());
    v1.extend_from_slice(&1u16.to_be_bytes());
    s.write_all(&v1).unwrap();
    s.flush().unwrap();

    let mut theirs = [0u8; 6];
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.read_exact(&mut theirs).unwrap();
    assert_eq!(u32::from_be_bytes(theirs[0..4].try_into().unwrap()), REPL_MAGIC);
    assert_eq!(
        u16::from_be_bytes(theirs[4..6].try_into().unwrap()),
        2,
        "a refused peer was not told the version this node speaks"
    );

    // And the refusal is counted and the connection closed, so a v1 peer cannot go on to send
    // frames that would be misparsed.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && a.refused_handshakes() == 0 {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(a.refused_handshakes(), 1, "the refusal was not counted");

    // Everything after the handshake is the courtesy Error frame and then EOF; no consensus frame
    // is ever accepted on this connection.
    let mut rest = Vec::new();
    let _ = s.read_to_end(&mut rest);
    assert!(
        rest.is_empty() || rest[0] == b'E',
        "a refused connection sent something other than an error frame: {:?}",
        &rest[..rest.len().min(8)]
    );
    assert_eq!(a.received(), 0);
}

#[test]
fn a_peer_that_cannot_be_reached_drops_the_oldest_and_counts_every_drop() {
    // A leader that BLOCKED writing to one partitioned follower would stop heartbeating the healthy
    // majority, turning one node's failure into the cluster's. So this transport drops — and counts
    // every drop, because a message that vanished with no number attached is indistinguishable
    // from a protocol bug.
    let mut opts = fast();
    opts.queue_depth = 4;
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    // Port 1 refuses every connection and needs no privilege to dial, so the peer is reliably
    // unreachable rather than probabilistically so.
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let t =
        Transport::from_listener(NodeId(1), l, BTreeMap::from([(NodeId(2), dead)]), opts).unwrap();

    for i in 0..20u64 {
        t.send(&Message {
            from: NodeId(1),
            to: NodeId(2),
            term: i,
            body: Body::PreVoteResp { granted: true },
        })
        .unwrap();
    }
    assert_eq!(t.sent(), 20);
    assert!(
        t.dropped() >= 16,
        "a depth-4 queue given 20 messages dropped only {}",
        t.dropped()
    );
    assert_eq!(t.dropped_to(NodeId(2)), t.dropped(), "the per-peer meter disagrees with the total");
}

#[test]
fn sending_to_an_unconfigured_peer_or_to_self_is_refused_rather_than_dropped() {
    // Both are configuration mistakes rather than network conditions, and both are silent
    // partitions if they are dropped: the node is permanently unreachable while every meter reads
    // healthy.
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let t = Transport::from_listener(NodeId(1), l, BTreeMap::new(), fast()).unwrap();

    let e = t
        .send(&Message {
            from: NodeId(1),
            to: NodeId(2),
            term: 1,
            body: Body::PreVoteResp { granted: true },
        })
        .unwrap_err();
    assert!(format!("{e}").contains("no address is configured for n2"), "got {e}");

    let e = t
        .send(&Message {
            from: NodeId(1),
            to: NodeId(1),
            term: 1,
            body: Body::PreVoteResp { granted: true },
        })
        .unwrap_err();
    assert!(format!("{e}").contains("to itself"), "got {e}");
    assert_eq!(t.sent(), 0, "a refused send was counted as sent");
}

#[test]
fn a_peer_map_naming_this_node_is_refused_at_bind() {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let e = Transport::from_listener(NodeId(1), l, BTreeMap::from([(NodeId(1), addr)]), fast())
        .unwrap_err();
    assert!(format!("{e}").contains("contains n1 itself"), "got {e}");
}

#[test]
fn a_queue_depth_of_zero_is_refused_rather_than_silently_partitioning_the_node() {
    let mut opts = fast();
    opts.queue_depth = 0;
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let e = Transport::from_listener(NodeId(1), l, BTreeMap::from([(NodeId(2), addr)]), opts)
        .unwrap_err();
    assert!(format!("{e}").contains("queue depth of 0"), "got {e}");
}

#[test]
fn shutdown_joins_every_thread_and_closes_the_listener() {
    // A transport that has been shut down must have no thread still holding a socket. If the
    // listener were merely leaked, the next test to bind would race the previous one.
    // A deliberately slow poll: if `shutdown` returned without joining, the accept thread would
    // still hold the listener for up to a whole interval afterwards, and the connect below would
    // succeed. At the 5ms default that window is a race; at 300ms it is a fact.
    let mut opts = fast();
    opts.poll_interval = Duration::from_millis(300);
    let (a, b) = pair(opts);
    let addr = a.local_addr();
    a.send(&Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::PreVoteResp { granted: true },
    })
    .unwrap();
    let _ = expect_recv(&b, Duration::from_secs(10));

    a.shutdown();
    a.shutdown(); // idempotent: a second call is a barrier, not an error

    let refused = TcpStream::connect_timeout(&addr, Duration::from_secs(1));
    assert!(
        refused.is_err(),
        "the listener was still accepting after shutdown, so a thread outlived the transport"
    );
    drop(b);
}

#[test]
fn a_frame_with_an_unknown_tag_closes_the_connection_rather_than_being_skipped() {
    // A stream carrying a tag this listener cannot route is a stream it cannot claim to be reading
    // correctly — a replication frame arriving on a consensus port is a misdialled peer, and
    // skipping it would leave the reader guessing at where the next frame starts.
    let (a, b) = pair(fast());
    let mut s = TcpStream::connect(b.local_addr()).unwrap();
    let mut hs = Vec::new();
    crate::replication::write_handshake(&mut hs).unwrap();
    s.write_all(&hs).unwrap();
    let mut theirs = [0u8; 6];
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.read_exact(&mut theirs).unwrap();

    // A perfectly valid *replication* frame, on a consensus port — then a perfectly valid
    // *consensus* frame behind it. The second one is the detector: if the reader had skipped the
    // unroutable frame and carried on, this would arrive and be delivered. It must not, because the
    // connection is closed at the first frame it cannot route.
    s.write_all(&crate::replication::Message::UpToDate { durable_lsn: 9 }.encode()).unwrap();
    s.write_all(
        &encode(&Message {
            from: NodeId(1),
            to: NodeId(2),
            term: 1,
            body: Body::PreVoteResp { granted: true },
        })
        .unwrap(),
    )
    .unwrap();
    s.flush().unwrap();

    let mut rest = Vec::new();
    let _ = s.read_to_end(&mut rest);
    assert!(rest.is_empty(), "the connection was not closed on an unroutable tag");

    // Give a reader that wrongly carried on every chance to deliver the frame behind it.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        b.received(),
        0,
        "a consensus frame behind an unroutable one was delivered, so the reader skipped rather \
         than closed and its idea of where frames begin is no longer trustworthy"
    );
    assert!(b.try_recv().is_none());
    drop(a);
}

#[test]
fn a_catalog_command_with_bytes_after_its_ddl_record_is_refused() {
    // A hole the codec INHERITS rather than creates, found by reading `wal::log`: the `Ddl` arm of
    // `RecKind::deserialize` returns without ever comparing its cursor to the buffer's length, so
    // any number of bytes may follow the record and be silently discarded. The WAL never notices
    // because `read_record` hands it an exactly-sized slice (`&frame[28..total-4]`); a socket does
    // not.
    //
    // The consequence is not a misread record, it is a broken canonicality property: two byte
    // strings would decode to one `Command::Catalog`, and F7's MAC is taken over bytes.
    let rec = crate::wal::log::RecKind::Ddl {
        op: DdlOp::CreateTable,
        table: "t".into(),
        dir_root: 0,
        time_travel_root: 0,
        columns: vec![("id".into(), DataType::Integer, false)],
    };
    let mut rec_bytes = Vec::new();
    rec.serialize(&mut rec_bytes);
    rec_bytes.extend_from_slice(b"smuggled");

    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(4);
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(1); // Catalog
    b.extend_from_slice(&(rec_bytes.len() as u32).to_be_bytes());
    b.extend_from_slice(&rec_bytes);

    let e = decode(&b).unwrap_err();
    assert!(
        format!("{e}").contains("did not re-encode"),
        "eight smuggled bytes rode inside a Catalog command: {e}"
    );
}
