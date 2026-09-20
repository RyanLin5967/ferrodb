//! F3 — the transport's rules, one named test each.
//!
//! Every test below names a rule from `DISTRIBUTED.md` §F3 or from the module header of
//! `transport.rs`, and every one of them has been seen to **fail** with that rule deliberately
//! broken — the mutants and what each printed are recorded in `F3-transport.md` (a session scratch file that was NEVER committed -- the run is not preserved in this tree). A test
//! that has never failed is not evidence.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
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
fn an_oversized_length_is_refused_before_a_single_body_byte_is_read() {
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
        // **The "before anything is read" half, which the old name claimed and never checked.**
        // The cursor has moved exactly five bytes — the tag and the length — so the limit was
        // compared before `vec![0u8; len]` was reached. Had the allocation come first, a claim of
        // `u32::MAX` would have asked for four gigabytes to fill from a two-byte stream.
        assert_eq!(
            cur.position(),
            5,
            "the reader consumed {} bytes for a frame it refused; the limit is being checked after \
             the body is touched rather than before",
            cur.position()
        );
    }
}

#[test]
fn a_frame_claiming_four_billion_entries_is_refused_rather_than_reserved_for() {
    // The decoder's stated rule: nothing is reserved from a peer-chosen `count`, because
    // `Vec::with_capacity` on one is exactly the amplification the frame limit exists to prevent —
    // eight megabytes on the wire asking for gigabytes of address space.
    //
    // This is the test that distinguishes the two implementations: growing as entries decode gives
    // an error on the first missing byte, while reserving from the count aborts the process. An
    // abort is a failing test, so either way the rule is measured rather than asserted.
    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_be_bytes()); // from
    b.extend_from_slice(&2u32.to_be_bytes()); // to
    b.extend_from_slice(&1u64.to_be_bytes()); // term
    b.push(4); // Append
    b.extend_from_slice(&0u64.to_be_bytes()); // prev_round
    b.extend_from_slice(&0u64.to_be_bytes()); // prev_term
    b.extend_from_slice(&0u64.to_be_bytes()); // commit
    b.extend_from_slice(&u32::MAX.to_be_bytes()); // ...and four billion entries, none of them present
    let e = decode(&b).unwrap_err();
    assert!(
        format!("{e}").contains("entry 0 of 4294967295"),
        "a four-billion-entry claim was not refused at its first missing entry: {e}"
    );

    // The same for a node list, which has its own count.
    let mut c = Vec::new();
    c.extend_from_slice(&1u32.to_be_bytes());
    c.extend_from_slice(&2u32.to_be_bytes());
    c.extend_from_slice(&1u64.to_be_bytes());
    c.push(4);
    c.extend_from_slice(&0u64.to_be_bytes());
    c.extend_from_slice(&0u64.to_be_bytes());
    c.extend_from_slice(&0u64.to_be_bytes());
    c.extend_from_slice(&1u32.to_be_bytes());
    c.extend_from_slice(&1u64.to_be_bytes());
    c.extend_from_slice(&1u64.to_be_bytes());
    c.push(7); // Membership
    c.extend_from_slice(&1u64.to_be_bytes());
    c.extend_from_slice(&1u64.to_be_bytes());
    c.extend_from_slice(&u32::MAX.to_be_bytes()); // four billion members
    assert!(decode(&c).is_err(), "a four-billion-member claim was accepted");
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
        inbox_bytes: 32 * 1024 * 1024,
        max_inbound_conns: 256,
        idle_deadline: Duration::from_secs(60),
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

    // **A frame under replication's tag whose body IS a valid consensus message.**
    //
    // The first version of this test sent a replication `UpToDate`, and the mutant that removes the
    // tag check SURVIVED it: an `UpToDate` body is eight bytes, too short to decode as a consensus
    // message, so `decode` refused it and the connection closed anyway — for the wrong reason. The
    // test was measuring the decoder, not the tag check.
    //
    // The case the tag check actually guards is this one: bytes that decode perfectly well. A
    // replication `Records` frame carries `start_lsn` and then arbitrary WAL bytes, so its body can
    // be anything at all — and without the tag check those bytes are handed to the state machine
    // off a stream this node has no business reading as consensus.
    let smuggled = encode(&Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::PreVoteResp { granted: true },
    })
    .unwrap();
    let body = &smuggled[5..];
    let mut disguised = vec![b'R'];
    disguised.extend_from_slice(&(body.len() as u32).to_be_bytes());
    disguised.extend_from_slice(body);
    s.write_all(&disguised).unwrap();

    // ...and a genuine consensus frame behind it, which must also not arrive: the connection is
    // closed at the first frame it cannot route, not merely at frames it cannot parse.
    s.write_all(&smuggled).unwrap();
    s.flush().unwrap();

    // **Observed, not defaulted.** `let _ = read_to_end(..)` then `assert!(rest.is_empty())`
    // passes whenever the read ERRORS, because `rest` is then still empty — it asserts on a default
    // rather than on a close. This distinguishes the two: a clean close is `Ok(0)`, an abortive one
    // is `ConnectionReset` or `ConnectionAborted`, and anything else is a failure.
    //
    // **Both abortive kinds, because this close is necessarily abortive and Windows names it
    // differently.** The listener closes while the frame written behind the unroutable one is still
    // unread in its receive buffer, and TCP requires a close with unread data to send RST rather
    // than FIN — so the peer never sees a clean `Ok(0)` here. Linux and macOS surface that RST as
    // `ECONNRESET`; Windows surfaces the same event as `WSAECONNABORTED` (10053) or `WSAECONNRESET`
    // (10054) depending on which side of the stack observes it first. Accepting only `ConnectionReset`
    // therefore made this test a coin flip on windows-latest: it passed in the pull_request run for
    // `872a7d9` and failed in the push run for that same commit.
    //
    // This widens which OS *spelling* of "the listener closed" is accepted, and not what the test
    // demands. Every failure mode it exists to catch is still rejected below: a listener that stayed
    // open times out (`WouldBlock`/`TimedOut`), a listener that skipped the frame and carried on
    // returns `Ok(n)` with bytes, and `b.received() == 0` is checked separately after the sleep
    // regardless of which arm was taken. Fire-checked on both counts — see the note on that
    // assertion.
    let mut rest = Vec::new();
    match s.read_to_end(&mut rest) {
        // A clean close, or an abortive one. All three mean the listener closed.
        Ok(0) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        Ok(n) => panic!("the connection stayed open and sent {n} byte(s): {:?}", &rest[..n.min(16)]),
        // **The mutant's signature, named explicitly.** A read timeout expiring means the socket is
        // STILL OPEN with nothing to read — the listener kept the connection after a frame it
        // cannot route. Reported as that, rather than as "some other error": a mutant run of this
        // test hit exactly this arm and the old message blamed the read instead of the listener.
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            panic!(
                "the connection was still open after a frame under a tag this listener does not \
                 route (the read timed out rather than seeing a close), so the reader carried on \
                 over a stream it cannot claim to be parsing correctly"
            )
        }
        Err(e) => panic!("reading after an unroutable frame failed for another reason: {e}"),
    }

    // Give a reader that wrongly carried on every chance to deliver the frame behind it.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        b.received(),
        0,
        "a message arrived under a tag this listener does not route. Either the disguised frame was \
         decoded as consensus, or the reader skipped it and carried on to the frame behind it; \
         both mean this node acts on bytes it cannot claim to be reading correctly"
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


// ---------------------------------------------------------------------------------------------
// Why the frame reader is resumable at all
// ---------------------------------------------------------------------------------------------

/// Delivers `before` bytes one at a time and then times out for ever — a frame that straddles a
/// socket read timeout.
struct Halting {
    data: Vec<u8>,
    at: usize,
    before: usize,
}

impl Read for Halting {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.at >= self.before {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "read timed out"));
        }
        if self.at >= self.data.len() || buf.is_empty() {
            return Ok(0);
        }
        buf[0] = self.data[self.at];
        self.at += 1;
        Ok(1)
    }
}

#[test]
fn read_exact_loses_what_it_consumed_which_is_why_the_frame_reader_is_resumable() {
    // ANTI-VACUITY for `FrameReader`. Every other test here shows the resumable reader working;
    // this one shows the obvious alternative failing, so the extra machinery is evidenced rather
    // than asserted.
    //
    // `TcpStream` does not override `read_exact`, so it gets `default_read_exact`, which loops on
    // `read` and returns on the first non-`Interrupted` error — discarding both the bytes it has
    // already moved into the caller's buffer and any record of how many there were. A socket read
    // timeout is exactly such an error, and the read timeout is not optional here: it is how a
    // connection thread notices a shutdown.
    let data: Vec<u8> = (0u8..10).collect();

    let mut r = Halting { data: data.clone(), at: 0, before: 4 };
    let mut buf = [0u8; 10];
    let err = r.read_exact(&mut buf).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(r.at, 4, "the reader should have taken four bytes off the stream before timing out");
    // And that is the whole problem: four bytes are gone from the stream, `read_exact` returned no
    // count, and its contract says `buf`'s contents are unspecified — so a caller has no way to
    // find out. Retrying re-reads from byte 4, and every frame after this one is parsed from the
    // wrong offset.

    // The same reader, the same four bytes, through `FrameReader`: still held.
    let mut r2 = Halting { data, at: 0, before: 4 };
    let mut fr = FrameReader::new();
    for _ in 0..8 {
        match fr.poll(&mut r2) {
            Ok(Poll::Pending) => continue,
            other => panic!("expected the reader to be waiting, got {other:?}"),
        }
    }
    assert_eq!(
        fr.header_got, 4,
        "the resumable reader lost the same bytes `read_exact` loses, so it is not resumable"
    );
    assert_eq!(r2.at, 4, "no extra bytes were taken from the stream");
}

// ---------------------------------------------------------------------------------------------
// Resource lifetime
// ---------------------------------------------------------------------------------------------

#[test]
fn an_inbound_connection_frees_its_descriptor_when_it_closes() {
    // `try_clone` DUPS the descriptor, so a registry that is pushed onto and never drained holds
    // one open per connection this node has ever accepted — for the life of the process. A
    // reconnect is this transport's ordinary recovery from a write failure, so a long-lived node
    // would reach EMFILE and start refusing peers for a reason nothing in the cluster explains.
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = Transport::from_listener(NodeId(1), l, BTreeMap::new(), fast()).unwrap();

    const ROUNDS: usize = 40;
    for i in 0..ROUNDS {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut hs = Vec::new();
        crate::replication::write_handshake(&mut hs).unwrap();
        s.write_all(&hs).unwrap();
        s.flush().unwrap();
        let mut theirs = [0u8; 6];
        s.read_exact(&mut theirs).unwrap_or_else(|e| panic!("handshake {i}: {e}"));
        drop(s);
    }

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && t.live_inbound_conns() > 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        t.live_inbound_conns(),
        0,
        "{ROUNDS} connections were opened and closed, and the registry still holds {}; each one is \
         a leaked file descriptor",
        t.live_inbound_conns()
    );
}

#[test]
fn a_zero_timeout_is_refused_at_bind_rather_than_failing_every_socket_call() {
    // std treats a zero duration as an error for both `set_read_timeout` and `connect_timeout`, so
    // a zero here does not mean "do not wait" — it means every socket call fails with `invalid
    // input` and the node reports itself unable to reach anyone, for a reason that names nothing
    // about the cluster.
    for (name, mutate) in [
        ("poll_interval", 0usize),
        ("reconnect_delay", 1),
        ("handshake_deadline", 2),
    ] {
        let mut opts = fast();
        match mutate {
            0 => opts.poll_interval = Duration::ZERO,
            1 => opts.reconnect_delay = Duration::ZERO,
            _ => opts.handshake_deadline = Duration::ZERO,
        }
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let e = Transport::from_listener(NodeId(1), l, BTreeMap::new(), opts).unwrap_err();
        assert!(
            format!("{e}").contains(name) && format!("{e}").contains("is zero"),
            "a zero {name} was accepted: {e}"
        );
    }
}

#[test]
fn a_stopped_transport_is_distinguishable_from_a_quiet_one() {
    // `recv_timeout` returning `None` means "nothing arrived in that window" while the transport is
    // live and "nothing ever will" after it is stopped. A driver looping on it has to tell those
    // apart, or a shut-down transport reads exactly like a healthy cluster with nothing to say.
    let (a, b) = pair(fast());
    assert!(!a.is_stopped());
    assert!(a.recv_timeout(Duration::from_millis(20)).is_none(), "a quiet transport had traffic");
    assert!(!a.is_stopped(), "a quiet window is not a stopped transport");
    a.shutdown();
    assert!(a.is_stopped());
    assert!(a.recv_timeout(Duration::from_millis(20)).is_none());
    drop(b);
}


// ---------------------------------------------------------------------------------------------
// Defects found by a fresh-context adversarial review of 4c85d22
// ---------------------------------------------------------------------------------------------

#[test]
fn a_configuration_naming_more_nodes_than_the_wire_allows_is_refused_before_any_id_is_read() {
    // **This was a remote denial of service.** The disjointness check was
    // `learners.iter().find(|n| members.contains(n))`, and `Vec::contains` is linear, so it was
    // O(learners × members); `Config::with_learners`'s `retain` is a second O(learners × members).
    // One 8 MiB frame carries 2,097,152 `u32` node ids, so two lists of ~1M each cost on the order
    // of 10^12 comparisons — one frame from any peer that completed the handshake, one CPU, hours.
    //
    // The frame limit bounds the BYTES a peer can make this process hold. It says nothing about the
    // WORK a peer can make this process do, and that was the hole.
    let mut b = Vec::new();
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(4); // Append
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&0u64.to_be_bytes());
    b.extend_from_slice(&1u32.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(7); // Membership
    b.extend_from_slice(&1u64.to_be_bytes()); // version
    b.extend_from_slice(&1u64.to_be_bytes()); // term
    // A claim of a million members, and then NOTHING. If the count is honoured before it is
    // sanity-checked, the decoder walks a million ids it does not have; if it is refused first,
    // this frame costs nothing at all. The body being far too short to hold the claim is the point.
    b.extend_from_slice(&1_000_000u32.to_be_bytes());

    let e = decode(&b).unwrap_err();
    assert!(
        format!("{e}").contains("over the") && format!("{e}").contains("limit"),
        "a million-member configuration was not refused by the node cap: {e}"
    );

    // Anti-vacuity: a configuration at the cap is legal and decodes, so the refusal is about the
    // size and not about the frame being malformed.
    let ok = Config::new((1..=MAX_CONFIG_NODES as u32).map(NodeId), 1, 1);
    assert_eq!(ok.members().len(), MAX_CONFIG_NODES);
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
                command: Command::Membership { config: ok },
            }],
            commit: 0,
        },
    };
    assert_eq!(decode_frame(&encode(&m).unwrap()).unwrap(), m, "a configuration at the cap must work");

    // ...and one node over the cap is refused to its SENDER, so this node never emits a frame every
    // peer would refuse for a reason it could not see.
    let too_big = Config::new((1..=MAX_CONFIG_NODES as u32 + 1).map(NodeId), 1, 1);
    let e = encode(&Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::Append {
            prev_round: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 1,
                round: 1,
                command: Command::Membership { config: too_big },
            }],
            commit: 0,
        },
    })
    .unwrap_err();
    assert!(format!("{e}").contains("the wire format"), "got {e}");
}

#[test]
fn the_oldest_queued_frame_is_the_one_dropped_and_the_newest_always_survives() {
    // The existing overflow test counts drops and never observes WHICH frames survived, so a
    // `pop_back()` would pass it — the review found that, and it is right: the policy the test is
    // named for was unmeasured.
    //
    // Oldest and not newest is load-bearing. Consensus messages are cumulative: a later `Append`
    // carries a higher `commit` and later entries, and a later heartbeat supersedes an earlier one.
    // A queue that dropped the newest would hold a stale view of the leader's state and never catch
    // up, so the peer would be told something that is permanently out of date.
    let ob = Outbox {
        addr: "127.0.0.1:1".parse().unwrap(),
        state: Mutex::new(OutboxState { queue: VecDeque::new(), live: None, stopped: false }),
        woken: Condvar::new(),
        depth: 4,
        dropped: std::sync::atomic::AtomicU64::new(0),
    };

    // Ten frames, each identifiable by its term.
    for term in 1..=10u64 {
        let m = Message { from: NodeId(1), to: NodeId(2), term, body: Body::PreVoteResp { granted: true } };
        ob.push(encode(&m).unwrap());
    }

    let st = ob.state.lock().unwrap();
    assert_eq!(st.queue.len(), 4, "the queue grew past its depth");
    let surviving: Vec<u64> = st
        .queue
        .iter()
        .map(|f| decode(&f[5..]).expect("a queued frame must still decode").term)
        .collect();
    assert_eq!(
        surviving,
        vec![7, 8, 9, 10],
        "the queue kept {surviving:?}; dropping the OLDEST must leave the four newest, in order. \
         Keeping 1..4 would mean this peer is permanently told a stale view of the leader"
    );
    assert_eq!(ob.dropped.load(std::sync::atomic::Ordering::SeqCst), 6);
}

#[test]
fn an_undrained_inbox_is_bounded_in_bytes_and_every_refusal_is_counted() {
    // The frame limit caps ONE frame; the channel to the caller was std's unbounded mpsc, so a peer
    // writing faster than the caller drains — the normal state whenever the state machine is
    // applying or fsyncing — chose this process's memory however small each frame was.
    //
    // Refused rather than blocked: blocking the connection thread would park it where `shutdown`
    // cannot reach it.
    let mut opts = fast();
    opts.inbox_bytes = 4096; // a few hundred small messages' worth, reached quickly and precisely
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = Transport::from_listener(NodeId(2), l, BTreeMap::new(), opts).unwrap();

    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut hs = Vec::new();
    crate::replication::write_handshake(&mut hs).unwrap();
    s.write_all(&hs).unwrap();
    s.flush().unwrap();
    let mut theirs = [0u8; 6];
    s.read_exact(&mut theirs).unwrap();

    // A message with a 1 KiB payload: five of them exceed a 4 KiB budget, and the caller never
    // drains.
    let frame = encode(&Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::Append {
            prev_round: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 1,
                round: 1,
                command: Command::WalBatch { start_lsn: 0, bytes: vec![0x11; 1024] },
            }],
            commit: 0,
        },
    })
    .unwrap();
    for _ in 0..40 {
        s.write_all(&frame).unwrap();
    }
    s.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && t.inbound_dropped() == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        t.inbound_dropped() > 0,
        "40 undrained messages against a 4096-byte budget refused none of them, so the inbox is \
         still unbounded and a peer chooses this process's memory"
    );
    assert!(
        t.inbox_bytes() <= 4096,
        "the inbox holds {} bytes, over its own {} byte budget",
        t.inbox_bytes(),
        4096
    );

    // ...and the budget is RETURNED as the caller drains, or the transport wedges shut after one
    // burst. This is the half a one-way counter would pass and a working bound must not.
    //
    // ⛔ This used to read one `inbox_bytes()`, drain ONE message, and assert the counter had
    // fallen. That is racy by construction and it failed on a Windows CI runner with
    // "(3294 then 3294)": the connection thread still had frames to deliver, so the byte the
    // drain refunded was charged again to the next message before the second read. The race is
    // in the ASSERTION, not in the transport — a refund that is immediately re-spent is the
    // bound working.
    //
    // Drain to quiescence instead and assert the budget returns to ZERO, which is strictly
    // stronger (it pins every byte of every message, not one message's), and race-free once the
    // sender has stopped: the client wrote exactly `SENT` frames and nothing else connects.
    const SENT: u64 = 40;
    let mut drained = 0u64;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut quiet = 0;
    while Instant::now() < deadline && quiet < 3 {
        match t.recv_timeout(Duration::from_millis(200)) {
            Some(m) => {
                assert!(matches!(m.body, Body::Append { .. }));
                drained += 1;
                quiet = 0;
            }
            None => quiet += 1,
        }
    }
    assert!(drained > 0, "nothing was deliverable at all, so the refund was never exercised");
    assert_eq!(
        t.inbox_bytes(),
        0,
        "after draining every deliverable message the inbox still holds {} bytes, so a drain does \
         not return its charge and the transport refuses for ever once full",
        t.inbox_bytes()
    );
    // Nothing lost and nothing counted twice: every frame the peer sent was either delivered or
    // refused. This is what makes the zero above a REFUND rather than a counter someone zeroed.
    assert_eq!(
        drained + t.inbound_dropped(),
        SENT,
        "{drained} delivered + {} refused != {SENT} sent",
        t.inbound_dropped()
    );
}


#[test]
fn concurrent_inbound_connections_are_capped_and_the_refusals_are_counted() {
    // This transport does NOT authenticate — F7 does — so anything that can reach the port may
    // open a connection, and each costs a thread and two descriptors. An unbounded accept loop is
    // therefore an unauthenticated peer choosing how many threads this process runs, which is the
    // same class of hole the frame limit closes for bytes.
    let mut opts = fast();
    opts.max_inbound_conns = 4;
    opts.idle_deadline = Duration::from_secs(60);
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = Transport::from_listener(NodeId(1), l, BTreeMap::new(), opts).unwrap();

    // Held open, so they stay established and the cap is actually reached.
    let mut held = Vec::new();
    for _ in 0..24 {
        if let Ok(mut s) = TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
            let mut hs = Vec::new();
            crate::replication::write_handshake(&mut hs).unwrap();
            let _ = s.write_all(&hs);
            let _ = s.flush();
            held.push(s);
        }
    }

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && t.refused_conns() == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        t.refused_conns() > 0,
        "24 connections against a cap of 4 refused none; the accept loop is unbounded and a peer \
         chooses how many threads this process runs"
    );
    assert!(
        t.live_inbound_conns() <= 4,
        "the cap is 4 but {} connections are established",
        t.live_inbound_conns()
    );
    drop(held);
}

#[test]
fn a_silent_peer_is_closed_on_the_idle_deadline_rather_than_pinning_a_thread_for_ever() {
    // A peer whose host vanishes without a FIN leaves a socket that never becomes readable and
    // never errors, so its thread and both descriptors are held for the life of the process.
    // Consensus heartbeats every few ticks, so silence past the deadline is a gone peer, not a slow
    // one.
    let mut opts = fast();
    opts.idle_deadline = Duration::from_millis(200);
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = Transport::from_listener(NodeId(1), l, BTreeMap::new(), opts).unwrap();

    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut hs = Vec::new();
    crate::replication::write_handshake(&mut hs).unwrap();
    s.write_all(&hs).unwrap();
    s.flush().unwrap();
    let mut theirs = [0u8; 6];
    s.read_exact(&mut theirs).unwrap();
    // ...and then say nothing at all, for ever.

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && t.idle_closed() == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(t.idle_closed(), 1, "a silent peer was never closed on the idle deadline");

    // The descriptor goes with it: this is the half that proves the close actually happened rather
    // than a counter being bumped.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && t.live_inbound_conns() > 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(t.live_inbound_conns(), 0, "the idle connection was counted but not closed");
}

#[test]
fn a_send_after_shutdown_is_refused_rather_than_silently_discarded() {
    // Every other loss in this module has a counter. A stopped transport used to return `Ok`,
    // increment `sent`, and discard the frame — the one silent loss left, and the module header
    // claims there are none.
    let (a, b) = pair(fast());
    a.shutdown();
    let e = a
        .send(&Message {
            from: NodeId(1),
            to: NodeId(2),
            term: 1,
            body: Body::PreVoteResp { granted: true },
        })
        .unwrap_err();
    assert!(format!("{e}").contains("shut down"), "got {e}");
    assert_eq!(a.refused_after_stop(), 1);
    assert_eq!(a.sent(), 0, "a refused send was counted as sent");
    drop(b);
}

#[test]
fn a_catalog_name_too_long_for_the_wire_is_refused_by_the_sender() {
    // `wal::log::write_str` writes `s.len() as u16`, UNCHECKED. A name over 65535 bytes gets a
    // truncated length prefix followed by its full bytes, so `encode` would emit a frame the peer
    // misparses. The receiver's re-encode check catches it — on the wrong side of the wire, where
    // the useful information (that THIS node produced rubbish) is gone.
    let long = "x".repeat(70_000);
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
                command: Command::Catalog {
                    op: DdlOp::CreateTable,
                    table: long.clone(),
                    columns: vec![("id".into(), DataType::Integer, false)],
                },
            }],
            commit: 0,
        },
    };
    let e = format!("{}", encode(&m).unwrap_err());
    // Either refusal is correct and which one fires depends on how the truncated length happens to
    // reparse: the record may come back as a DIFFERENT readable record, or as no readable record at
    // all. What must never happen is that it is framed and sent.
    assert!(
        e.contains("does not survive its own encoding")
            || e.contains("did not encode to a readable log record"),
        "a 70,000-byte table name was framed instead of refused: {}",
        &e[..e.len().min(300)]
    );

    // **The case only the re-encode check can catch**, and the reason the assertion above is not
    // enough on its own: a truncation that reparses SUCCESSFULLY, but to a different record.
    //
    // A mutant that deleted the re-encode check SURVIVED the assertion above, because an all-'x'
    // name makes the truncated record unparseable and the sibling branch refuses it instead. So the
    // check was untested. This name is built so the record parses cleanly and wrongly:
    //
    //   * its length is 65536 + 4, so `write_str`'s `as u16` writes a prefix of 4;
    //   * `take_str` therefore reads four bytes as the whole table name;
    //   * the next two bytes are NUL, so the column count reads as ZERO and parsing stops there;
    //   * `RecKind::deserialize`'s `Ddl` arm never compares its cursor to the buffer length, so the
    //     remaining 65534 bytes are silently discarded and it returns `Ok`.
    //
    // The result is a valid-looking `Ddl` for a table called "aaaa" with no columns. Only
    // re-encoding it and comparing the bytes notices that it is not what was sent.
    let mut sneaky = String::from("aaaa");
    sneaky.push('\0');
    sneaky.push('\0');
    sneaky.push_str(&"b".repeat(65536 + 4 - 6));
    assert_eq!(sneaky.len(), 65536 + 4, "the construction depends on this exact length");

    let m3 = Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::Append {
            prev_round: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 1,
                round: 1,
                command: Command::Catalog {
                    op: DdlOp::CreateTable,
                    table: sneaky.clone(),
                    columns: vec![("id".into(), DataType::Integer, false)],
                },
            }],
            commit: 0,
        },
    };
    let e3 = format!("{}", encode(&m3).unwrap_err());
    assert!(
        e3.contains("does not survive its own encoding"),
        "a name that reparses to a DIFFERENT record was framed and sent; only the re-encode check \
         can see this, and it did not fire. Got: {}",
        &e3[..e3.len().min(200)]
    );

    // And prove the premise rather than assuming it: the record really does deserialize cleanly to
    // the wrong thing. If this ever stops being true the test above stops testing anything, so the
    // premise is asserted rather than described.
    let rec = crate::wal::log::RecKind::Ddl {
        op: DdlOp::CreateTable,
        table: sneaky,
        dir_root: 0,
        time_travel_root: 0,
        columns: vec![("id".into(), DataType::Integer, false)],
    };
    let mut bytes = Vec::new();
    rec.serialize(&mut bytes);
    match crate::wal::log::RecKind::deserialize(&bytes) {
        Ok(crate::wal::log::RecKind::Ddl { table, columns, .. }) => {
            assert_eq!(table, "aaaa", "the truncated prefix no longer yields a short table name");
            assert!(columns.is_empty(), "the NUL bytes no longer read as a zero column count");
        }
        other => panic!(
            "the premise of this test no longer holds: the record does not reparse cleanly, it \
             gives {other:?}. The re-encode check is then untested again"
        ),
    }

    // Anti-vacuity: a name one byte inside the limit still encodes and round-trips, so the refusal
    // is about the truncation and not about long names in general.
    let ok = "y".repeat(u16::MAX as usize);
    let m2 = Message {
        from: NodeId(1),
        to: NodeId(2),
        term: 1,
        body: Body::Append {
            prev_round: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 1,
                round: 1,
                command: Command::Catalog {
                    op: DdlOp::CreateTable,
                    table: ok,
                    columns: vec![("id".into(), DataType::Integer, false)],
                },
            }],
            commit: 0,
        },
    };
    assert_eq!(decode_frame(&encode(&m2).unwrap()).unwrap(), m2, "a maximal name must round trip");
}


#[test]
fn a_peer_that_connects_and_says_nothing_is_closed_on_the_handshake_deadline() {
    // Otherwise a peer pins a thread and two descriptors by doing nothing at all, which is cheaper
    // for the attacker than any of the frames this codec refuses.
    let mut opts = fast();
    opts.handshake_deadline = Duration::from_millis(300);
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = Transport::from_listener(NodeId(1), l, BTreeMap::new(), opts).unwrap();

    let s = TcpStream::connect(addr).unwrap();
    // Not one byte is sent.

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && t.refused_handshakes() == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        t.refused_handshakes(),
        1,
        "a peer that connected and sent nothing was never refused, so it holds a thread for ever"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && t.live_inbound_conns() > 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(t.live_inbound_conns(), 0, "the silent connection was counted but not released");
    drop(s);
}

#[test]
fn a_broken_connection_is_reconnected_and_the_frame_lost_to_it_is_counted() {
    // **The transport's ONLY recovery path, and it had no test.** Every write failure drops the
    // connection and relies on the next iteration to redial; if that redial did not work, a single
    // transient error would partition this node permanently while every meter read healthy.
    //
    // Driven by a hand-rolled peer so the break is deliberate rather than hoped for: accept, read
    // one frame, abort the connection, then accept again and read the next.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let t = Transport::from_listener(
        NodeId(1),
        l,
        BTreeMap::from([(NodeId(2), peer_addr)]),
        fast(),
    )
    .unwrap();

    let msg = |term: u64| Message {
        from: NodeId(1),
        to: NodeId(2),
        term,
        body: Body::PreVoteResp { granted: true },
    };

    // --- first connection: take one frame, then abort it -------------------------------------
    let (mut c1, _) = listener.accept().expect("the transport should dial on its own");
    c1.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut hs = [0u8; 6];
    c1.read_exact(&mut hs).expect("the dialer sends its handshake first");
    let mut ours = Vec::new();
    crate::replication::write_handshake(&mut ours).unwrap();
    c1.write_all(&ours).unwrap();
    c1.flush().unwrap();

    t.send(&msg(1)).unwrap();
    let mut head = [0u8; 5];
    c1.read_exact(&mut head).expect("the first frame should arrive");
    assert_eq!(head[0], CONSENSUS_TAG);
    let n = u32::from_be_bytes(head[1..5].try_into().unwrap()) as usize;
    let mut body = vec![0u8; n];
    c1.read_exact(&mut body).unwrap();
    assert_eq!(decode(&body).unwrap().term, 1);

    // Close hard, then keep sending. `set_linger` would force an immediate RST but it is still
    // unstable (rust#88494), so this relies on the loop below instead: the first write after a FIN
    // may succeed, the peer answers RST, and the next one fails with EPIPE. Either way the sender
    // meets a real write error, which is the condition under test.
    let _ = c1.shutdown(Shutdown::Both);
    drop(c1);

    // --- keep sending; at least one frame is lost to the broken socket ------------------------
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut term = 2u64;
    while Instant::now() < deadline && t.lost_in_flight() == 0 {
        let _ = t.send(&msg(term));
        term += 1;
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        t.lost_in_flight() > 0,
        "no frame was recorded as lost, so a broken connection is losing messages silently"
    );

    // --- and it reconnects, which is the half that matters ------------------------------------
    let (mut c2, _) = listener
        .accept()
        .expect("the transport must redial after a write failure; without this the node is \
                 permanently partitioned by one transient error");
    c2.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    c2.read_exact(&mut hs).unwrap();
    c2.write_all(&ours).unwrap();
    c2.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut arrived = None;
    while Instant::now() < deadline && arrived.is_none() {
        let _ = t.send(&msg(term));
        term += 1;
        if c2.read_exact(&mut head).is_ok() {
            let n = u32::from_be_bytes(head[1..5].try_into().unwrap()) as usize;
            let mut body = vec![0u8; n];
            if c2.read_exact(&mut body).is_ok() {
                arrived = Some(decode(&body).unwrap().term);
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let got = arrived.expect("no frame arrived on the reconnected socket");
    assert!(got >= 2, "the frame on the new connection was term {got}, from before the break");
}


#[test]
fn the_spawn_failure_teardown_actually_stops_the_threads_it_is_given() {
    // The defect: a failed `thread::spawn` in `from_listener` returned via `?`, which dropped the
    // local `threads` vec — DETACHING every sender thread already started — and dropped the only
    // remaining `stop` handle. Those threads then dialled their peers for the life of the process
    // with nothing able to tell them to stop, and the caller held nothing that could reap them.
    //
    // The failure itself is not reachable from a test: it needs `pthread_create` to return EAGAIN,
    // i.e. RLIMIT_NPROC exhaustion, which a test cannot arrange without wrecking the run. So the
    // TEARDOWN is tested directly instead — that is the part that was missing, and a test of it is
    // worth more than no test at all. Named so nobody mistakes it for a test of the spawn failure.
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(Counters::default());
    let opts = fast();
    let ob = Arc::new(Outbox {
        addr: "127.0.0.1:1".parse().unwrap(), // refuses instantly, so the loop is in its retry path
        state: Mutex::new(OutboxState { queue: VecDeque::new(), live: None, stopped: false }),
        woken: Condvar::new(),
        depth: 4,
        dropped: std::sync::atomic::AtomicU64::new(0),
    });
    let ob_c = Arc::clone(&ob);
    let stop_c = Arc::clone(&stop);
    let counters_c = Arc::clone(&counters);
    let opts_c = opts.clone();
    let h = std::thread::spawn(move || sender_loop(ob_c, stop_c, counters_c, opts_c));

    // Let it get properly into the dial-and-retry loop, which is where an orphan would live.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && counters.connect_failures.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        counters.connect_failures.load(Ordering::SeqCst) > 0,
        "the thread never reached its retry loop, so this test would pass without stopping anything"
    );

    // The teardown must return only once that thread is done. If it did not stop it, the join below
    // would hang and the test would time out rather than pass.
    stop_started(&stop, &[Arc::clone(&ob)], vec![h]);
    assert!(stop.load(Ordering::SeqCst), "the teardown did not raise the stop flag");
    assert!(ob.state.lock().unwrap().stopped, "the teardown did not stop the outbox");
}
