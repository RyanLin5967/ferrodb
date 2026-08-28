//! One ferrodb consensus node, as its own process, driven over stdin/stdout.
//!
//! Exists for `tests/integration_consensus_failover.rs`, which needs three genuinely separate
//! processes: a `kill -9` has to destroy a real address space for the test to mean anything. An
//! in-process cluster sharing a heap and an allocator cannot fail the way a machine does.
//!
//! # Why the peer map arrives on stdin instead of on argv
//!
//! Every node's peer map needs every other node's address, so binding node A to discover its port
//! and then constructing node B is circular. The usual way out — pick three free ports, close them,
//! hand them over — leaves a window in which a port is published but unowned, and something else on
//! a busy machine can take it. So this binds its listener FIRST, prints the address it actually got,
//! and waits to be told about the others. `Transport::from_listener` is the primitive for exactly
//! this reason.
//!
//! # Protocol
//!
//! Out: `READY <addr>` · `ROLE <role> <term>` · `APPLIED <n> <round>` · `DUMP <n,n,...>` ·
//!      `REFUSED <n>` · `TERM <term>`
//! In:  `START <members-csv> <id=addr,...>` · `PROPOSE <n>` · `DUMP` · `STATUS`

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc;
use std::time::Duration;

use ferrodb::consensus::config::Config;
use ferrodb::consensus::node::{Applier, Node, NodeOptions};
use ferrodb::consensus::{Command, Entry, NodeId, Role};
use ferrodb::error::FerroError;

/// Bytes of redo carried by one `PROPOSE`. See the comment at its construction.
const PAYLOAD_BYTES: usize = 32 * 1024;

/// Records what consensus agreed on, and says so on stdout the moment it does.
///
/// The print is the *acknowledgement*: the test treats an `APPLIED n` line as the client having
/// been told the write is durable, and afterwards demands that `n` survive on the new leader. So
/// this must print only from `apply`, which the driver calls only for a committed round.
struct Speaking {
    seen: Vec<u64>,
}

impl Applier for Speaking {
    fn apply(&mut self, e: &Entry) -> Result<(), FerroError> {
        if let Command::WalBatch { start_lsn, .. } = &e.command {
            self.seen.push(*start_lsn);
            say(&format!("APPLIED {} {}", start_lsn, e.round));
        }
        Ok(())
    }
}

/// Print and flush. An unflushed line is a line the test never sees, and the symptom is a timeout
/// that reads exactly like a node that never became leader.
fn say(s: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{s}");
    let _ = out.flush();
}

fn parse_peers(s: &str) -> BTreeMap<NodeId, SocketAddr> {
    let mut m = BTreeMap::new();
    for part in s.split(',').filter(|p| !p.is_empty()) {
        let (id, addr) = part.split_once('=').unwrap_or_else(|| panic!("bad peer spec {part:?}"));
        m.insert(
            NodeId(id.parse().unwrap_or_else(|e| panic!("bad node id in {part:?}: {e}"))),
            addr.parse().unwrap_or_else(|e| panic!("bad addr in {part:?}: {e}")),
        );
    }
    m
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: {} <node-id> <dir>", args[0]);
        std::process::exit(2);
    }
    let self_id = NodeId(args[1].parse().expect("node id"));
    let dir = std::path::PathBuf::from(&args[2]);

    // Bind before announcing anything, so the address printed is one this process owns.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    say(&format!("READY {addr}"));

    // stdin on its own thread: the main loop must keep ticking while it waits for a command, and a
    // blocking read here would stop the clock. A node whose clock stops cannot notice a dead
    // leader, which is the one thing this binary exists to demonstrate.
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });

    // Wait for the configuration. Nothing else is legal first: a node that started campaigning on
    // an empty configuration would be a cluster of one.
    let (members, peers) = loop {
        let line = rx.recv().expect("stdin closed before START");
        let mut it = line.split_whitespace();
        match it.next() {
            Some("START") => {
                let members: Vec<NodeId> = it
                    .next()
                    .expect("START needs a member list")
                    .split(',')
                    .map(|s| NodeId(s.parse().expect("member id")))
                    .collect();
                let peers = parse_peers(it.next().unwrap_or(""));
                break (members, peers);
            }
            _ => eprintln!("ignoring {line:?} before START"),
        }
    };

    let cfg = Config::new(members, 1, 0);
    // A distinct seed per node. Two nodes drawing the same election timeout split the vote every
    // term, and the cluster livelocks with no leader and no error.
    let opts = NodeOptions::new(&dir, peers, 0x9E37_79B9_u64.wrapping_mul(self_id.0 as u64 + 1))
        .tick_of(Duration::from_millis(20));

    let mut node = Node::start(self_id, cfg, listener, opts, Speaking { seen: Vec::new() })
        .unwrap_or_else(|e| panic!("node {self_id:?} could not start: {e:?}"));

    let mut last_role = Role::Follower;
    let mut last_term = 0;

    loop {
        if let Err(e) = node.poll(Duration::from_millis(5)) {
            eprintln!("node {self_id:?} poll failed: {e:?}");
            std::process::exit(1);
        }

        for (role, term, _leader) in node.take_transitions() {
            if (role, term) != (last_role, last_term) {
                say(&format!("ROLE {role:?} {term}"));
                last_role = role;
                last_term = term;
            }
        }
        for why in node.take_refusals() {
            eprintln!("refused: {why:?}");
            say("REFUSED");
        }

        while let Ok(line) = rx.try_recv() {
            let mut it = line.split_whitespace();
            match it.next() {
                Some("PROPOSE") => {
                    let n: u64 = it.next().expect("PROPOSE needs a number").parse().expect("n");
                    // A realistically sized payload, not a token one. Two reasons, and the second
                    // is the important one: an entry that encodes to eight bytes does not exercise
                    // the transport's framing, and — because replication of it completes faster
                    // than any kill can be timed — it makes the cluster's behaviour under failure
                    // independent of whether replication actually happened. With a real batch the
                    // pipeline has inertia, so a leader that acknowledged ahead of its followers is
                    // visibly ahead of them when it dies. 32 KiB is eight ferrodb pages of redo.
                    let c = Command::WalBatch { start_lsn: n, bytes: vec![n as u8; PAYLOAD_BYTES] };
                    if let Err(e) = node.propose(c) {
                        eprintln!("propose {n} failed: {e:?}");
                    }
                }
                Some("DUMP") => {
                    let seen = &node.applier().seen;
                    let joined =
                        seen.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(",");
                    say(&format!("DUMP {joined}"));
                }
                Some("STATUS") => {
                    say(&format!(
                        "STATUS {:?} {} commit={} applied={}",
                        node.role(),
                        node.term(),
                        node.commit_round(),
                        node.applied()
                    ));
                }
                Some("") | None => {}
                Some(other) => eprintln!("unknown command {other:?}"),
            }
        }
    }
}
