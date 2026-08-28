//! **The one that matters.** Three real processes, writes in flight, `kill -9` the leader, and
//! nothing acknowledged may be lost.
//!
//! Every other consensus test in this repo runs the state machine in a single address space —
//! `consensus/tests_sim.rs` drives it deterministically, `tests_election.rs` and
//! `tests_replicate.rs` step it by hand. Those prove the algorithm. None of them proves that the
//! *driver* — `consensus/node.rs`, which owns the clock, the socket and the fsync — wires it to a
//! machine correctly, because in all of them the clock, the socket and the disk are the test's own.
//!
//! So this one uses none of that. Three OS processes, three TCP listeners, three directories on the
//! real filesystem, and a signal that a process cannot catch, handle or flush through.
//!
//! # What counts as "acknowledged"
//!
//! An `APPLIED n` line on the leader's stdout, and nothing weaker. The driver emits it from the
//! applier, which it calls only for a round consensus has committed — meaning a quorum has it on
//! disk. A proposal that was merely *sent*, or merely assigned a round, is deliberately not
//! counted: those are exactly the writes a correct system is allowed to lose.
//!
//! # Why the assertion is on the SURVIVORS' state, not on a count
//!
//! "The cluster kept working" is not the property. A cluster that elects a new leader and has
//! silently dropped an acknowledged write passes any liveness check you write. So the surviving
//! leader is asked what it holds, and every acknowledged `n` must be in it.
//!
//! # Forced to fire, and the two injections that did NOT fire
//!
//! The loss assertion was proven capable of failing before it was trusted: injecting "a follower
//! stores bytes that are not the ones it acknowledges" into `node.rs`'s `Persist` arm produced
//! `acknowledged=19 held=0 missing=19` and the intended failure. The clean tree then passed three
//! consecutive runs.
//!
//! **Two other injections passed this test, and that is a limit worth stating rather than a
//! success.** Measured 2026-08-28 on this machine:
//!
//! 1. *Commit without a quorum* (`quorum_matched` returning the leader's own `durable`). Passed:
//!    replication to localhost peers completes before any externally timed kill can land, so the
//!    survivors held the writes anyway.
//! 2. *Acknowledge on the leader's own fsync instead of on commit.* Passed, for the same reason —
//!    the leader and its followers share one disk, so the ack-to-replication gap is about one
//!    localhost round trip, and a `kill` issued by a test process cannot be timed into it. The
//!    measurement was `acknowledged=14 held=14 missing=0`.
//!
//! So this test detects a survivor that does not *hold* what was acknowledged. It does **not**
//! detect a leader that acknowledges *ahead* of its quorum, because on one machine that defect has
//! no observable window. That property is covered where it can be forced deterministically:
//! `src/consensus/sim.rs`, whose chaos sweep drives partitions and crashes from a seed and asserts
//! `a_committed_round_is_never_lost_across_a_chaos_sweep`. Neither test subsumes the other — the
//! simulator owns the algorithm under adversarial scheduling, this one owns the driver against a
//! real clock, a real socket, a real disk and a signal that cannot be caught.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

/// How many writes the client offers. Enough that the kill lands with some still in flight.
const WRITES: u64 = 300;
/// Kill the leader once this many are acknowledged, so the failure is mid-stream and not after it.
const KILL_AFTER: usize = 12;
/// Generous: a loaded CI runner is slow, and a flaky timeout here would be indistinguishable from
/// the bug this test exists to catch.
const ELECTION_BUDGET: Duration = Duration::from_secs(45);

// ---------------------------------------------------------------- freshness

/// Refuse to run against a stale example binary.
///
/// `cargo test` does NOT rebuild examples, so a test that spawns one can silently exercise a build
/// from before the change under test. `integration_replication_e2e.rs` records that this is not
/// hypothetical: its first fire-check passed while the injected defect was live, because the
/// binary predated it. A test that cannot observe the code it claims to test is worse than none.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| {
            panic!("{} is missing ({e}); run: cargo build --examples", bin.display())
        })
        .modified()
        .expect("mtime");
    let own_src = std::fs::metadata("examples/consensus_node.rs")
        .ok()
        .and_then(|m| m.modified().ok());
    let newest_src = [walk_newest(Path::new("src")), own_src].into_iter().flatten().max();
    if let Some(src_time) = newest_src {
        assert!(
            bin_time >= src_time,
            "{} is older than src/ or examples/consensus_node.rs — cargo test does not rebuild \
             examples, so this would test a stale binary. Run: cargo build --examples",
            bin.display()
        );
    }
}

fn walk_newest(dir: &Path) -> Option<std::time::SystemTime> {
    let mut newest = None;
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        let t = if p.is_dir() {
            walk_newest(&p)
        } else {
            std::fs::metadata(&p).ok().and_then(|m| m.modified().ok())
        };
        if let Some(t) = t {
            newest = Some(match newest {
                Some(cur) if t <= cur => cur,
                _ => t,
            });
        }
    }
    newest
}

fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    // "" on unix, ".exe" on Windows — hardcoding the unix name is how every example-spawning test
    // in this repo once failed on the Windows runner with "cannot find the file specified".
    let out = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&out);
    out
}

// ---------------------------------------------------------------- the harness

/// One node's process, its stdin, and every line it has said.
struct NodeProc {
    id: u32,
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
    addr: String,
    stderr_path: PathBuf,
    dead: bool,
}

impl NodeProc {
    fn tell(&mut self, s: &str) {
        // A dead node's pipe is closed; telling it something is not an error, it is the point.
        let _ = writeln!(self.stdin, "{s}");
        let _ = self.stdin.flush();
    }

    /// Whatever the process wrote to stderr — captured to a FILE rather than discarded, so a
    /// failure can quote the node's last words instead of reporting an unexplained timeout.
    fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn(bin: &Path, id: u32, root: &Path) -> NodeProc {
    let dir = root.join(format!("n{id}"));
    std::fs::create_dir_all(&dir).expect("node dir");
    let stderr_path = root.join(format!("n{id}.stderr"));
    let errf = std::fs::File::create(&stderr_path).expect("stderr file");

    let mut child = Command::new(bin)
        .arg(id.to_string())
        .arg(&dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(errf))
        .spawn()
        .unwrap_or_else(|e| panic!("could not spawn node {id}: {e}"));

    let stdin = child.stdin.take().expect("stdin");
    let stdout = child.stdout.take().expect("stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                return;
            }
        }
    });

    let mut n = NodeProc {
        id,
        child,
        stdin,
        lines: rx,
        addr: String::new(),
        stderr_path,
        dead: false,
    };
    // Wait for the address it actually bound, rather than picking a port and hoping. See the
    // example's header: closing a port to publish it leaves a window in which it is unowned.
    let ready = n
        .lines
        .recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|e| panic!("node {id} never said READY ({e}); stderr: {}", n.stderr()));
    let addr = ready
        .strip_prefix("READY ")
        .unwrap_or_else(|| panic!("node {id} said {ready:?} instead of READY"));
    n.addr = addr.to_string();
    n
}

/// Drain everything every live node has said, without blocking on any of them.
fn pump(nodes: &mut [NodeProc], sink: &mut Vec<(u32, String)>) {
    for n in nodes.iter_mut() {
        if n.dead {
            continue;
        }
        while let Ok(l) = n.lines.try_recv() {
            sink.push((n.id, l));
        }
    }
}

/// Block until `pred` holds over everything said so far, or fail with what was actually said.
fn wait_for<F>(
    nodes: &mut [NodeProc],
    sink: &mut Vec<(u32, String)>,
    budget: Duration,
    what: &str,
    mut pred: F,
) where
    F: FnMut(&[(u32, String)]) -> bool,
{
    let deadline = Instant::now() + budget;
    loop {
        pump(nodes, sink);
        if pred(sink) {
            return;
        }
        if Instant::now() >= deadline {
            let transcript: Vec<String> =
                sink.iter().map(|(i, l)| format!("  n{i}: {l}")).collect();
            let errs: Vec<String> = nodes
                .iter()
                .map(|n| format!("  n{} stderr: {}", n.id, n.stderr().replace('\n', " | ")))
                .collect();
            panic!(
                "timed out after {budget:?} waiting for {what}.\ntranscript:\n{}\n{}",
                transcript.join("\n"),
                errs.join("\n")
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The node most recently seen announcing itself leader, and the term it did it in.
fn latest_leader(sink: &[(u32, String)], exclude: Option<u32>) -> Option<(u32, u64)> {
    let mut best: Option<(u32, u64)> = None;
    for (id, line) in sink {
        if Some(*id) == exclude {
            continue;
        }
        if let Some(rest) = line.strip_prefix("ROLE Leader ") {
            if let Ok(term) = rest.trim().parse::<u64>() {
                if best.is_none_or(|(_, t)| term >= t) {
                    best = Some((*id, term));
                }
            }
        }
    }
    best
}

fn acked(sink: &[(u32, String)], node: u32) -> Vec<u64> {
    sink.iter()
        .filter(|(i, _)| *i == node)
        .filter_map(|(_, l)| l.strip_prefix("APPLIED "))
        .filter_map(|r| r.split_whitespace().next())
        .filter_map(|n| n.parse::<u64>().ok())
        .collect()
}

// ---------------------------------------------------------------- the test

#[test]
fn a_killed_leader_is_replaced_and_no_acknowledged_write_is_lost() {
    let bin = example_bin("consensus_node");
    let root = tempfile::tempdir().expect("tempdir");
    let ids = [1u32, 2, 3];

    let mut nodes: Vec<NodeProc> = ids.iter().map(|&i| spawn(&bin, i, root.path())).collect();

    // Every node learns the whole cluster only once every listener is bound and its real address
    // is known. This is the step that makes the test race-free rather than merely usually-passing.
    let addrs: BTreeMap<u32, String> =
        nodes.iter().map(|n| (n.id, n.addr.clone())).collect();
    let members = ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",");
    for n in nodes.iter_mut() {
        let peers = addrs
            .iter()
            .filter(|(i, _)| **i != n.id)
            .map(|(i, a)| format!("{i}={a}"))
            .collect::<Vec<_>>()
            .join(",");
        n.tell(&format!("START {members} {peers}"));
    }

    let mut sink: Vec<(u32, String)> = Vec::new();

    // ---- 1. a leader emerges at all
    wait_for(&mut nodes, &mut sink, ELECTION_BUDGET, "a first leader", |s| {
        latest_leader(s, None).is_some()
    });
    let (leader_id, first_term) = latest_leader(&sink, None).expect("just waited for it");

    // ---- 2. writes, and a kill that lands while they are still in flight
    let leader_ix = nodes.iter().position(|n| n.id == leader_id).expect("leader is one of ours");
    for n in 1..=WRITES {
        nodes[leader_ix].tell(&format!("PROPOSE {n}"));
    }

    wait_for(&mut nodes, &mut sink, ELECTION_BUDGET, "the first writes to be acknowledged", |s| {
        acked(s, leader_id).len() >= KILL_AFTER
    });

    let survivors: Vec<u32> = ids.iter().copied().filter(|i| *i != leader_id).collect();
    let acknowledged = acked(&sink, leader_id);
    assert!(
        acknowledged.len() >= KILL_AFTER,
        "expected at least {KILL_AFTER} acknowledged writes before the kill, got {}",
        acknowledged.len()
    );

    // SIGKILL. Not a shutdown, not a drop: `Child::kill` is SIGKILL on unix, which the process
    // cannot catch, cannot handle and cannot flush a buffer through. A graceful stop would let the
    // leader finish its in-flight work and would test nothing about failure.
    nodes[leader_ix].child.kill().expect("kill the leader");
    let _ = nodes[leader_ix].child.wait();
    nodes[leader_ix].dead = true;

    // ---- 3. a NEW leader, in a strictly later term
    wait_for(&mut nodes, &mut sink, ELECTION_BUDGET, "a new leader after the kill", |s| {
        matches!(latest_leader(s, Some(leader_id)), Some((_, t)) if t > first_term)
    });
    let (new_leader, new_term) =
        latest_leader(&sink, Some(leader_id)).expect("just waited for it");
    assert!(
        survivors.contains(&new_leader),
        "the new leader {new_leader} is not one of the survivors {survivors:?}"
    );
    assert!(
        new_term > first_term,
        "the new leader is in term {new_term}, not above the dead leader's {first_term}; a \
         successor that did not raise the term is the split-brain this design forbids"
    );

    // ---- 4. THE PROPERTY: nothing acknowledged was lost.
    let new_ix = nodes.iter().position(|n| n.id == new_leader).expect("survivor");
    // Ask repeatedly: the first DUMP may be served before the new leader has replayed everything
    // its predecessor committed, and the property is about the settled state.
    let deadline = Instant::now() + ELECTION_BUDGET;
    let mut missing: Vec<u64>;
    loop {
        nodes[new_ix].tell("DUMP");
        std::thread::sleep(Duration::from_millis(200));
        pump(&mut nodes, &mut sink);
        let held: Vec<u64> = sink
            .iter()
            .filter(|(i, l)| *i == new_leader && l.starts_with("DUMP "))
            .last()
            .map(|(_, l)| {
                l["DUMP ".len()..]
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .filter_map(|s| s.parse::<u64>().ok())
                    .collect()
            })
            .unwrap_or_default();
        missing = acknowledged.iter().copied().filter(|n| !held.contains(n)).collect();
        if missing.is_empty() || Instant::now() >= deadline {
            break;
        }
    }

    eprintln!(
        "DIAG acknowledged={} held={} missing={}",
        acknowledged.len(),
        acknowledged.len() - missing.len(),
        missing.len()
    );
    assert!(
        missing.is_empty(),
        "the surviving leader n{new_leader} is missing {} write(s) that n{leader_id} had already \
         ACKNOWLEDGED before it was killed: {missing:?}.\nacknowledged was {acknowledged:?}\n\
         This is acknowledged data loss — the single property the whole of Phase F exists to buy.",
        missing.len()
    );

    // Anti-vacuity. Every assertion above passes trivially if nothing was ever acknowledged, and a
    // test that can pass while proving nothing is the failure mode this repo has shipped three
    // times. So: the acknowledgement set must be non-trivial, and the cluster must genuinely have
    // survived rather than merely gone quiet.
    assert!(
        acknowledged.len() >= KILL_AFTER,
        "vacuous: only {} writes were ever acknowledged",
        acknowledged.len()
    );
    nodes[new_ix].tell("STATUS");
    wait_for(&mut nodes, &mut sink, ELECTION_BUDGET, "the new leader to report status", |s| {
        s.iter().any(|(i, l)| *i == new_leader && l.starts_with("STATUS "))
    });
}
