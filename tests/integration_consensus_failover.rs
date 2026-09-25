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
        // **A `src/**/tests_*.rs` file does not link into an example binary**, so editing one
        // cannot make that binary stale — and `cargo build --examples` correctly does not rebuild
        // for it, because those modules are `#[cfg(test)]` and are not part of the lib's non-test
        // fingerprint. Counting them made this guard fire on a tree whose examples WERE fresh:
        // one edit to `src/consensus/tests_transport.rs` failed 53 tests across 5 targets.
        // The convention is enforced, not assumed — see `test_only_sources_are_cfg_test_gated`.
        if p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("tests_")) {
            continue;
        }
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

// ================================================================ D42: what an expiry MEANS
//
// A wall-clock deadline in a test cannot tell "the cluster failed" from "this test was never
// scheduled". This file's 45 s budget has produced FOUR false REDs under agent-fleet load
// (`suite-d19-merge-0245Z`, `suite-E79c-0457Z`, `suite-E79c-1036Z`, D40 run 1), each costing a
// re-run and twice nearly costing a wrong diagnosis. The test passes alone in ~3.1 s, so 45 s is
// already a 15x margin: it is not expiring because it is too small.
//
// ⛔ `ELECTION_BUDGET` IS NOT THE FIX AND IS NOT TOUCHED. Raising it is "never edit a test to make
// it pass" wearing a constant, and it only moves the load at which the test flakes. What follows
// changes what a 45 s expiry is allowed to MEAN, never how long the test waits.
//
// TWO SIGNALS, AND NEITHER IS SUFFICIENT ALONE.
//
//   B. The poll loop's own iteration count. `pump` uses `try_recv` and never blocks, so this loop
//      is pure sleep — its iteration count is a direct in-process measurement of whether THIS
//      THREAD got scheduled. A genuinely wedged cluster does not stop this thread from spinning,
//      so a real failure still reaches its nominal count and still reports FAILED at 45 s exactly
//      as it does today. Only a starved *test process* reclassifies. That asymmetry is the whole
//      point: a mechanism that changed the verdict for both causes would be a relabelling.
//      ⚠ Its blind spot, stated here rather than discovered later: the nodes are CHILD PROCESSES.
//      This thread can be scheduled fine while they starve, and then B alone says FAILED wrongly.
//
//   C. The node child processes' CPU consumed per wall second. This covers exactly B's blind spot,
//      and B covers C's — B needs no child bookkeeping and catches the case where our own thread is
//      the starved one. This is not "check the load average": load is ambient and observational,
//      while this measures THE SUBJECT, so a busy machine that nonetheless gave our children CPU
//      correctly yields FAILED. (Nothing here reads a load average, deliberately.)
//
// Prior art, named rather than re-derived: the Linux hung-task detector separates "busy" from
// "never ran" by whether a watchdog was TOUCHED, not by elapsed wall time — B is that touch count.
// cgroup v2 PSI reports "runnable but not running" directly and is the right instrument, but it is
// Linux-only and this machine is macOS; B is its portable degradation. JVM GC pauses are reported
// with both wall and CPU for the same reason C exists.
//
// The long-term shape is NOT this classifier: it is an injectable logical clock in the nodes, which
// deletes the ambiguous outcome instead of classifying it (FoundationDB, TigerBeetle, `madsim`).
// That changes `src/`'s consensus timers to fix a harness problem, so it is recorded in
// `SCALE-DESIGN.md` D42 option D as the right next row and is deliberately not taken here.

/// The interval the wait loops below sleep between polls.
///
/// Named rather than written as a literal at each `sleep`, because the classifier divides the
/// budget by it to get the count a scheduled loop would reach. A literal that drifted from the
/// divisor would silently move the starvation threshold instead of failing to compile.
const POLL: Duration = Duration::from_millis(10);

/// The settled-state loop at the end of the test polls slower: each of its iterations costs a round
/// trip to a node, not a `try_recv`.
const DUMP_POLL: Duration = Duration::from_millis(200);

/// Fraction of its nominal iteration count below which a poll loop did not get the CPU.
///
/// MEASURED, not chosen. `bench/d42_childcpu_probe.py` runs this exact 10 ms loop for one
/// `ELECTION_BUDGET` and reaches 3820 of 4500 nominal iterations — a ratio of **0.849** — on a
/// machine already carrying loadavg 13.5 (`bench/d42_childcpu_probe.txt`). This threshold is a
/// decade below that measured floor. A decade and not a tuned number on purpose: the design
/// predicts a starved loop reaches *tens* of iterations (~0.005), so 0.085 sits near the geometric
/// middle of a ~170x gap and no value inside that gap changes a verdict.
const STARVED_SELF: f64 = 0.085;

/// CPU-seconds per live node per wall-second below which the node processes did not get the CPU.
///
/// MEASURED in the same window by the same probe: three idle nodes with a settled leader consume
/// 0.58 CPU-seconds over 45.21 s of wall — **0.0043 cpu-s per node-wall-s**. This is a decade below
/// that. An idle cluster is the right floor to calibrate against because it is the LOWER bound on
/// healthy: a cluster that is genuinely broken re-runs elections and burns strictly more.
///
/// ⚠ **THE DESIGN'S STATED PREMISE FOR THIS SIGNAL IS WRONG BY FOUR ORDERS OF MAGNITUDE**, and the
/// correction is recorded here rather than left in a chat message. `SCALE-DESIGN.md` D42 option C
/// says "~40 s of child CPU over 45 s of wall means they ran and genuinely failed". They do not:
/// these nodes block in `recv_timeout(min(5ms, until_tick))` (`Node::poll`), so a
/// HEALTHY node burns 0.4 % of a core, not 90 %. The DIRECTION option C rests on survives — a node
/// that is not scheduled consumes less than one that is — but the magnitude does not, and a
/// threshold set from the design's number would have called every run on this machine starved.
///
/// ⚠ **Known blind spot of this constant, stated in the mechanism:** `ps` reports CPU time to
/// centisecond resolution, so over a 45 s window with two live nodes this threshold sits about
/// four `ps` ticks above zero. C therefore fires only when the children got very nearly NO cpu at
/// all. That makes it conservative in the safe direction — it will not manufacture an INCONCLUSIVE
/// for a healthy-but-failing cluster — at the cost of missing MODERATE child starvation, which B
/// would have to catch instead.
const STARVED_CHILD: f64 = 0.00043;

/// `tools/verify-suite.sh` greps for these two strings to route a run into its existing REFUSAL
/// channel — the one that already says "a run that collected nothing has not passed, whatever its
/// exit code says". INCONCLUSIVE is that same principle applied to a different cause.
///
/// The strings are duplicated in that shell script by necessity, since it cannot read a Rust
/// constant. `tools/verify-suite-selftest.sh` part 3 fails if the two copies ever disagree, so the
/// duplication cannot rot silently.
const VERDICT_INCONCLUSIVE: &str = "FERRODB-VERDICT: INCONCLUSIVE";
const VERDICT_CLASSIFIER_BROKEN: &str = "FERRODB-VERDICT: CLASSIFIER-BROKEN";

/// What signal C managed to say. Three states, not two, because "the instrument failed" and "the
/// instrument legitimately cannot speak here" must not be collapsed: collapsing them is how a guard
/// quietly trades coverage for precision.
enum ChildCpu {
    /// Total CPU seconds across the sampled pids.
    Sampled(f64),
    /// C cannot speak, for a reason that is not a defect. The classifier falls back to B alone and
    /// PRINTS this reason, so the remaining blind spot is named in the artifact.
    Unavailable(String),
    /// C should have worked and did not. The run does not get to be a verdict in either direction.
    Broken(String),
}

/// Total CPU seconds consumed by `pids`, read from `ps`.
fn sample_child_cpu(pids: &[u32]) -> ChildCpu {
    if !cfg!(unix) {
        return ChildCpu::Unavailable(
            "child CPU accounting needs `ps`, which is unix-only; signal C cannot run on this \
             platform and the verdict below rests on signal B alone"
                .to_string(),
        );
    }
    if pids.is_empty() {
        return ChildCpu::Unavailable("no live node processes left to sample".to_string());
    }
    let list = pids.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",");
    // `-e` is deliberately absent. `ps -eo ... -p PID` IGNORES the pid filter and reports every
    // process on the machine, which would sum the whole box into this measurement and report a
    // starved cluster as a healthy one.
    let out = match Command::new("ps").args(["-o", "pid=,time=", "-p", &list]).output() {
        Ok(o) => o,
        Err(e) => return ChildCpu::Broken(format!("could not run `ps`: {e}")),
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let (mut total, mut seen) = (0.0f64, 0usize);
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(pid), Some(t)) = (it.next(), it.next()) else { continue };
        let Ok(pid) = pid.parse::<u32>() else { continue };
        if !pids.contains(&pid) {
            continue;
        }
        match parse_ps_time(t) {
            Some(s) => {
                total += s;
                seen += 1;
            }
            None => {
                return ChildCpu::Broken(format!(
                    "`ps` printed a CPU time this cannot parse: {t:?}. Refusing to guess at it — a \
                     misparsed time would decide the verdict silently."
                ));
            }
        }
    }
    if seen == 0 {
        return ChildCpu::Broken(format!(
            "`ps -o pid=,time= -p {list}` reported none of the {} node process(es) (exit {:?}). The \
             instrument failed here, not the cluster, so this run is not a verdict in either \
             direction.",
            pids.len(),
            out.status.code()
        ));
    }
    if seen < pids.len() {
        return ChildCpu::Unavailable(format!(
            "`ps` reported only {seen} of {} node processes, so one exited during the window and \
             its CPU cannot be differenced; the verdict below rests on signal B alone",
            pids.len()
        ));
    }
    ChildCpu::Sampled(total)
}

/// Parse `ps -o time=`: `MM:SS.cc`, or `HH:MM:SS.cc`.
///
/// Measured on this machine: `ps -o time= -p 1` prints `303:18.01`. The minutes field is NOT capped
/// at 59, so a parser that assumed a leading field must be hours would read 303 hours off an
/// uptime of 14 days and every child would look infinitely healthy.
fn parse_ps_time(s: &str) -> Option<f64> {
    let mut parts: Vec<&str> = s.split(':').collect();
    let secs: f64 = parts.pop()?.trim().parse().ok()?;
    let mins: f64 = parts.pop().map(|m| m.trim().parse().ok()).unwrap_or(Some(0.0))?;
    let hours: f64 = parts.pop().map(|h| h.trim().parse().ok()).unwrap_or(Some(0.0))?;
    if !parts.is_empty() {
        return None;
    }
    Some(hours * 3600.0 + mins * 60.0 + secs)
}

/// Every live node's pid. A dead node has been reaped, so sampling it would report nothing and drag
/// the aggregate to zero — which is starvation's own signature, and therefore the one mistake this
/// function must not make.
fn live_pids(nodes: &[NodeProc]) -> Vec<u32> {
    nodes.iter().filter(|n| !n.dead).map(|n| n.child.id()).collect()
}

/// Which way a deadline expiry gets to be read.
#[derive(PartialEq, Eq, Debug)]
enum Kind {
    /// Both signals say this process and its nodes got the machine. The expiry is the cluster's.
    Failed,
    /// One of them says we were descheduled. The expiry says nothing about the cluster.
    Inconclusive,
    /// The classifier could not measure. The run is not a verdict in either direction.
    ClassifierBroken,
}

/// A poll loop that measures whether IT — and the processes it is waiting on — got the machine.
struct LoadWitness {
    started: Instant,
    deadline: Instant,
    budget: Duration,
    poll: Duration,
    /// Taken at the HALFWAY mark rather than at entry, so a wait that succeeds in ~3 s — which is
    /// every healthy run — spawns no `ps` and prints nothing at all. Both signals are then measured
    /// over the same second half of the budget, which is also the only stretch of a doomed wait in
    /// which nothing is happening.
    baseline: Option<(Instant, u64, ChildCpu)>,
    iters: u64,
    last: Instant,
    max_stall: Duration,
}

impl LoadWitness {
    fn new(budget: Duration, poll: Duration) -> Self {
        let now = Instant::now();
        Self {
            started: now,
            deadline: now + budget,
            budget,
            poll,
            baseline: None,
            iters: 0,
            last: now,
            max_stall: Duration::ZERO,
        }
    }

    fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// One turn of the loop: sleep, count it, and notice how long the sleep actually took.
    fn sleep_and_tick(&mut self, pids: &[u32]) {
        std::thread::sleep(self.poll);
        let now = Instant::now();
        self.iters += 1;
        self.max_stall = self.max_stall.max(now - self.last);
        self.last = now;
        if self.baseline.is_none() && now - self.started >= self.budget / 2 {
            self.baseline = Some((now, self.iters, sample_child_cpu(pids)));
        }
    }

    /// Read both signals and say what this expiry is allowed to mean.
    fn classify(&self, pids: &[u32]) -> (Kind, String) {
        let now = Instant::now();
        // Measure over the baseline window when there is one, and over the whole budget when
        // starvation was severe enough that the halfway mark was never observed — which is itself
        // evidence, so it is reported rather than silently widened.
        let (from, iters_at, cpu0, note) = match &self.baseline {
            Some((t, i, c)) => (*t, *i, Some(c), String::new()),
            None => (
                self.started,
                0,
                None,
                format!(
                    "\n    ⚠ the halfway mark at {:?} was never observed by this loop, so the \
                     window below is the whole budget and signal C has no baseline to difference",
                    self.budget / 2
                ),
            ),
        };
        let wall = (now - from).as_secs_f64();
        let iters = self.iters - iters_at;
        let nominal = wall / self.poll.as_secs_f64();
        let self_ratio = if nominal > 0.0 { iters as f64 / nominal } else { 0.0 };
        let starved_self = self_ratio < STARVED_SELF;

        let cpu_now = cpu0.map(|_| sample_child_cpu(pids));
        // `c_spoke` exists because the FAILED verdict line used to read "both signals say this
        // process and its nodes got the machine" unconditionally. That sentence is FALSE whenever C
        // is Unavailable — B alone had spoken, and child starvation had not been excluded at all.
        // A verdict that claims more coverage than it has is the precise failure a guard is
        // supposed to prevent, so the wording is now derived from whether C actually answered.
        let mut c_spoke = false;
        let (starved_children, c_line, c_broken) = match (cpu0, &cpu_now) {
            (Some(ChildCpu::Sampled(a)), Some(ChildCpu::Sampled(b))) => {
                let delta = (b - a).max(0.0);
                let n = pids.len().max(1) as f64;
                let rate = delta / (n * wall);
                c_spoke = true;
                (
                    rate < STARVED_CHILD,
                    format!(
                        "    child CPU     : {delta:.2} cpu-s over {wall:.2}s across {} live \
                         node(s) = {rate:.5} per node-wall-s  (starved below {STARVED_CHILD})  {}",
                        pids.len(),
                        if rate < STARVED_CHILD { "STARVED" } else { "HEALTHY" }
                    ),
                    None,
                )
            }
            (Some(ChildCpu::Broken(why)), _) => (false, String::new(), Some(why.clone())),
            (_, Some(ChildCpu::Broken(why))) => (false, String::new(), Some(why.clone())),
            (Some(ChildCpu::Unavailable(why)), _) => {
                (false, format!("    child CPU     : UNAVAILABLE — {why}"), None)
            }
            (_, Some(ChildCpu::Unavailable(why))) => {
                (false, format!("    child CPU     : UNAVAILABLE — {why}"), None)
            }
            (None, _) | (_, None) => (
                false,
                "    child CPU     : UNAVAILABLE — no mid-window baseline was taken".to_string(),
                None,
            ),
        };

        let kind = if c_broken.is_some() {
            Kind::ClassifierBroken
        } else if starved_self || starved_children {
            Kind::Inconclusive
        } else {
            Kind::Failed
        };

        let verdict_line = match kind {
            Kind::Failed if c_spoke => {
                "    verdict       : FAILED — both signals say this process and its nodes got the \
                 machine,\n                    so the expiry belongs to the cluster, not the \
                 scheduler."
                    .to_string()
            }
            Kind::Failed => {
                "    verdict       : FAILED ON SIGNAL B ALONE — this process got the machine. \
                 Signal C\n                    could not speak (see the line above), so CHILD \
                 starvation was NOT excluded\n                    and this verdict is weaker than \
                 a two-signal FAILED."
                    .to_string()
            }
            Kind::Inconclusive => format!(
                "    verdict       : INCONCLUSIVE — {} starved, so this expiry says nothing \
                 about\n                    the cluster. Re-run it on a quieter machine; do NOT \
                 record this as a failure.",
                match (starved_self, starved_children) {
                    (true, true) => "both this process and its nodes were",
                    (true, false) => "this process was",
                    _ => "the node child processes were",
                }
            ),
            Kind::ClassifierBroken => format!(
                "    verdict       : CLASSIFIER-BROKEN — {}",
                c_broken.as_deref().unwrap_or("unknown")
            ),
        };

        let evidence = format!(
            "  classifier (D42): whether this {:?} expiry is a verdict at all{note}\n\
             \x20   window        : the last {wall:.2}s of the {:?} budget\n\
             \x20   self-schedule : {iters} of {nominal:.0} nominal {:?} polls = {self_ratio:.4}  \
             (starved below {STARVED_SELF})  {}\n\
             \x20   max stall     : {:.0}ms between consecutive polls  (informational; not a \
             verdict input)\n\
             {c_line}\n\
             {verdict_line}",
            self.budget,
            self.budget,
            self.poll,
            if starved_self { "STARVED" } else { "HEALTHY" },
            self.max_stall.as_secs_f64() * 1000.0,
        );
        (kind, evidence)
    }
}

/// Everything every node has said, plus its last words on stderr — the thing a failure must quote
/// instead of reporting an unexplained timeout.
fn transcript(nodes: &[NodeProc], sink: &[(u32, String)]) -> String {
    let lines: Vec<String> = sink.iter().map(|(i, l)| format!("  n{i}: {l}")).collect();
    let errs: Vec<String> = nodes
        .iter()
        .map(|n| format!("  n{} stderr: {}", n.id, n.stderr().replace('\n', " | ")))
        .collect();
    format!("transcript:\n{}\n{}", lines.join("\n"), errs.join("\n"))
}

/// Panic with a non-verdict, for the two kinds that are not the cluster's fault. Returns for
/// `Kind::Failed` so the caller can raise the failure that actually names the property.
fn refuse_unless_failed(
    kind: &Kind,
    evidence: &str,
    nodes: &[NodeProc],
    sink: &[(u32, String)],
    what: &str,
) {
    match kind {
        Kind::Failed => (),
        Kind::Inconclusive => panic!(
            "{VERDICT_INCONCLUSIVE} — waiting for {what} ran out of budget, but this run did not \
             get the machine, so the expiry is not evidence about the cluster.\n{evidence}\n{}",
            transcript(nodes, sink)
        ),
        Kind::ClassifierBroken => panic!(
            "{VERDICT_CLASSIFIER_BROKEN} — waiting for {what} ran out of budget and the load \
             classifier could not measure whether that was starvation. A run it cannot classify \
             is not a pass and not a failure.\n{evidence}\n{}",
            transcript(nodes, sink)
        ),
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
    let mut w = LoadWitness::new(budget, POLL);
    loop {
        pump(nodes, sink);
        if pred(sink) {
            return;
        }
        if w.expired() {
            let pids = live_pids(nodes);
            let (kind, evidence) = w.classify(&pids);
            refuse_unless_failed(&kind, &evidence, nodes, sink, what);
            panic!(
                "timed out after {budget:?} waiting for {what}.\n{}\n{evidence}",
                transcript(nodes, sink)
            );
        }
        let pids = live_pids(nodes);
        w.sleep_and_tick(&pids);
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
    //
    // This is the fifth wait in this file and the only one that hand-rolls its deadline rather
    // than calling `wait_for`. It gets the same D42 classifier, and it NEEDS it most: a starved
    // expiry here leaves the loop with `missing` non-empty and reports acknowledged DATA LOSS —
    // the one property the whole of Phase F exists to buy — when the truth is that the new leader
    // was never scheduled long enough to replay. That is the most expensive false RED this file
    // can produce, and a fix that covered only `wait_for` would have left it in place.
    let mut w = LoadWitness::new(ELECTION_BUDGET, DUMP_POLL);
    let mut classifier_note = String::new();
    let mut missing: Vec<u64>;
    loop {
        nodes[new_ix].tell("DUMP");
        let pids = live_pids(&nodes);
        w.sleep_and_tick(&pids);
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
        if missing.is_empty() {
            break;
        }
        if w.expired() {
            let (kind, evidence) = w.classify(&pids);
            refuse_unless_failed(
                &kind,
                &evidence,
                &nodes,
                &sink,
                "the surviving leader to hold every acknowledged write",
            );
            // FAILED. Fall through to the data-loss assertion below — that is the one that names
            // the property — and carry the evidence with it, so the RED arrives already holding
            // the proof that it was the cluster and not the scheduler.
            classifier_note = format!("\n{evidence}");
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
         This is acknowledged data loss — the single property the whole of Phase F exists to \
         buy.{classifier_note}",
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
