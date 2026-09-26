//! r11-dist part B: failover and orphan disposal on three real `Node`s (loopback TCP, real fsync),
//! against the number of live branches N, for HEAD and three published-fix arms.
//!
//! PREREG: artie-research `frontier/round11/r11-dist/PREREG.md` §5 (079f108). One invocation is one
//! N with all four arms; it runs under the fleet lock. Every number printed is labelled with its
//! instrument: `Instant` wall time (ms), or an integer counter.
//!
//! Per arm: start 3 nodes, each driven by its own thread (as a server's driver would be); elect a
//! stable leader L; grow N forks owned by L (fixture: batched proposals with group commit on, then
//! group commit off for HEAD/ZK/M1); [P7 at N in {10^3, 10^6}, HEAD cluster only, forks owned by a
//! phantom node 9 so L's orphan set is untouched]; stop L; time the election (B1); the new leader L'
//! disposes of L's orphans (B2); a fresh fork + merge on L' to Applied (B3); integers C0 C1 C2 and
//! leader/term changes (B4); restart the other survivor from its directory and time the replay (B5).
//!
//! Proposal paths (PREREG A6): HEAD's sweep calls the seam's `propose` from the caller's thread,
//! exactly as `NodeReplicator` does, sharing the node mutex with the driver. HEADQ, GC, growth and
//! P7 instead hand commands to the node's OWN driver thread through a queue, which it proposes
//! between polls (the single-event-loop shape of etcd's `Propose` channel and hashicorp/raft's
//! `applyCh`): HEADQ one command per driver turn, GC up to 1,024 per turn with group commit on.
//!
//! Usage: r11_dist_fleet <N> <scratch-root> [arm order, e.g. HEAD,HEADQ,GC,ZK,M1]

#[path = "r11_dist/arms.rs"]
mod arms;

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arms::{abandon_c, cid, fork_c, merge_c, sentinel, HeadCopy, Ledger, Zk, V};
use ferrodb::agent_sql::cluster::{BranchApplier, BranchLedger, ClusterAgents, ClusterBranchId, MergeVerdict, Replicated};
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::consensus::config::Config;
use ferrodb::consensus::node::{Applier, Node, NodeOptions};
use ferrodb::consensus::{Command, Entry, NodeId, Round};
use ferrodb::error::FerroError;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

/// Growth batch (fixture): proposals queued per drain with group commit on.
const GROW_BATCH: usize = 1024;
/// HEAD at N = 10^6: the sweep's first K proposals are timed; the whole sweep is extrapolated.
const PREFIX_K: usize = 10_000;
/// Phantom owner of P7's fork stream, so the dead leader's orphan set is untouched by it.
const PHANTOM: u32 = 9;
const P7_SECS: u64 = 20;
const POLL: Duration = Duration::from_millis(1);

// ---------------------------------------------------------------------------------------------
// Appliers
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Obs {
    applied: AtomicU64,
    max_apply_ns: AtomicU64,
    applies: AtomicU64,
}

/// Times every apply and publishes the applied round; wraps each arm's real applier.
struct Instr {
    inner: Box<dyn Applier + Send>,
    obs: Arc<Obs>,
}

impl Applier for Instr {
    fn apply(&mut self, e: &Entry) -> Result<(), FerroError> {
        let t0 = Instant::now();
        let r = self.inner.apply(e);
        let ns = t0.elapsed().as_nanos() as u64;
        self.obs.max_apply_ns.fetch_max(ns, Ordering::Relaxed);
        self.obs.applies.fetch_add(1, Ordering::Relaxed);
        self.obs.applied.store(e.round, Ordering::Release);
        r
    }
}

/// ZK or M1 as an `Applier`: the part-A ledger, with each merge verdict kept.
struct LA<L: Ledger + Send> {
    l: Arc<Mutex<L>>,
    verdicts: Arc<Mutex<BTreeMap<Round, V>>>,
}

impl<L: Ledger + Send> Applier for LA<L> {
    fn apply(&mut self, e: &Entry) -> Result<(), FerroError> {
        if let Some(v) = lock(&self.l).apply(e) {
            lock(&self.verdicts).insert(e.round, v);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    Head,
    HeadQ,
    Gc,
    Zk,
    M1,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Head => "HEAD",
            Arm::HeadQ => "HEADQ",
            Arm::Gc => "GC",
            Arm::Zk => "ZK",
            Arm::M1 => "M1",
        }
    }
}

/// One node's ledger, whichever arm.
#[derive(Clone)]
enum ArmL {
    Head(Arc<Mutex<BranchLedger>>),
    Zk(Arc<Mutex<Zk>>, Arc<Mutex<BTreeMap<Round, V>>>),
    M1(Arc<Mutex<HeadCopy>>, Arc<Mutex<BTreeMap<Round, V>>>),
}

impl ArmL {
    fn new(arm: Arm) -> ArmL {
        match arm {
            Arm::Head | Arm::HeadQ | Arm::Gc => ArmL::Head(Arc::new(Mutex::new(BranchLedger::new()))),
            Arm::Zk => ArmL::Zk(Arc::new(Mutex::new(Zk::new())), Arc::default()),
            Arm::M1 => ArmL::M1(Arc::new(Mutex::new(HeadCopy::new(true))), Arc::default()),
        }
    }
    fn applier(&self, obs: Arc<Obs>) -> Instr {
        let inner: Box<dyn Applier + Send> = match self {
            ArmL::Head(l) => Box::new(BranchApplier::new(l.clone())),
            ArmL::Zk(l, v) => Box::new(LA { l: l.clone(), verdicts: v.clone() }),
            ArmL::M1(l, v) => Box::new(LA { l: l.clone(), verdicts: v.clone() }),
        };
        Instr { inner, obs }
    }
    fn is_live(&self, id: u64) -> bool {
        match self {
            ArmL::Head(l) => lock(l).get(ClusterBranchId(id)).map(|b| b.state.is_live()).unwrap_or(false),
            ArmL::Zk(l, _) => lock(l).is_live(id),
            ArmL::M1(l, _) => lock(l).is_live(id),
        }
    }
    fn live_count_of(&self, node: u32) -> usize {
        match self {
            ArmL::Head(l) => lock(l).live_owned_by(NodeId(node)).len(),
            ArmL::Zk(l, _) => lock(l).live_ids_of(node).len(),
            ArmL::M1(l, _) => lock(l).live_ids_of(node).len(),
        }
    }
    fn verdict(&self, round: Round) -> Option<V> {
        match self {
            ArmL::Head(l) => lock(l).verdict_at(round).map(|v| match v {
                MergeVerdict::Applied { .. } => V::Applied,
                MergeVerdict::ReEvaluate { .. } => V::ReEval,
                MergeVerdict::Refused { .. } => V::Refused,
            }),
            ArmL::Zk(_, v) | ArmL::M1(_, v) => lock(v).get(&round).copied(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The seam: NodeReplicator's `propose` logic (cluster.rs :746-767 at 7dc428f), over `Node<Instr>`
// ---------------------------------------------------------------------------------------------

struct Rep {
    id: NodeId,
    node: Mutex<Option<Node<Instr>>>,
    /// Commands handed to this node's driver thread, proposed between its polls.
    queue: Mutex<std::collections::VecDeque<Command>>,
    /// Commands the driver takes per turn (HEADQ: 1; GC and growth: GROW_BATCH).
    batch: std::sync::atomic::AtomicUsize,
    /// Last round the driver's queued proposals reached, and how many it saw refused.
    q_last_round: AtomicU64,
    q_refused: AtomicU64,
    /// Instrument: the longest interval between two driver turns (us), and turns taken.
    max_gap_us: AtomicU64,
    turns: AtomicU64,
}

impl Rep {
    fn new(id: NodeId, node: Node<Instr>) -> Rep {
        Rep {
            id,
            node: Mutex::new(Some(node)),
            queue: Mutex::new(std::collections::VecDeque::new()),
            batch: std::sync::atomic::AtomicUsize::new(GROW_BATCH),
            q_last_round: AtomicU64::new(0),
            q_refused: AtomicU64::new(0),
            max_gap_us: AtomicU64::new(0),
            turns: AtomicU64::new(0),
        }
    }
    fn enqueue(&self, cmds: impl IntoIterator<Item = Command>) {
        lock(&self.queue).extend(cmds);
    }
    fn queue_len(&self) -> usize {
        lock(&self.queue).len()
    }
    /// One driver turn's proposals: take up to `batch` queued commands and propose them in one drain.
    fn drive_queue(&self) {
        let k = self.batch.load(Ordering::Relaxed).max(1);
        let cmds: Vec<Command> = {
            let mut q = lock(&self.queue);
            let k = k.min(q.len());
            q.drain(..k).collect()
        };
        if cmds.is_empty() {
            return;
        }
        let n = cmds.len() as u64;
        let r = self.with(|node| {
            let _ = node.take_refusals();
            let r = node.propose_many(cmds);
            (r, node.take_refusals().len() as u64)
        });
        match r {
            Some((Ok(round), 0)) => self.q_last_round.store(round, Ordering::Release),
            Some((Ok(_), refused)) => {
                self.q_refused.fetch_add(refused.min(n), Ordering::Relaxed);
            }
            _ => {
                self.q_refused.fetch_add(n, Ordering::Relaxed);
            }
        }
    }
}

impl Rep {
    fn with<R>(&self, f: impl FnOnce(&mut Node<Instr>) -> R) -> Option<R> {
        lock(&self.node).as_mut().map(f)
    }
}

impl Replicated for Rep {
    fn propose(&self, c: Command) -> Result<Round, FerroError> {
        let mut g = lock(&self.node);
        let n = g.as_mut().ok_or_else(|| FerroError::Internal("node stopped".into()))?;
        let _ = n.take_refusals();
        let before = n.last_round();
        let round = n.propose(c)?;
        if let Some(why) = n.take_refusals().into_iter().next() {
            return Err(why);
        }
        if round <= before {
            return Err(FerroError::Internal("no round assigned and no refusal".into()));
        }
        Ok(round)
    }
    fn committed_head(&self) -> Round {
        self.with(|n| n.commit_round()).unwrap_or(0)
    }
    fn pump(&self) -> Result<(), FerroError> {
        match lock(&self.node).as_mut() {
            Some(n) => n.poll(POLL),
            None => Ok(()),
        }
    }
    fn leader(&self) -> Option<NodeId> {
        self.with(|n| n.leader()).flatten()
    }
}

// ---------------------------------------------------------------------------------------------
// The cluster
// ---------------------------------------------------------------------------------------------

struct Cluster {
    arm: Arm,
    reps: Vec<Arc<Rep>>,
    ledgers: Vec<ArmL>,
    obs: Vec<Arc<Obs>>,
    stops: Vec<Arc<AtomicBool>>,
    threads: Vec<Option<JoinHandle<u64>>>,
    addrs: BTreeMap<NodeId, SocketAddr>,
    dirs: Vec<PathBuf>,
    cfg: Config,
    errs: Arc<Mutex<Vec<String>>>,
}

/// A10: a per-run seed salt. 0 (the default) gives every earlier run's seeds exactly.
static SEED_SALT: AtomicU64 = AtomicU64::new(0);

fn opts(dir: &Path, addrs: &BTreeMap<NodeId, SocketAddr>, id: NodeId, i: usize) -> NodeOptions {
    let peers: BTreeMap<NodeId, SocketAddr> = addrs.iter().filter(|(k, _)| **k != id).map(|(k, v)| (*k, *v)).collect();
    // NodeOptions::new's own default tick (50 ms) is kept: the production timing.
    let salt = SEED_SALT.load(Ordering::Relaxed).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    let mut o = NodeOptions::new(dir, peers, 0x5eed ^ (i as u64 + 1).wrapping_mul(0x9E37_79B9) ^ salt);
    // A12 control: the transport's idle close (default 60 s), overridable per run.
    if let Some(s) = std::env::var("R11_IDLE_DEADLINE_S").ok().and_then(|x| x.parse::<u64>().ok()) {
        o.transport.idle_deadline = Duration::from_secs(s);
    }
    o
}

fn spawn_driver(rep: Arc<Rep>, stop: Arc<AtomicBool>, errs: Arc<Mutex<Vec<String>>>) -> JoinHandle<u64> {
    std::thread::spawn(move || {
        let mut polls = 0u64;
        let mut last = Instant::now();
        while !stop.load(Ordering::Relaxed) {
            let now = Instant::now();
            rep.max_gap_us.fetch_max(now.duration_since(last).as_micros() as u64, Ordering::Relaxed);
            last = now;
            rep.drive_queue();
            if let Err(e) = rep.pump() {
                lock(&errs).push(format!("{} driver: {e}", rep.id));
                break;
            }
            polls += 1;
            rep.turns.fetch_add(1, Ordering::Relaxed);
            std::thread::yield_now();
        }
        polls
    })
}

impl Cluster {
    fn start(arm: Arm, root: &Path) -> Cluster {
        let listeners: Vec<TcpListener> = (0..3).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
        let addrs: BTreeMap<NodeId, SocketAddr> =
            listeners.iter().enumerate().map(|(i, l)| (NodeId(i as u32 + 1), l.local_addr().unwrap())).collect();
        let cfg = Config::new((1..=3).map(NodeId), 1, 0);
        let errs = Arc::new(Mutex::new(Vec::new()));
        let mut c = Cluster {
            arm,
            reps: Vec::new(),
            ledgers: Vec::new(),
            obs: Vec::new(),
            stops: Vec::new(),
            threads: Vec::new(),
            addrs: addrs.clone(),
            dirs: Vec::new(),
            cfg: cfg.clone(),
            errs: errs.clone(),
        };
        for (i, l) in listeners.into_iter().enumerate() {
            let id = NodeId(i as u32 + 1);
            let dir = root.join(format!("n{}", i + 1));
            let ledger = ArmL::new(arm);
            let obs = Arc::new(Obs::default());
            let node = Node::start(id, cfg.clone(), l, opts(&dir, &addrs, id, i), ledger.applier(obs.clone())).unwrap();
            let rep = Arc::new(Rep::new(id, node));
            let stop = Arc::new(AtomicBool::new(false));
            c.threads.push(Some(spawn_driver(rep.clone(), stop.clone(), errs.clone())));
            c.reps.push(rep);
            c.ledgers.push(ledger);
            c.obs.push(obs);
            c.stops.push(stop);
            c.dirs.push(dir);
        }
        c
    }

    fn set_gc(&self, on: bool) {
        for r in &self.reps {
            r.with(|n| n.set_group_commit(on));
        }
    }

    /// One node claims office, every live node agrees, and its own round is committed; held 50 ms.
    fn elect(&self, alive: &[usize], bound: Duration) -> Option<(usize, Round)> {
        let t0 = Instant::now();
        let mut who: Option<usize> = None;
        let mut since = Instant::now();
        while t0.elapsed() < bound {
            let claiming: Vec<usize> = alive.iter().copied().filter(|i| self.reps[*i].leader() == Some(NodeId(*i as u32 + 1))).collect();
            let settled = claiming.len() == 1 && {
                let l = claiming[0];
                let last = self.reps[l].with(|n| n.last_round()).unwrap_or(0);
                alive.iter().all(|i| self.reps[*i].leader() == Some(NodeId(l as u32 + 1))) && self.reps[l].committed_head() >= last
            };
            if settled {
                if who != Some(claiming[0]) {
                    who = Some(claiming[0]);
                    since = Instant::now();
                } else if since.elapsed() >= Duration::from_millis(50) {
                    let l = claiming[0];
                    return Some((l, self.reps[l].committed_head()));
                }
            } else {
                who = None;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
        None
    }

    fn wait_applied(&self, nodes: &[usize], round: Round, bound: Duration) -> bool {
        let t0 = Instant::now();
        while t0.elapsed() < bound {
            if nodes.iter().all(|i| self.obs[*i].applied.load(Ordering::Acquire) >= round) {
                return true;
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        false
    }

    /// Stop a node's driver, shut its transport and drop it (so its files and port are released).
    fn stop_node(&mut self, i: usize) -> u64 {
        self.stops[i].store(true, Ordering::Relaxed);
        let polls = self.threads[i].take().map(|t| t.join().unwrap_or(0)).unwrap_or(0);
        if let Some(n) = lock(&self.reps[i].node).take() {
            n.shutdown();
            drop(n);
        }
        polls
    }

    /// `stop_node`, timed in its two halves: joining the driver, then shutting and dropping the node.
    fn stop_node_timed(&mut self, i: usize) -> (f64, f64) {
        let t0 = Instant::now();
        self.stops[i].store(true, Ordering::Relaxed);
        let _ = self.threads[i].take().map(|t| t.join().unwrap_or(0));
        let join_ms = ms(t0);
        let t1 = Instant::now();
        if let Some(n) = lock(&self.reps[i].node).take() {
            n.shutdown();
            drop(n);
        }
        (join_ms, ms(t1))
    }

    /// B1 decomposed (PREREG A9): after `i` is stopped, when does a survivor first claim office,
    /// and when has that claimant committed the round it held at its claim (its NoOp)?
    fn election_phases(&self, survivors: &[usize], bound: Duration) -> (Option<f64>, Option<f64>) {
        let t0 = Instant::now();
        let mut claim: Option<(usize, Round, f64)> = None;
        while t0.elapsed() < bound {
            if claim.is_none() {
                if let Some(x) = survivors.iter().copied().find(|x| self.reps[*x].leader() == Some(NodeId(*x as u32 + 1))) {
                    let last = self.reps[x].with(|n| n.last_round()).unwrap_or(0);
                    claim = Some((x, last, ms(t0)));
                }
            }
            if let Some((x, last, c_ms)) = claim {
                if self.reps[x].committed_head() >= last {
                    return (Some(c_ms), Some(ms(t0)));
                }
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        (claim.map(|c| c.2), None)
    }

    /// B5: start node `i` again on its own directory and address, with a fresh ledger.
    fn restart(&mut self, i: usize) -> Result<(), String> {
        let id = NodeId(i as u32 + 1);
        let l = TcpListener::bind(self.addrs[&id]).map_err(|e| format!("rebind {}: {e}", self.addrs[&id]))?;
        let ledger = ArmL::new(self.arm);
        let obs = Arc::new(Obs::default());
        let node = Node::start(id, self.cfg.clone(), l, opts(&self.dirs[i], &self.addrs, id, i), ledger.applier(obs.clone()))
            .map_err(|e| format!("Node::start: {e}"))?;
        *lock(&self.reps[i].node) = Some(node);
        self.ledgers[i] = ledger;
        self.obs[i] = obs;
        let stop = Arc::new(AtomicBool::new(false));
        self.threads[i] = Some(spawn_driver(self.reps[i].clone(), stop.clone(), self.errs.clone()));
        self.stops[i] = stop;
        Ok(())
    }

    fn shutdown_all(&mut self) {
        for i in 0..3 {
            self.stop_node(i);
        }
    }
}

/// Hand `cmds` to node `l`'s driver, `batch` per turn; wait until the queue is drained. Returns
/// the last round its proposals reached, or why it could not.
fn propose_queued(c: &Cluster, l: usize, cmds: Vec<Command>, batch: usize, bound: Duration) -> Result<Round, String> {
    let r = &c.reps[l];
    r.batch.store(batch, Ordering::Relaxed);
    let refused0 = r.q_refused.load(Ordering::Relaxed);
    r.enqueue(cmds);
    let t0 = Instant::now();
    while r.queue_len() > 0 {
        if t0.elapsed() > bound {
            return Err(format!("queue not drained in {bound:?}: {} left", r.queue_len()));
        }
        std::thread::sleep(Duration::from_micros(200));
    }
    // The last batch may still be inside its drain; wait for the driver to finish the turn.
    let turns = r.turns.load(Ordering::Relaxed);
    while r.turns.load(Ordering::Relaxed) < turns + 2 && t0.elapsed() < bound {
        std::thread::sleep(Duration::from_micros(200));
    }
    let refused = r.q_refused.load(Ordering::Relaxed) - refused0;
    if refused > 0 {
        return Err(format!("{refused} queued proposals refused"));
    }
    Ok(r.q_last_round.load(Ordering::Acquire))
}

// ---------------------------------------------------------------------------------------------
// B0: fsync on this volume
// ---------------------------------------------------------------------------------------------

fn b0(root: &Path) -> (f64, f64) {
    let p = root.join("b0.sync");
    let mut f = std::fs::OpenOptions::new().create(true).truncate(true).write(true).open(&p).unwrap();
    let mut v = Vec::new();
    for _ in 0..2000 {
        f.write_all(&[7u8; 64]).unwrap();
        let t0 = Instant::now();
        f.sync_data().unwrap();
        v.push(t0.elapsed().as_secs_f64() * 1e3);
    }
    let _ = std::fs::remove_file(&p);
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (v[v.len() / 2], v[v.len() * 99 / 100])
}

// ---------------------------------------------------------------------------------------------
// One arm at one N
// ---------------------------------------------------------------------------------------------

fn run_arm(arm: Arm, n: u64, root: &Path) {
    let tag = format!("arm={} N={n}", arm.name());
    let dir = root.join(format!("{}_{n}", arm.name()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut c = Cluster::start(arm, &dir);
    let all = [0usize, 1, 2];
    let Some((l, _)) = c.elect(&all, Duration::from_secs(60)) else {
        println!("B {tag} VOID: no stable leader in 60 s; errs={:?}", lock(&c.errs));
        c.shutdown_all();
        let _ = std::fs::remove_dir_all(&dir);
        return;
    };
    let lid = l as u32 + 1;

    // GROW (fixture): N forks owned by L, group-committed.
    c.set_gc(true);
    let t0 = Instant::now();
    let grow_last = match propose_queued(&c, l, (1..=n).map(|i| fork_c(cid(lid, i))).collect(), GROW_BATCH, Duration::from_secs(900)) {
        Ok(r) => r,
        Err(e) => {
            println!("B {tag} VOID: growth refused: {e}");
            c.shutdown_all();
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
    };
    let grown = c.wait_applied(&all, grow_last, Duration::from_secs(900));
    let grow_ms = ms(t0);
    if arm != Arm::Gc {
        c.set_gc(false);
    }
    let live_before: Vec<usize> = c.ledgers.iter().map(|x| x.live_count_of(lid)).collect();
    println!("B {tag} grow_ms={grow_ms:.0} grown={grown} grow_last_round={grow_last} live_of_L_per_node={live_before:?} (fixture phase, group-committed)");
    if !grown || live_before.iter().any(|x| *x as u64 != n) {
        println!("B {tag} VOID: growth did not reach every node");
        c.shutdown_all();
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }

    // P7 (HEAD cluster only, N in {10^3, 10^6}): a single-proposal fork stream, owner PHANTOM.
    if arm == Arm::Head && (n == 1_000 || n == 1_000_000) {
        let f1 = (l + 1) % 3;
        for r in &c.reps {
            r.max_gap_us.store(0, Ordering::Relaxed);
        }
        let term_p7 = c.reps[l].with(|x| x.term()).unwrap_or(0);
        let t0 = Instant::now();
        let start_commit = c.reps[l].committed_head();
        let mut lags: Vec<u64> = Vec::new();
        let mut i = 0u64;
        let refused0 = c.reps[l].q_refused.load(Ordering::Relaxed);
        c.reps[l].batch.store(1, Ordering::Relaxed);
        // Keep the driver's queue non-empty (saturation) without ever holding the node mutex here.
        while t0.elapsed() < Duration::from_secs(P7_SECS) {
            if c.reps[l].queue_len() < 64 {
                c.reps[l].enqueue((0..64).map(|k| fork_c(cid(PHANTOM, i + k + 1))));
                i += 64;
            }
            let head = c.reps[l].committed_head();
            let fa = c.obs[f1].applied.load(Ordering::Acquire);
            lags.push(head.saturating_sub(fa));
            std::thread::sleep(Duration::from_millis(5));
        }
        let committed = c.reps[l].committed_head() - start_commit;
        let left = { let mut q = lock(&c.reps[l].queue); let k = q.len() as u64; q.clear(); k };
        i -= left;
        let refused = c.reps[l].q_refused.load(Ordering::Relaxed) - refused0;
        c.reps[l].batch.store(GROW_BATCH, Ordering::Relaxed);
        lags.sort_unstable();
        let p99 = lags.get(lags.len() * 99 / 100).copied().unwrap_or(0);
        println!(
            "B {tag} P7 path=driver-queue,1-per-turn,GC-off proposed={i} refused={refused} committed_in_{P7_SECS}s={committed} R_per_s={:.1} follower_apply_lag_p99_rounds={p99} lag_samples={}",
            committed as f64 / P7_SECS as f64,
            lags.len()
        );
        std::thread::sleep(Duration::from_millis(50));
        let last = c.reps[l].with(|x| x.last_round()).unwrap_or(0);
        c.wait_applied(&all, last, Duration::from_secs(120));
        let gaps: Vec<f64> = c.reps.iter().map(|r| r.max_gap_us.load(Ordering::Relaxed) as f64 / 1e3).collect();
        let leaders: Vec<Option<NodeId>> = c.reps.iter().map(|r| r.leader()).collect();
        let terms: Vec<u64> = c.reps.iter().map(|r| r.with(|x| x.term()).unwrap_or(0)).collect();
        let moved = c.reps[l].leader() != Some(NodeId(lid));
        println!(
            "B {tag} P7_after term_at_start={term_p7} terms={terms:?} leaders={leaders:?} driver_max_gap_ms_per_node={gaps:?} leadership_moved={moved}{}",
            if moved { " => B1 for this arm is VOID (the node stopped below is no longer the leader)" } else { "" }
        );
    }

    // FAILOVER: stop L; time the election among the survivors (B1).
    let survivors: Vec<usize> = all.iter().copied().filter(|i| *i != l).collect();
    let syncs_before: Vec<u64> = survivors.iter().map(|i| c.reps[*i].with(|x| x.persist_syncs()).unwrap_or(0)).collect();
    for r in &c.reps {
        r.max_gap_us.store(0, Ordering::Relaxed);
    }
    let t_fail = Instant::now();
    let (join_ms, drop_ms) = c.stop_node_timed(l);
    let t_after_stop = Instant::now();
    let (claim_ms, noop_ms) = c.election_phases(&survivors, Duration::from_secs(120));
    let Some((l2, _)) = c.elect(&survivors, Duration::from_secs(120)) else {
        println!("B {tag} VOID: no leader among survivors in 120 s; errs={:?}", lock(&c.errs));
        c.shutdown_all();
        let _ = std::fs::remove_dir_all(&dir);
        return;
    };
    let t_elect_ms = ms(t_fail);
    let elect_after_stop_ms = ms(t_after_stop);
    let egaps: Vec<f64> = survivors.iter().map(|i| c.reps[*i].max_gap_us.load(Ordering::Relaxed) as f64 / 1e3).collect();
    println!(
        "B {tag} B1_parts stop_join_ms={join_ms:.1} stop_drop_ms={drop_ms:.1} claim_ms_after_stop={claim_ms:?} noop_committed_ms_after_stop={noop_ms:?} elect_ms_after_stop={elect_after_stop_ms:.1} survivor_driver_max_gap_ms={egaps:?}"
    );
    let l2id = l2 as u32 + 1;
    let other = *survivors.iter().find(|i| **i != l2).unwrap();
    let term0 = c.reps[l2].with(|x| x.term()).unwrap_or(0);
    let leader_before_serve = l2id;

    // DISPOSE (B2).
    for i in [l2, other] {
        c.reps[i].max_gap_us.store(0, Ordering::Relaxed);
    }
    let t_disp = Instant::now();
    let (props, last_round, prefix) = match arm {
        Arm::Head => {
            // The real sweep, on the seam NodeReplicator's shape gives it: proposals from this
            // (the caller's) thread, sharing the node mutex with the driver.
            let ledger = match &c.ledgers[l2] {
                ArmL::Head(x) => x.clone(),
                _ => unreachable!(),
            };
            let agents = ClusterAgents::new(NodeId(l2id), Arc::new(AgentRuntime::new()), c.reps[l2].clone(), ledger);
            match agents.abandon_orphans_of(NodeId(lid)) {
                Ok(out) => (out.len() as u64, out.last().map(|x| x.1).unwrap_or(0), false),
                Err(e) => {
                    println!("B {tag} abandon_orphans_of REFUSED after {} proposals: {e}", agents.cost().proposals);
                    (agents.cost().proposals, 0, false)
                }
            }
        }
        Arm::HeadQ | Arm::Gc => {
            // HEAD's per-branch sweep (the real orphans_of, one Abandon per orphan), proposed by the
            // driver: HEADQ one per turn with one fsync each; GC up to GROW_BATCH per turn, one fsync.
            let orphans = match &c.ledgers[l2] {
                ArmL::Head(x) => lock(x).orphans_of(NodeId(lid)),
                _ => unreachable!(),
            };
            let (batch, take) = if arm == Arm::HeadQ { (1, if n > 100_000 { PREFIX_K } else { orphans.len() }) } else { (GROW_BATCH, orphans.len()) };
            let cmds: Vec<Command> = orphans.into_iter().take(take).map(|id| abandon_c(id.0)).collect();
            let k = cmds.len() as u64;
            match propose_queued(&c, l2, cmds, batch, Duration::from_secs(1100)) {
                Ok(r) => (k, r, take < n as usize),
                Err(e) => {
                    println!("B {tag} queued sweep refused: {e}");
                    (k, 0, take < n as usize)
                }
            }
        }
        Arm::Zk | Arm::M1 => match c.reps[l2].propose(abandon_c(sentinel(lid))) {
            Ok(r) => (1, r, false),
            Err(e) => {
                println!("B {tag} fence refused: {e}");
                (0, 0, false)
            }
        },
    };
    let propose_ms = ms(t_disp);
    let applied = last_round > 0 && c.wait_applied(&[l2, other], last_round, Duration::from_secs(1200));
    let dispose_ms = ms(t_disp);
    let term1 = c.reps[l2].with(|x| x.term()).unwrap_or(0);
    let leader_after = c.reps[other].leader();
    let max_apply_ns: Vec<u64> = [l2, other].iter().map(|i| c.obs[*i].max_apply_ns.load(Ordering::Relaxed)).collect();
    let max_gap_ms: Vec<f64> = [l2, other].iter().map(|i| c.reps[*i].max_gap_us.load(Ordering::Relaxed) as f64 / 1e3).collect();
    let syncs_after: Vec<u64> = survivors.iter().map(|i| c.reps[*i].with(|x| x.persist_syncs()).unwrap_or(0)).collect();
    let sync_delta: Vec<u64> = syncs_after.iter().zip(&syncs_before).map(|(a, b)| a - b).collect();
    println!(
        "B {tag} B1_t_elect_ms={t_elect_ms:.1} B2_dispose_ms={dispose_ms:.1} propose_ms={propose_ms:.1} C0_props={props} prefix={prefix} per_prop_ms={:.3} applied_all={applied} B4_term_before={term0} term_after={term1} leader_seen_by_other={leader_after:?} max_apply_ns_survivors={max_apply_ns:?} driver_max_gap_ms_survivors={max_gap_ms:?} persist_syncs_since_failover_survivors={sync_delta:?}",
        if props > 0 { dispose_ms / props as f64 } else { f64::NAN }
    );

    // SERVE (B3): a fresh fork and merge on whoever leads now (L' unless the sweep deposed it).
    let t_serve = Instant::now();
    let l2 = match c.elect(&survivors, Duration::from_secs(60)) {
        Some((x, _)) => x,
        None => l2,
    };
    let other = *survivors.iter().find(|i| **i != l2).unwrap();
    let l2id = l2 as u32 + 1;
    println!("B {tag} B3_leader_after_dispose=n{l2id} (changed={})", l2id != leader_before_serve);
    let fresh = cid(l2id, 1);
    let serve = (|| -> Result<(Round, Option<V>), String> {
        let fr = c.reps[l2].propose(fork_c(fresh)).map_err(|e| e.to_string())?;
        if !c.wait_applied(&[l2], fr, Duration::from_secs(60)) {
            return Err("fork not applied".into());
        }
        let base = c.reps[l2].committed_head();
        let mr = c.reps[l2].propose(merge_c(fresh, base)).map_err(|e| e.to_string())?;
        if !c.wait_applied(&[l2, other], mr, Duration::from_secs(60)) {
            return Err("merge not applied".into());
        }
        Ok((mr, c.ledgers[l2].verdict(mr)))
    })();
    let serve_ms = ms(t_serve);
    let b3 = t_elect_ms + dispose_ms + serve_ms;
    match &serve {
        Ok((mr, v)) => println!("B {tag} B3_serve_tail_ms={serve_ms:.1} B3_t_serve_ms={b3:.1} merge_round={mr} verdict={v:?}"),
        Err(e) => println!("B {tag} B3 FAILED: {e}"),
    }

    // Integers C1 (L's branches still live) and C2 (fresh fork wrongly not live before its merge:
    // checked through the merge verdict above; here the survivors' liveness of L's branches).
    let c1: Vec<usize> = [l2, other].iter().map(|i| c.ledgers[*i].live_count_of(lid)).collect();
    println!("B {tag} C1_L_live_on_survivors={c1:?} (HEADQ prefix run at N>1e5: expected N-K) errs={:?}", lock(&c.errs));

    // B5: restart the non-leader survivor and time the replay to its last log round.
    let last_log = c.reps[other].with(|x| x.last_round()).unwrap_or(0);
    c.stop_node(other);
    let t_rs = Instant::now();
    match c.restart(other) {
        Ok(()) => {
            let start_ms = ms(t_rs);
            let ok = c.wait_applied(&[other], last_log, Duration::from_secs(600));
            println!(
                "B {tag} B5_restart_ms={:.1} node_start_ms={start_ms:.1} replayed_to={} last_log_round={last_log} ok={ok} applies={}",
                ms(t_rs),
                c.obs[other].applied.load(Ordering::Acquire),
                c.obs[other].applies.load(Ordering::Relaxed)
            );
            let c1r = c.ledgers[other].live_count_of(lid);
            println!("B {tag} C1_after_restart={c1r}");
        }
        Err(e) => println!("B {tag} B5 SKIPPED: {e}"),
    }
    // A9: repeated failovers at this N, without re-growing: restart every stopped node, let it
    // catch up, then stop whoever leads and time the phases. R11_B1_REPS (default 0).
    let reps: usize = std::env::var("R11_B1_REPS").ok().and_then(|x| x.parse().ok()).unwrap_or(0);
    for rep in 0..reps {
        for i in 0..3 {
            if lock(&c.reps[i].node).is_none() {
                if let Err(e) = c.restart(i) {
                    println!("B {tag} B1R rep={rep} VOID: restart n{}: {e}", i + 1);
                }
            }
        }
        let Some((x, _)) = c.elect(&all, Duration::from_secs(120)) else {
            println!("B {tag} B1R rep={rep} VOID: no stable leader");
            break;
        };
        let last = c.reps[x].with(|n| n.last_round()).unwrap_or(0);
        if !c.wait_applied(&all, last, Duration::from_secs(600)) {
            println!("B {tag} B1R rep={rep} VOID: a restarted node did not catch up to {last}");
            break;
        }
        let rest: Vec<usize> = all.iter().copied().filter(|i| *i != x).collect();
        for r in &c.reps {
            r.max_gap_us.store(0, Ordering::Relaxed);
        }
        let t0 = Instant::now();
        let (j, d) = c.stop_node_timed(x);
        let ta = Instant::now();
        let (cl, nc) = c.election_phases(&rest, Duration::from_secs(120));
        let ok = c.elect(&rest, Duration::from_secs(120)).is_some();
        let gaps: Vec<f64> = rest.iter().map(|i| c.reps[*i].max_gap_us.load(Ordering::Relaxed) as f64 / 1e3).collect();
        println!(
            "B {tag} B1R rep={rep} stopped=n{} last_round={last} stop_join_ms={j:.1} stop_drop_ms={d:.1} claim_ms_after_stop={cl:?} noop_committed_ms_after_stop={nc:?} elect_ms_after_stop={:.1} B1_total_ms={:.1} elected={ok} survivor_driver_max_gap_ms={gaps:?}",
            x + 1,
            ms(ta),
            ms(t0)
        );
    }
    c.shutdown_all();
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------------------------
// A10: first failovers after growth, one per seed, M1 arm. Records each node's role transitions
// during growth and during the election, the election timers, the stop/claim/noop split and the
// survivors' driver gaps.
// ---------------------------------------------------------------------------------------------

fn role_counts(t: &[(ferrodb::consensus::Role, u64, Option<NodeId>)]) -> String {
    use ferrodb::consensus::Role;
    let c = |r: Role| t.iter().filter(|x| x.0 == r).count();
    format!("F{}/P{}/C{}/L{}", c(Role::Follower), c(Role::PreCandidate), c(Role::Candidate), c(Role::Leader))
}

fn take_all(c: &Cluster) -> Vec<Vec<(ferrodb::consensus::Role, u64, Option<NodeId>)>> {
    c.reps.iter().map(|r| r.with(|n| n.take_transitions()).unwrap_or_default()).collect()
}

fn run_firstfail(n: u64, seed: u64, root: &Path) {
    SEED_SALT.store(seed, Ordering::Relaxed);
    let tag = format!("N={n} seed={seed}");
    let dir = root.join(format!("ff_{n}_{seed}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut c = Cluster::start(Arm::M1, &dir);
    let all = [0usize, 1, 2];
    let Some((l, _)) = c.elect(&all, Duration::from_secs(60)) else {
        println!("FF {tag} VOID: no stable leader");
        c.shutdown_all();
        let _ = std::fs::remove_dir_all(&dir);
        return;
    };
    let lid = l as u32 + 1;
    let init = take_all(&c);
    let timers_init: Vec<(u32, u32)> = c.reps.iter().map(|r| r.with(|n| n.election_timer()).unwrap_or((0, 0))).collect();
    c.set_gc(true);
    for r in &c.reps {
        r.max_gap_us.store(0, Ordering::Relaxed);
    }
    let t0 = Instant::now();
    let grow = propose_queued(&c, l, (1..=n).map(|i| fork_c(cid(lid, i))).collect(), GROW_BATCH, Duration::from_secs(600));
    let grown = match &grow {
        Ok(last) => c.wait_applied(&all, *last, Duration::from_secs(600)),
        Err(_) => false,
    };
    let grow_ms = ms(t0);
    c.set_gc(false);
    let growth = take_all(&c);
    let grow_gaps: Vec<f64> = c.reps.iter().map(|r| r.max_gap_us.load(Ordering::Relaxed) as f64 / 1e3).collect();
    let terms: Vec<u64> = c.reps.iter().map(|r| r.with(|x| x.term()).unwrap_or(0)).collect();
    let leaders: Vec<Option<NodeId>> = c.reps.iter().map(|r| r.leader()).collect();
    println!(
        "FF {tag} growth grow_ms={grow_ms:.0} grown={grown} grow_err={:?} leader=n{lid} init_transitions={:?} growth_transitions={:?} growth_driver_max_gap_ms={grow_gaps:?} terms_after_growth={terms:?} leaders_after_growth={leaders:?} timers_after_initial_election={timers_init:?}",
        grow.as_ref().err(),
        init.iter().map(|t| role_counts(t)).collect::<Vec<_>>(),
        growth.iter().map(|t| role_counts(t)).collect::<Vec<_>>()
    );
    if !grown || c.reps[l].leader() != Some(NodeId(lid)) {
        println!("FF {tag} VOID: growth did not complete under the same leader");
        c.shutdown_all();
        drop(c);
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }
    let survivors: Vec<usize> = all.iter().copied().filter(|i| *i != l).collect();
    // A12 control: idle the cluster (no proposals) before the failover.
    if let Some(s) = std::env::var("R11_PRE_FAIL_SLEEP_S").ok().and_then(|x| x.parse::<u64>().ok()) {
        std::thread::sleep(Duration::from_secs(s));
    }
    let tc_before: Vec<Option<(u64, u64, u64, u64, u64, usize, u64, u64)>> = survivors.iter().map(|i| c.reps[*i].with(|n| n.transport_counters())).collect();
    let timers_at_stop: Vec<(u32, u32)> = survivors.iter().map(|i| c.reps[*i].with(|n| n.election_timer()).unwrap_or((0, 0))).collect();
    for r in &c.reps {
        r.max_gap_us.store(0, Ordering::Relaxed);
    }
    let t_fail = Instant::now();
    let (join_ms, drop_ms) = c.stop_node_timed(l);
    // Sample each survivor's timer every ms until one of them leads with its round committed.
    let ta = Instant::now();
    let mut draws: Vec<Vec<u32>> = vec![Vec::new(); survivors.len()];
    let mut claim: Option<(usize, Round, f64)> = None;
    let mut noop: Option<f64> = None;
    while ta.elapsed() < Duration::from_secs(60) {
        for (k, i) in survivors.iter().enumerate() {
            if let Some((to, _)) = c.reps[*i].with(|n| n.election_timer()) {
                if draws[k].last() != Some(&to) {
                    draws[k].push(to);
                }
            }
        }
        if claim.is_none() {
            if let Some(x) = survivors.iter().copied().find(|x| c.reps[*x].leader() == Some(NodeId(*x as u32 + 1))) {
                let last = c.reps[x].with(|n| n.last_round()).unwrap_or(0);
                claim = Some((x, last, ms(ta)));
            }
        }
        if let Some((x, last, _)) = claim {
            if c.reps[x].committed_head() >= last {
                noop = Some(ms(ta));
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let elected = c.elect(&survivors, Duration::from_secs(60));
    let elect_ms = ms(ta);
    let b1 = ms(t_fail);
    let el = take_all(&c);
    let egaps: Vec<f64> = survivors.iter().map(|i| c.reps[*i].max_gap_us.load(Ordering::Relaxed) as f64 / 1e3).collect();
    let term_after: Vec<u64> = survivors.iter().map(|i| c.reps[*i].with(|x| x.term()).unwrap_or(0)).collect();
    let tc_after: Vec<Option<(u64, u64, u64, u64, u64, usize, u64, u64)>> = survivors.iter().map(|i| c.reps[*i].with(|n| n.transport_counters())).collect();
    // (idle_closed, lost_in_flight, connect_failures, dropped, inbound_dropped) deltas over the election.
    let tdelta: Vec<Option<(u64, u64, u64, u64, u64)>> = tc_before
        .iter()
        .zip(&tc_after)
        .map(|(b, a)| match (b, a) {
            (Some(b), Some(a)) => Some((a.6 - b.6, a.4 - b.4, a.7 - b.7, a.1 - b.1, a.3 - b.3)),
            _ => None,
        })
        .collect();
    let idle_closed_total: Vec<Option<u64>> = tc_after.iter().map(|a| a.map(|a| a.6)).collect();
    println!("FF {tag} transport election_delta(idle_closed,lost_in_flight,connect_failures,dropped,inbound_dropped)={tdelta:?} idle_closed_total_at_end={idle_closed_total:?} idle_deadline_s={:?} pre_fail_sleep_s={:?}", std::env::var("R11_IDLE_DEADLINE_S").ok(), std::env::var("R11_PRE_FAIL_SLEEP_S").ok());
    println!(
        "FF {tag} election winner={:?} B1_ms={b1:.1} stop_join_ms={join_ms:.1} stop_drop_ms={drop_ms:.1} claim_ms={:?} noop_ms={:?} elect_ms={elect_ms:.1} survivors={:?} timers_at_stop(timeout,since_heard)={timers_at_stop:?} timeouts_seen_during_election={draws:?} election_transitions={:?} terms_after={term_after:?} survivor_driver_max_gap_ms={egaps:?}",
        elected.map(|(x, _)| format!("n{}", x + 1)),
        claim.map(|c| c.2),
        noop,
        survivors.iter().map(|i| format!("n{}", i + 1)).collect::<Vec<_>>(),
        survivors.iter().map(|i| role_counts(&el[*i])).collect::<Vec<_>>()
    );
    c.shutdown_all();
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A11: one group-committed growth to N (the fixture every part-B arm used), with a sample of
/// every node each second, and a full dump if the leader's commit index stops while its log is ahead.
fn run_growmon(n: u64, seed: u64, root: &Path) {
    SEED_SALT.store(seed, Ordering::Relaxed);
    let dir = root.join(format!("gm_{n}_{seed}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let c = Arc::new(std::sync::Mutex::new(Cluster::start(Arm::Gc, &dir)));
    let all = [0usize, 1, 2];
    let l = {
        let g = lock(&c);
        match g.elect(&all, Duration::from_secs(60)) {
            Some((l, _)) => l,
            None => {
                println!("GM VOID: no stable leader");
                return;
            }
        }
    };
    let lid = l as u32 + 1;
    let reps: Vec<Arc<Rep>> = lock(&c).reps.clone();
    let obs: Vec<Arc<Obs>> = lock(&c).obs.clone();
    for r in &reps {
        r.with(|n| n.set_group_commit(true));
        r.max_gap_us.store(0, Ordering::Relaxed);
        let _ = r.with(|n| n.take_transitions());
    }
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    // The sampler: one line per second; resets each driver's gap so each line is that second's max.
    let sampler = {
        let reps = reps.clone();
        let obs = obs.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut last_commit = 0u64;
            let mut still = 0u32;
            let mut dumped = false;
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(1));
                let mut cols = Vec::new();
                for (i, r) in reps.iter().enumerate() {
                    let gap = r.max_gap_us.swap(0, Ordering::Relaxed) as f64 / 1e3;
                    let v = r.with(|x| {
                        (x.commit_round(), x.last_round(), x.term(), x.role(), x.leader(), x.election_timer(), x.persist_syncs(), x.take_transitions().len())
                    });
                    if let Some((cm, lr, term, role, ld, (to, sh), syncs, tr)) = v {
                        cols.push(format!(
                            "n{}:{role:?}/t{term}/ld{:?} commit={cm} last={lr} applied={} timer={to}/{sh} syncs={syncs} transitions={tr} gap_ms={gap:.0} q={}",
                            i + 1,
                            ld.map(|x| x.0),
                            obs[i].applied.load(Ordering::Acquire),
                            r.queue_len()
                        ));
                    }
                }
                let lc = reps[l].committed_head();
                let ll = reps[l].with(|x| x.last_round()).unwrap_or(0);
                println!("GM t={:.0}s {}", t0.elapsed().as_secs_f64(), cols.join(" | "));
                if lc == last_commit && ll > lc {
                    still += 1;
                } else {
                    still = 0;
                }
                last_commit = lc;
                if still >= 30 && !dumped {
                    dumped = true;
                    println!("GM STALL: leader n{} commit {lc} unchanged for {still} s with last {ll}", l + 1);
                    for (i, r) in reps.iter().enumerate() {
                        let d = r.with(|x| (x.peer_progress(), x.transport_counters()));
                        println!("GM STALL-DUMP n{} progress(peer,next,matched,silent,needs_snapshot,diverged)={:?} transport(sent,dropped,received,inbound_dropped,lost_in_flight,inbox_bytes,idle_closed,connect_failures)={:?}", i + 1, d.as_ref().map(|x| &x.0), d.as_ref().map(|x| x.1));
                    }
                }
            }
        })
    };
    let res = {
        let g = lock(&c);
        let r = propose_queued(&g, l, (1..=n).map(|i| fork_c(cid(lid, i))).collect(), GROW_BATCH, Duration::from_secs(600));
        let ok = match &r {
            Ok(last) => g.wait_applied(&all, *last, Duration::from_secs(420)),
            Err(_) => false,
        };
        (r, ok)
    };
    stop.store(true, Ordering::Relaxed);
    let _ = sampler.join();
    let g = lock(&c);
    for (i, r) in g.reps.iter().enumerate() {
        let d = r.with(|x| (x.peer_progress(), x.transport_counters(), x.commit_round(), x.last_round()));
        println!("GM END n{} applied={} (progress, transport, commit, last)={:?}", i + 1, g.obs[i].applied.load(Ordering::Acquire), d);
    }
    println!("GM RESULT N={n} seed={seed} leader=n{lid} proposed={:?} grown={} grow_ms={:.0}", res.0, res.1, ms(t0));
    drop(g);
    if let Ok(m) = Arc::try_unwrap(c) {
        let mut cl = m.into_inner().unwrap_or_else(PoisonError::into_inner);
        cl.shutdown_all();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("growmon") {
        // r11_dist_fleet growmon <scratch-root> <N> <seed>: one group-committed growth, sampled each second.
        let root = PathBuf::from(args.get(2).expect("scratch root"));
        let n: u64 = args.get(3).and_then(|x| x.parse().ok()).expect("N");
        let seed: u64 = args.get(4).and_then(|x| x.parse().ok()).unwrap_or(0);
        std::fs::create_dir_all(&root).unwrap();
        println!("# r11_dist_fleet growmon N={n} seed={seed}");
        let (med, p99) = b0(&root);
        println!("B0 sync_data_ms median={med:.3} p99={p99:.3} samples=2000");
        run_growmon(n, seed, &root);
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("firstfail") {
        // r11_dist_fleet firstfail <scratch-root> <N:seed,N:seed,...> <hold-budget-s>
        let root = PathBuf::from(args.get(2).expect("scratch root"));
        std::fs::create_dir_all(&root).unwrap();
        let samples: Vec<(u64, u64)> = args
            .get(3)
            .expect("samples")
            .split(',')
            .map(|x| {
                let (a, b) = x.split_once(':').expect("N:seed");
                (a.parse().unwrap(), b.parse().unwrap())
            })
            .collect();
        let budget = Duration::from_secs(args.get(4).and_then(|x| x.parse().ok()).unwrap_or(1000));
        println!("# r11_dist_fleet firstfail samples={samples:?} budget_s={}", budget.as_secs());
        let (med, p99) = b0(&root);
        println!("B0 sync_data_ms median={med:.3} p99={p99:.3} samples=2000");
        let t0 = Instant::now();
        for (n, seed) in samples {
            // A 10^6 sample needs about 3 min; do not start one that could overrun the hold.
            let need = if n >= 1_000_000 { Duration::from_secs(240) } else { Duration::from_secs(20) };
            if t0.elapsed() + need > budget {
                println!("FF N={n} seed={seed} SKIPPED: hold budget ({} s used)", t0.elapsed().as_secs());
                continue;
            }
            let ts = Instant::now();
            run_firstfail(n, seed, &root);
            println!("# sample N={n} seed={seed} wall_ms={:.0}", ms(ts));
        }
        println!("# done wall_ms={:.0}", ms(t0));
        return;
    }
    let n: u64 = args.get(1).and_then(|s| s.parse().ok()).expect("usage: r11_dist_fleet <N> <scratch-root> [HEAD,GC,ZK,M1]");
    let root = PathBuf::from(args.get(2).expect("scratch root"));
    std::fs::create_dir_all(&root).unwrap();
    let order: Vec<Arm> = args
        .get(3)
        .map(|s| s.as_str())
        .unwrap_or("HEAD,GC,ZK,M1")
        .split(',')
        .map(|a| match a {
            "HEAD" => Arm::Head,
            "HEADQ" => Arm::HeadQ,
            "GC" => Arm::Gc,
            "ZK" => Arm::Zk,
            "M1" => Arm::M1,
            other => panic!("unknown arm {other}"),
        })
        .collect();
    println!("# r11_dist_fleet N={n} root={} order={:?} size_of::<Entry>={}", root.display(), order, std::mem::size_of::<Entry>());
    let (med, p99) = b0(&root);
    println!("B0 sync_data_ms median={med:.3} p99={p99:.3} samples=2000");
    let t0 = Instant::now();
    for arm in order {
        let ta = Instant::now();
        run_arm(arm, n, &root);
        println!("# arm {} wall_ms={:.0}", arm.name(), ms(ta));
    }
    println!("# done wall_ms={:.0}", ms(t0));
}
