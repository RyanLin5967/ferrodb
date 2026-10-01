//! r11-dist part A: E-O3 and E-O2 against the real `BranchLedger`, beside the published-fix arms, in
//! one process with no disk and no network.
//!
//! PREREG: artie-research `frontier/round11/r11-dist/PREREG.md` (registered at 079f108, before this
//! file existed). Every expected value below is computed from the fixture's constants (T, N, K, S),
//! never from the subject.
//!
//! Arms (PREREG §2): HEAD (the real ledger and the real `ClusterAgents::abandon_orphans_of`), ZK
//! (apply-time orphan set, ZooKeeper `closeSession`), M1 (an owner fence at a log position,
//! CockroachDB's liveness epoch), OFL (M1 plus the lazy record: only merges reach the log), and for
//! E-O2 the M2 snapshot arm. M1 and M2 run on `Copy`, this file's copy of HEAD's rules, whose
//! agreement with the real ledger is itself measured (`conform`).
//!
//! Modes:
//!   counts <T> <N>   P2 P3 P4 A-W A-R A-L, every arm, workload W (integers)
//!   p6               log entries per merged result, k = 16, HEAD through the real ClusterAgents
//!   p8 <seed>        safety equivalence on a seeded trace, negative controls, and `conform`
//!   o2 <N>           E-O2 part A (C0..C4), mutants F and S, anti-vacuity, negative control
//!   timed            P1, P1-fire, A-S, A-Z (run it under lockrun)
//!   adm              admitted merges per commit batch: HEAD's scalar rule vs per-key certification
//!   retire <T> <N> <N2>   item 3: M1 retire-at-snapshot, records and heap before/after, then a tail
//!   s6 <N> <budget>       item 3: a snapshot that drops the fence map must be caught
//!   conformwide <seed>    HEAD vs the copy on a trace that reaches every apply path; M1 with and
//!                         without snapshots on the same trace

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use ferrodb::agent_sql::cluster::{
    moves_base, BranchEffect, BranchLedger, ClusterAgents, ClusterBranchId, MergeVerdict,
    Replicated,
};
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::consensus::{BranchOp, Command, Entry, NodeId, Round};
use ferrodb::error::FerroError;

// ---------------------------------------------------------------------------------------------
// Heap instrument (P3): live bytes held by the global allocator.
// ---------------------------------------------------------------------------------------------

struct Counting;
static LIVE: AtomicI64 = AtomicI64::new(0);
/// A17 (1): allocator calls (alloc, alloc_zeroed, realloc), for the fence-apply attribution.
static ALLOCS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        if !p.is_null() {
            LIVE.fetch_add(l.size() as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        if !p.is_null() {
            LIVE.fetch_add(l.size() as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size() as i64, Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new) };
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        if !q.is_null() {
            LIVE.fetch_add(new as i64 - l.size() as i64, Ordering::Relaxed);
        }
        q
    }
}

#[global_allocator]
static GA: Counting = Counting;

fn heap() -> i64 {
    LIVE.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------------------------
// Fixture vocabulary
// ---------------------------------------------------------------------------------------------

/// `cluster.rs` `LOCAL_BITS` / `LOCAL_MASK` (private there; the packing is `ClusterBranchId::of`).
const LOCAL_BITS: u32 = 40;
const LOCAL_MASK: u64 = (1u64 << LOCAL_BITS) - 1;

fn cid(node: u32, local: u64) -> u64 {
    ((node as u64) << LOCAL_BITS) | local
}
fn owner_of(id: u64) -> u32 {
    (id >> LOCAL_BITS) as u32
}
/// The fence / close-owner sentinel (O2 M1): an `Abandon` of a local id no node will ever mint.
fn sentinel(node: u32) -> u64 {
    cid(node, LOCAL_MASK)
}
fn is_sentinel(id: u64) -> bool {
    id != 0 && id & LOCAL_MASK == LOCAL_MASK
}
fn fork_c(child: u64) -> Command {
    Command::Branch { op: BranchOp::Fork { child, parent: 0, fork_epoch: 1, lease_millis: 900_000 } }
}
fn merge_c(branch: u64, base_round: Round) -> Command {
    Command::Branch { op: BranchOp::Merge { branch, base_round } }
}
fn abandon_c(branch: u64) -> Command {
    Command::Branch { op: BranchOp::Abandon { branch } }
}
fn wal_c(n: u64) -> Command {
    Command::WalBatch { start_lsn: n, bytes: Vec::new() }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum V {
    Applied,
    ReEval,
    Refused,
}

fn class(v: &MergeVerdict) -> V {
    match v {
        MergeVerdict::Applied { .. } => V::Applied,
        MergeVerdict::ReEvaluate { .. } => V::ReEval,
        MergeVerdict::Refused { .. } => V::Refused,
    }
}

/// splitmix64: the fixture's only randomness, seeded from argv.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------------------------
// The arms
// ---------------------------------------------------------------------------------------------

trait Ledger {
    /// Apply one committed entry; `Some` for a merge's verdict class.
    fn apply(&mut self, e: &Entry) -> Option<V>;
    fn is_live(&self, id: u64) -> bool;
    /// Branch records held, the trunk counted as one where the arm keeps it.
    fn records(&self) -> usize;
    fn live_ids_of(&self, node: u32) -> Vec<u64>;
    /// Records touched by the single largest apply so far (HEAD: not instrumented, see `Head`).
    fn touched_max(&self) -> u64;
}

/// HEAD: the real ledger.
struct Head(Arc<Mutex<BranchLedger>>);

impl Ledger for Head {
    fn apply(&mut self, e: &Entry) -> Option<V> {
        match lock(&self.0).apply(e) {
            BranchEffect::Merged(v) => Some(class(&v)),
            _ => None,
        }
    }
    fn is_live(&self, id: u64) -> bool {
        lock(&self.0).get(ClusterBranchId(id)).map(|b| b.state.is_live()).unwrap_or(false)
    }
    fn records(&self) -> usize {
        lock(&self.0).all().count()
    }
    fn live_ids_of(&self, node: u32) -> Vec<u64> {
        lock(&self.0).live_owned_by(NodeId(node)).into_iter().map(|c| c.0).collect()
    }
    /// Not instrumented: HEAD's `apply_branch` does one map get/insert per entry (READ at 7dc428f).
    /// Printed as 0 and labelled READ in the output, never as a measurement.
    fn touched_max(&self) -> u64 {
        0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum St {
    Live,
    Merged(Round),
    Abandoned(Round),
    Reaped(Round, u32),
}

/// Same five fields and state width as `ReplicatedBranch`, so M1's heap is comparable with HEAD's.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
struct Rec {
    parent: u64,
    fork_epoch: u64,
    lease_millis: u64,
    forked_at: Round,
    st: St,
}

/// HEAD's rules copied (`cluster.rs` `apply` / `apply_branch` / `merge_verdict` at 7dc428f), plus
/// the M1 fence when `m1`, and two planted mutants for the controls.
#[derive(Clone)]
struct HeadCopy {
    br: BTreeMap<u64, Rec>,
    last_applied: Round,
    lbm: Round,
    fence: BTreeMap<u32, Round>,
    m1: bool,
    /// Mutant F (O2): the fence ignores position, so it kills the owner for ever.
    mut_f: bool,
    /// Negative control for P8: the fence is recorded but never checked.
    mut_nofence: bool,
    /// Copy mutant for `conform`: `>=` for `>` in the verdict.
    mut_ge: bool,
    touched: u64,
    /// Item 3 (retire-at-snapshot): per owner, the highest local id a snapshot has retired. A later
    /// fork of an id at or below it is refused, as HEAD refuses a fork of any id it ever recorded.
    /// Assumes per-owner ids are minted in increasing order (the owner's catalog `next_id`).
    retired_hwm: BTreeMap<u32, u64>,
    /// Branch ops this copy refused, counted where HEAD returns `BranchEffect::Rejected`.
    rejected: u64,
    /// A14 F-wm: when set (M1 only), a fork is refused unless its local id exceeds the highest local
    /// id its owner ever had ACCEPTED. Maintained at apply time, so fork acceptance depends on the log
    /// alone, never on when a node took a snapshot. Replaces the snapshot-time `retired_hwm` rule.
    use_wm: bool,
    fork_wm: BTreeMap<u32, u64>,
    /// A17 (2): CockroachDB node liveness in the model. `None` is every earlier arm, unchanged.
    hb: Option<Hb>,
}

/// A17 (2): per-node liveness records and what the rules refused. Encodings are model-internal sentinels,
/// as the M1 fence already is: HEARTBEAT(n, e, x) is a Fork of `cid(n, LOCAL_MASK - 1)` with
/// `fork_epoch = e` and `lease_millis = x`; FENCE(n, e, t) is a Merge of `sentinel(n)` with
/// `base_round = t << 16 | e` (t is the proposer's clock).
#[derive(Clone, Default)]
struct Hb {
    /// node -> (epoch, expiration)
    live: BTreeMap<u32, (u64, u64)>,
    max_offset: u64,
    fence_applied: u64,
    fence_refused: u64,
    hb_refused: u64,
    fork_epoch_refused: u64,
    /// HB-m1: the fence ignores liveness. HB-m2: the heartbeat ignores the epoch. HB-m3: the fork ignores the epoch.
    mut_fence_ignores_liveness: bool,
    mut_hb_ignores_epoch: bool,
    mut_fork_ignores_epoch: bool,
}

impl HeadCopy {
    fn new(m1: bool) -> HeadCopy {
        let mut br = BTreeMap::new();
        br.insert(
            0,
            Rec { parent: 0, fork_epoch: 0, lease_millis: u64::MAX, forked_at: 0, st: St::Live },
        );
        HeadCopy {
            br,
            last_applied: 0,
            lbm: 0,
            fence: BTreeMap::new(),
            m1,
            mut_f: false,
            mut_nofence: false,
            mut_ge: false,
            touched: 0,
            retired_hwm: BTreeMap::new(),
            rejected: 0,
            use_wm: false,
            fork_wm: BTreeMap::new(),
            hb: None,
        }
    }

    /// Item 3: a snapshot of this ledger at its `last_applied`, retiring at most `budget` records
    /// (in id order) that can never be live again: Merged, Abandoned, Reaped, or fenced. Keeps the
    /// fence map, `lbm`, `last_applied` and the retired high-water marks. A snapshot writer walks the
    /// records once (O(records)) and the result holds O(live + unretired + nodes).
    /// `drop_fence` is S6's mutant (r11-dist-refute-proof): a snapshot that loses the fence map.
    fn snapshot_retire(&self, budget: u64, drop_fence: bool) -> (HeadCopy, u64) {
        self.snapshot_retire2(budget, drop_fence, false)
    }

    /// `drop_wm` is F-wm's own negative control: a snapshot that loses the watermark map.
    fn snapshot_retire2(&self, budget: u64, drop_fence: bool, drop_wm: bool) -> (HeadCopy, u64) {
        let mut br = BTreeMap::new();
        let mut hwm = self.retired_hwm.clone();
        let mut retired = 0u64;
        for (id, r) in &self.br {
            if *id != 0 && retired < budget && !self.rec_live(*id, r) {
                retired += 1;
                let h = hwm.entry(owner_of(*id)).or_insert(0);
                *h = (*h).max(*id & LOCAL_MASK);
                continue;
            }
            br.insert(*id, *r);
        }
        let snap = HeadCopy {
            br,
            last_applied: self.last_applied,
            lbm: self.lbm,
            fence: if drop_fence { BTreeMap::new() } else { self.fence.clone() },
            m1: self.m1,
            mut_f: self.mut_f,
            mut_nofence: self.mut_nofence,
            mut_ge: self.mut_ge,
            touched: 0,
            retired_hwm: hwm,
            rejected: self.rejected,
            use_wm: self.use_wm,
            fork_wm: if drop_wm { BTreeMap::new() } else { self.fork_wm.clone() },
            hb: self.hb.clone(),
        };
        (snap, retired)
    }

    fn rec_live(&self, id: u64, r: &Rec) -> bool {
        if r.st != St::Live {
            return false;
        }
        if !self.m1 || id == 0 || self.mut_nofence {
            return true;
        }
        let o = owner_of(id);
        if self.mut_f {
            return !self.fence.contains_key(&o);
        }
        r.forked_at > self.fence.get(&o).copied().unwrap_or(0)
    }
}

impl Ledger for HeadCopy {
    fn apply(&mut self, e: &Entry) -> Option<V> {
        if e.round <= self.last_applied {
            return None;
        }
        self.last_applied = e.round;
        let round = e.round;
        let op = match &e.command {
            Command::Branch { op } => op,
            other => {
                if moves_base(other) {
                    self.lbm = round;
                }
                return None;
            }
        };
        self.touched = self.touched.max(1);
        match op {
            BranchOp::Fork { child, parent, fork_epoch, lease_millis } => {
                if let Some(hb) = self.hb.as_mut() {
                    if *child != 0 && *child & LOCAL_MASK == LOCAL_MASK - 1 {
                        // HEARTBEAT(n, e, x): applies only at the node's current epoch.
                        let rec = hb.live.entry(owner_of(*child)).or_insert((*fork_epoch, 0));
                        if rec.0 == *fork_epoch || hb.mut_hb_ignores_epoch {
                            rec.1 = rec.1.max(*lease_millis);
                        } else {
                            hb.hb_refused += 1;
                        }
                        return None;
                    }
                    // A fork carries its owner's epoch (CockroachDB's lease-epoch check).
                    let cur = hb.live.get(&owner_of(*child)).map(|r| r.0);
                    if cur != Some(*fork_epoch) && !hb.mut_fork_ignores_epoch {
                        hb.fork_epoch_refused += 1;
                        self.rejected += 1;
                        return None;
                    }
                }
                if *child == 0 || self.br.contains_key(child) {
                    self.rejected += 1;
                    return None;
                }
                if self.use_wm {
                    if self.fork_wm.get(&owner_of(*child)).is_some_and(|w| *child & LOCAL_MASK <= *w) {
                        self.rejected += 1;
                        return None;
                    }
                } else if self.retired_hwm.get(&owner_of(*child)).is_some_and(|h| *child & LOCAL_MASK <= *h) {
                    // A retired id: HEAD still holds its record and refuses the fork as a collision.
                    self.rejected += 1;
                    return None;
                }
                match self.br.get(parent) {
                    Some(p) if self.rec_live(*parent, p) => {}
                    _ => {
                        self.rejected += 1;
                        return None;
                    }
                }
                self.br.insert(
                    *child,
                    Rec {
                        parent: *parent,
                        fork_epoch: *fork_epoch,
                        lease_millis: *lease_millis,
                        forked_at: round,
                        st: St::Live,
                    },
                );
                if self.use_wm {
                    let w = self.fork_wm.entry(owner_of(*child)).or_insert(0);
                    *w = (*w).max(*child & LOCAL_MASK);
                }
                None
            }
            BranchOp::Merge { branch, base_round } => {
                if let Some(hb) = self.hb.as_mut() {
                    if is_sentinel(*branch) {
                        // FENCE(n, e, t): IncrementEpoch, only if the epoch matches and the record expired.
                        let n = owner_of(*branch);
                        let (e, t) = (*base_round & 0xFFFF, *base_round >> 16);
                        let rec = hb.live.get(&n).copied().unwrap_or((e, 0));
                        let expired = rec.1 + hb.max_offset < t;
                        if rec.0 == e && (expired || hb.mut_fence_ignores_liveness) {
                            hb.live.insert(n, (e + 1, rec.1));
                            hb.fence_applied += 1;
                            let f = self.fence.entry(n).or_insert(0);
                            *f = (*f).max(round);
                        } else {
                            hb.fence_refused += 1;
                        }
                        return None;
                    }
                }
                let v = match self.br.get(branch) {
                    None => V::Refused,
                    Some(r) if !self.rec_live(*branch, r) => V::Refused,
                    Some(_) => {
                        let moved = if self.mut_ge { self.lbm >= *base_round } else { self.lbm > *base_round };
                        if moved { V::ReEval } else { V::Applied }
                    }
                };
                if v == V::Applied {
                    if let Some(r) = self.br.get_mut(branch) {
                        r.st = St::Merged(round);
                    }
                    self.lbm = round;
                }
                Some(v)
            }
            BranchOp::Abandon { branch } => {
                if self.m1 && is_sentinel(*branch) {
                    let f = self.fence.entry(owner_of(*branch)).or_insert(0);
                    *f = (*f).max(round);
                    return None;
                }
                match self.br.get(branch).copied() {
                    // HEAD checks `state.is_live()` only (not the fence): kept identical.
                    Some(r) if r.st == St::Live => {
                        self.br.get_mut(branch).unwrap().st = St::Abandoned(round);
                    }
                    _ => self.rejected += 1,
                }
                None
            }
            BranchOp::Reap { branch, generation } => {
                match self.br.get_mut(branch) {
                    Some(r) if !matches!(r.st, St::Reaped(..)) => {
                        r.st = St::Reaped(round, *generation);
                    }
                    _ => self.rejected += 1,
                }
                None
            }
        }
    }
    fn is_live(&self, id: u64) -> bool {
        self.br.get(&id).map(|r| self.rec_live(id, r)).unwrap_or(false)
    }
    fn records(&self) -> usize {
        self.br.len()
    }
    fn live_ids_of(&self, node: u32) -> Vec<u64> {
        self.br
            .iter()
            .filter(|(id, r)| **id != 0 && owner_of(**id) == node && self.rec_live(**id, r))
            .map(|(id, _)| *id)
            .collect()
    }
    fn touched_max(&self) -> u64 {
        self.touched
    }
}

/// ZK: records exist only while live; a close-owner command deletes the owner's set AT APPLY.
struct Zk {
    recs: BTreeMap<u64, Rec>,
    by_owner: BTreeMap<u32, BTreeSet<u64>>,
    last_applied: Round,
    lbm: Round,
    touched: u64,
}

impl Zk {
    fn new() -> Zk {
        Zk { recs: BTreeMap::new(), by_owner: BTreeMap::new(), last_applied: 0, lbm: 0, touched: 0 }
    }
    fn delete(&mut self, id: u64) {
        if self.recs.remove(&id).is_some() {
            if let Some(s) = self.by_owner.get_mut(&owner_of(id)) {
                s.remove(&id);
            }
        }
    }
}

impl Ledger for Zk {
    fn apply(&mut self, e: &Entry) -> Option<V> {
        if e.round <= self.last_applied {
            return None;
        }
        self.last_applied = e.round;
        let round = e.round;
        let op = match &e.command {
            Command::Branch { op } => op,
            other => {
                if moves_base(other) {
                    self.lbm = round;
                }
                return None;
            }
        };
        self.touched = self.touched.max(1);
        match op {
            BranchOp::Fork { child, parent, fork_epoch, lease_millis } => {
                if *child == 0 || self.recs.contains_key(child) {
                    return None;
                }
                if *parent != 0 && !self.recs.contains_key(parent) {
                    return None;
                }
                self.recs.insert(
                    *child,
                    Rec {
                        parent: *parent,
                        fork_epoch: *fork_epoch,
                        lease_millis: *lease_millis,
                        forked_at: round,
                        st: St::Live,
                    },
                );
                self.by_owner.entry(owner_of(*child)).or_default().insert(*child);
                None
            }
            BranchOp::Merge { branch, base_round } => {
                let v = if !self.recs.contains_key(branch) {
                    V::Refused
                } else if self.lbm > *base_round {
                    V::ReEval
                } else {
                    V::Applied
                };
                if v == V::Applied {
                    self.delete(*branch);
                    self.lbm = round;
                }
                Some(v)
            }
            BranchOp::Abandon { branch } => {
                if is_sentinel(*branch) {
                    // `ephemerals.remove(session)`, then delete every member: the whole set, here.
                    let set = self.by_owner.remove(&owner_of(*branch)).unwrap_or_default();
                    self.touched = self.touched.max(set.len() as u64);
                    for id in set {
                        self.recs.remove(&id);
                    }
                } else {
                    self.delete(*branch);
                }
                None
            }
            BranchOp::Reap { branch, .. } => {
                self.delete(*branch);
                None
            }
        }
    }
    fn is_live(&self, id: u64) -> bool {
        id == 0 || self.recs.contains_key(&id)
    }
    fn records(&self) -> usize {
        self.recs.len() + 1
    }
    fn live_ids_of(&self, node: u32) -> Vec<u64> {
        self.by_owner.get(&node).map(|s| s.iter().copied().collect()).unwrap_or_default()
    }
    fn touched_max(&self) -> u64 {
        self.touched
    }
}

/// OFL's own log vocabulary: only merges, fences and base-moving others reach it.
#[derive(Clone, Debug)]
enum OCmd {
    Merge { branch: u64, base_round: Round, inc: u32 },
    Fence { node: u32, dead_inc: u32 },
    Other { moves: bool },
}

#[derive(Clone, Default)]
struct Ofl {
    cur: BTreeMap<u32, u32>,
    lbm: Round,
    last_applied: Round,
    /// Negative control: the incarnation check deleted.
    mut_noinc: bool,
}

impl Ofl {
    fn apply(&mut self, round: Round, c: &OCmd) -> Option<V> {
        if round <= self.last_applied {
            return None;
        }
        self.last_applied = round;
        match c {
            OCmd::Merge { branch, base_round, inc } => {
                let cur = self.cur.get(&owner_of(*branch)).copied().unwrap_or(0);
                let v = if !self.mut_noinc && *inc != cur {
                    V::Refused
                } else if self.lbm > *base_round {
                    V::ReEval
                } else {
                    V::Applied
                };
                if v == V::Applied {
                    self.lbm = round;
                }
                Some(v)
            }
            OCmd::Fence { node, dead_inc } => {
                let c = self.cur.entry(*node).or_insert(0);
                *c = (*c).max(dead_inc + 1);
                None
            }
            OCmd::Other { moves } => {
                if *moves {
                    self.lbm = round;
                }
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The counting seam: the real `ClusterAgents` proposes through it, and nothing is fsynced.
// ---------------------------------------------------------------------------------------------

struct CountSeam {
    leader: NodeId,
    term: u64,
    next: AtomicU64,
    proposals: AtomicU64,
    sink: Mutex<Vec<Entry>>,
    /// Ledgers `pump` applies the sink to (the coordinator's own, for `merge`'s waits).
    apply_to: Vec<Arc<Mutex<BranchLedger>>>,
    applied: Mutex<usize>,
}

impl CountSeam {
    fn new(leader: NodeId, term: u64, next_round: Round, apply_to: Vec<Arc<Mutex<BranchLedger>>>) -> Arc<CountSeam> {
        Arc::new(CountSeam {
            leader,
            term,
            next: AtomicU64::new(next_round),
            proposals: AtomicU64::new(0),
            sink: Mutex::new(Vec::new()),
            apply_to,
            applied: Mutex::new(0),
        })
    }
    fn take(&self) -> Vec<Entry> {
        std::mem::take(&mut *lock(&self.sink))
    }
}

impl Replicated for CountSeam {
    fn propose(&self, c: Command) -> Result<Round, FerroError> {
        let round = self.next.fetch_add(1, Ordering::SeqCst);
        lock(&self.sink).push(Entry { term: self.term, round, command: c });
        self.proposals.fetch_add(1, Ordering::SeqCst);
        Ok(round)
    }
    fn committed_head(&self) -> Round {
        self.next.load(Ordering::SeqCst) - 1
    }
    fn pump(&self) -> Result<(), FerroError> {
        let sink = lock(&self.sink);
        let mut applied = lock(&self.applied);
        for e in &sink[*applied..] {
            for l in &self.apply_to {
                lock(l).apply(e);
            }
        }
        *applied = sink.len();
        Ok(())
    }
    fn leader(&self) -> Option<NodeId> {
        Some(self.leader)
    }
}

/// HEAD's disposal, through the real code: `abandon_orphans_of` on a coordinator bound to
/// `sweeper`, whose ledger is `ledger`. Returns the proposed entries (not yet applied anywhere).
fn head_sweep(ledger: &Arc<Mutex<BranchLedger>>, sweeper: u32, dead: u32, term: u64, next_round: Round) -> Vec<Entry> {
    let seam = CountSeam::new(NodeId(sweeper), term, next_round, Vec::new());
    let agents = ClusterAgents::new(NodeId(sweeper), Arc::new(AgentRuntime::new()), seam.clone(), ledger.clone());
    let out = agents.abandon_orphans_of(NodeId(dead)).expect("abandon_orphans_of refused");
    let entries = seam.take();
    assert_eq!(out.len(), entries.len(), "the sweep's report and its proposals disagree");
    assert_eq!(agents.cost().proposals, entries.len() as u64);
    entries
}

fn check(id: &str, arm: &str, pred: &str, ok: bool, got: String) {
    println!("CHECK {id} arm={arm} pred={pred} got={got} {}", if ok { "PASS" } else { "FAIL" });
}

// ---------------------------------------------------------------------------------------------
// Workload W (E-O3): n1 forks T; the first T-N are disposed (i % 16 == 0 merges, else abandons);
// the last N stay live. Streamed, so nothing but the ledger holds T.
// ---------------------------------------------------------------------------------------------

fn run_w(t: u64, n: u64, mut sink: impl FnMut(Entry)) -> Round {
    let mut round = 0;
    for i in 1..=t {
        round += 1;
        sink(Entry { term: 1, round, command: fork_c(cid(1, i)) });
    }
    for i in 1..=(t - n) {
        let head = round;
        round += 1;
        let c = if (i - 1) % 16 == 0 { merge_c(cid(1, i), head) } else { abandon_c(cid(1, i)) };
        sink(Entry { term: 1, round, command: c });
    }
    round
}

fn merges_in_w(t: u64, n: u64) -> u64 {
    (t - n).div_ceil(16)
}

fn mode_counts(t: u64, n: u64) {
    assert!(n <= t && n >= 1);
    println!("# counts T={t} N={n}");
    // ---------------- HEAD
    {
        let h0 = heap();
        let ledger = Arc::new(Mutex::new(BranchLedger::new()));
        let mut head = Head(ledger.clone());
        let (mut forks, mut verdicts, mut applied_v) = (0u64, 0u64, 0u64);
        let last = run_w(t, n, |e| {
            if matches!(e.command, Command::Branch { op: BranchOp::Fork { .. } }) {
                forks += 1;
            }
            if let Some(v) = head.apply(&e) {
                verdicts += 1;
                if v == V::Applied {
                    applied_v += 1;
                }
            }
        });
        let h1 = heap();
        let recs = head.records();
        let t0 = Instant::now();
        let props = head_sweep(&ledger, 2, 1, 2, last + 1);
        let sweep_ms = t0.elapsed().as_secs_f64() * 1e3;
        let p4 = props.len() as u64;
        for e in &props {
            head.apply(e);
        }
        let live_after = head.live_ids_of(1).len();
        let recs_after = head.records();
        let bpr = (h1 - h0) as f64 / recs as f64;
        println!(
            "ARM=HEAD T={t} N={n} forks_applied={forks} verdicts={verdicts} applied={applied_v} P2_records={recs} P3_heap_bytes={} P3_bytes_per_record={bpr:.1} P4_proposals={p4} A-W=READ:1 A-R_records_after={recs_after} A-L_n1_live_after={live_after} sweep_ms_inmem={sweep_ms:.1}",
            h1 - h0
        );
        check("anti-vacuity", "HEAD", &format!("forks={t},verdicts={}", merges_in_w(t, n)), forks == t && verdicts == merges_in_w(t, n) && applied_v == verdicts, format!("{forks},{verdicts},applied={applied_v}"));
        check("P2", "HEAD", &format!("{}", t + 1), recs as u64 == t + 1, recs.to_string());
        check("P3", "HEAD", "90..150", (90.0..=150.0).contains(&bpr), format!("{bpr:.1}"));
        check("P4", "HEAD", &n.to_string(), p4 == n, p4.to_string());
        check("A-R", "HEAD", &format!("{}", t + 1), recs_after as u64 == t + 1, recs_after.to_string());
        check("A-L", "HEAD", "0", live_after == 0, live_after.to_string());
    }
    // ---------------- ZK
    {
        let h0 = heap();
        let mut zk = Zk::new();
        let last = run_w(t, n, |e| {
            zk.apply(&e);
        });
        let h1 = heap();
        let recs = zk.records();
        let before_touched = zk.touched_max();
        zk.apply(&Entry { term: 2, round: last + 1, command: abandon_c(sentinel(1)) });
        let aw = zk.touched_max();
        let live_after = zk.live_ids_of(1).len();
        let bpr = (h1 - h0) as f64 / (n + 1) as f64;
        println!(
            "ARM=ZK T={t} N={n} P2_records={recs} P3_heap_bytes={} P3_bytes_per_live_record={bpr:.1} P4_proposals=1 A-W_touched_max={aw} (before_disposal={before_touched}) A-R_records_after={} A-L_n1_live_after={live_after}",
            h1 - h0,
            zk.records()
        );
        check("P2", "ZK", &format!("{}", n + 1), recs as u64 == n + 1, recs.to_string());
        check("P3", "ZK", "110..200 per live", (110.0..=200.0).contains(&bpr), format!("{bpr:.1}"));
        check("A-W", "ZK", &n.to_string(), aw == n, aw.to_string());
        check("A-R", "ZK", "1", zk.records() == 1, zk.records().to_string());
        check("A-L", "ZK", "0", live_after == 0, live_after.to_string());
    }
    // ---------------- M1
    {
        let h0 = heap();
        let mut m1 = HeadCopy::new(true);
        let last = run_w(t, n, |e| {
            m1.apply(&e);
        });
        let h1 = heap();
        let recs = m1.records();
        m1.apply(&Entry { term: 2, round: last + 1, command: abandon_c(sentinel(1)) });
        let live_after = m1.live_ids_of(1).len();
        let bpr = (h1 - h0) as f64 / recs as f64;
        println!(
            "ARM=M1 T={t} N={n} P2_records={recs} P3_heap_bytes={} P3_bytes_per_record={bpr:.1} P4_proposals=1 A-W_touched_max={} A-R_records_after={} A-L_n1_live_after={live_after}",
            h1 - h0,
            m1.touched_max(),
            m1.records()
        );
        check("P2", "M1", &format!("{}", t + 1), recs as u64 == t + 1, recs.to_string());
        check("P3", "M1", "90..150", (90.0..=150.0).contains(&bpr), format!("{bpr:.1}"));
        check("A-W", "M1", "1", m1.touched_max() == 1, m1.touched_max().to_string());
        check("A-R", "M1", &format!("{}", t + 1), m1.records() as u64 == t + 1, m1.records().to_string());
        check("A-L", "M1", "0", live_after == 0, live_after.to_string());
    }
    // ---------------- OFL: only merges reach the log
    {
        let h0 = heap();
        let mut o = Ofl::default();
        let mut round = 0;
        let mut entries = 0u64;
        for i in 1..=(t - n) {
            if (i - 1) % 16 == 0 {
                let head = round;
                round += 1;
                entries += 1;
                o.apply(round, &OCmd::Merge { branch: cid(1, i), base_round: head, inc: 0 });
            }
        }
        round += 1;
        o.apply(round, &OCmd::Fence { node: 1, dead_inc: 0 });
        let h1 = heap();
        let bytes = h1 - h0;
        println!("ARM=OFL T={t} N={n} P2_branch_records=0 state_nodes={} P3_heap_bytes_total={bytes} P4_proposals=1 log_entries_lifecycle={entries}", o.cur.len());
        check("P3", "OFL", "<65536 total", bytes < 65536, bytes.to_string());
        check("OFL-entries", "OFL", &merges_in_w(t, n).to_string(), entries == merges_in_w(t, n), entries.to_string());
    }
}

// ---------------------------------------------------------------------------------------------
// P6: entries per merged result, k = 16, HEAD through the real ClusterAgents
// ---------------------------------------------------------------------------------------------

fn mode_p6() {
    use ferrodb::agent_sql::runtime::{ExecCtx, RunIdentity};
    use ferrodb::branch::types::BranchId;
    use ferrodb::buffer::buffer_pool::BufferPoolManager;
    use ferrodb::catalog::catalog::Catalog;
    use ferrodb::storage::disk_manager::DiskManager;
    use ferrodb::wal::log::WalManager;
    use ferrodb::wal::txn::TxnManager;

    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("p6.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("p6.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let runtime = Arc::new(AgentRuntime::new());

    let ledger = Arc::new(Mutex::new(BranchLedger::new()));
    let seam = CountSeam::new(NodeId(1), 1, 1, vec![ledger.clone()]);
    let agents = ClusterAgents::new(NodeId(1), runtime.clone(), seam.clone(), ledger.clone());
    let mut sessions = Vec::new();
    for _ in 0..16 {
        sessions.push(
            agents
                .fork(RunIdentity { agent_id: "p6", run_id: Some("r"), ..RunIdentity::default() }, BranchId::TRUNK)
                .unwrap(),
        );
    }
    seam.pump().unwrap();
    for s in &sessions[1..] {
        agents.abandon(s).unwrap();
    }
    let mut ctx = ExecCtx { catalog: &mut catalog, bp: bp.clone(), txn: txn.clone() };
    let r = agents.merge(&mut ctx, sessions[0].branch());
    let merged = match &r {
        Ok(rep) => rep.merge_round.is_some(),
        Err(e) => {
            println!("P6 merge error: {e}");
            false
        }
    };
    seam.pump().unwrap();
    let log = lock(&seam.sink).clone();
    let mut kinds: BTreeMap<&str, u64> = BTreeMap::new();
    for e in &log {
        let k = match &e.command {
            Command::Branch { op: BranchOp::Fork { .. } } => "Fork",
            Command::Branch { op: BranchOp::Merge { .. } } => "Merge",
            Command::Branch { op: BranchOp::Abandon { .. } } => "Abandon",
            Command::Branch { op: BranchOp::Reap { .. } } => "Reap",
            Command::LeaseTick { .. } => "LeaseTick",
            _ => "Other",
        };
        *kinds.entry(k).or_default() += 1;
    }
    println!("ARM=HEAD P6 merged={merged} entries={} by_kind={kinds:?} proposals_counter={}", log.len(), agents.cost().proposals);
    check("P6", "HEAD", "32 (16F+1M+15A, 0 Reap, 0 LeaseTick)", merged && log.len() == 32 && kinds.get("Fork") == Some(&16) && kinds.get("Merge") == Some(&1) && kinds.get("Abandon") == Some(&15) && !kinds.contains_key("Reap") && !kinds.contains_key("LeaseTick"), log.len().to_string());
    println!("ARM=ZK P6 entries=32 (BY ARM DEFINITION: ZooKeeper logs every create and delete)");
    println!("ARM=M1 P6 entries=32 (BY ARM DEFINITION: M1 changes only the disposal)");
    println!("ARM=OFL P6 entries=1 (BY ARM DEFINITION: only the merge reaches the log)");
}

// ---------------------------------------------------------------------------------------------
// P8: seeded trace, safety equivalence, negative controls; plus `conform` on the same log
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Ev {
    Fork { id: u64, inc: u32 },
    Merge { id: u64, base_pos: usize, inc: u32, planted: bool, dup: bool },
    Abandon { id: u64 },
    Write,
    Fail { owner: u32, dead_inc: u32 },
}

fn gen_trace(seed: u64, len: usize) -> Vec<Ev> {
    let mut rng = Rng(seed);
    let mut evs: Vec<Ev> = Vec::with_capacity(len + 64);
    let mut next_local = [0u64; 4];
    let mut inc = [0u32; 4];
    // generator's view: live (id, fork_pos, inc) per owner
    let mut live: Vec<Vec<(u64, usize, u32)>> = vec![Vec::new(); 4];
    let fail_at = [len * 3 / 10, len * 6 / 10];
    let fail_owner = [1u32, 2u32];
    while evs.len() < len {
        let pos = evs.len();
        if let Some(k) = fail_at.iter().position(|&p| p == pos) {
            let o = fail_owner[k];
            let fenced: Vec<(u64, usize, u32)> = std::mem::take(&mut live[o as usize]);
            evs.push(Ev::Fail { owner: o, dead_inc: inc[o as usize] });
            // planted: a zombie merge from the fenced incarnation, with a FRESH base
            if let Some(&(id, _, i)) = fenced.first() {
                let p = evs.len();
                evs.push(Ev::Merge { id, base_pos: p - 1, inc: i, planted: true, dup: false });
            }
            inc[o as usize] += 1;
            continue;
        }
        let r = rng.below(100);
        let o = 1 + rng.below(3) as u32;
        if r < 40 || live[o as usize].is_empty() {
            next_local[o as usize] += 1;
            let id = cid(o, next_local[o as usize]);
            evs.push(Ev::Fork { id, inc: inc[o as usize] });
            live[o as usize].push((id, pos, inc[o as usize]));
        } else if r < 60 {
            let k = rng.below(live[o as usize].len() as u64) as usize;
            let (id, fpos, i) = live[o as usize].swap_remove(k);
            // The gate read a committed head at or after the fork (HEAD's merge pumps until the
            // fork is applied before reading it), up to 50 events stale.
            let lo = fpos.max(pos.saturating_sub(50));
            let hi = pos - 1;
            let base_pos = lo + rng.below((hi - lo + 1) as u64) as usize;
            evs.push(Ev::Merge { id, base_pos, inc: i, planted: false, dup: false });
            if rng.below(100) == 0 {
                evs.push(Ev::Merge { id, base_pos, inc: i, planted: false, dup: true });
            }
        } else if r < 80 {
            let k = rng.below(live[o as usize].len() as u64) as usize;
            let (id, _, _) = live[o as usize].swap_remove(k);
            evs.push(Ev::Abandon { id });
        } else {
            evs.push(Ev::Write);
        }
    }
    evs
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum P8Arm {
    Head,
    Zk,
    M1,
    M1NoFence,
}

/// Run the trace through one log-carrying arm. Returns (verdict per event index, the log).
fn p8_run(evs: &[Ev], arm: P8Arm) -> (BTreeMap<usize, V>, Vec<Entry>) {
    let head_ledger = Arc::new(Mutex::new(BranchLedger::new()));
    let mut l: Box<dyn Ledger> = match arm {
        P8Arm::Head => Box::new(Head(head_ledger.clone())),
        P8Arm::Zk => Box::new(Zk::new()),
        P8Arm::M1 => Box::new(HeadCopy::new(true)),
        P8Arm::M1NoFence => {
            let mut c = HeadCopy::new(true);
            c.mut_nofence = true;
            Box::new(c)
        }
    };
    let mut log: Vec<Entry> = Vec::new();
    let mut pos_round: Vec<Round> = Vec::with_capacity(evs.len());
    let mut out = BTreeMap::new();
    let mut wal = 0u64;
    for (p, ev) in evs.iter().enumerate() {
        let push = |c: Command, log: &mut Vec<Entry>, l: &mut Box<dyn Ledger>| -> Option<V> {
            let e = Entry { term: 1, round: log.len() as Round + 1, command: c };
            let v = l.apply(&e);
            log.push(e);
            v
        };
        match ev {
            Ev::Fork { id, .. } => {
                push(fork_c(*id), &mut log, &mut l);
            }
            Ev::Merge { id, base_pos, .. } => {
                let base = pos_round[*base_pos];
                if let Some(v) = push(merge_c(*id, base), &mut log, &mut l) {
                    out.insert(p, v);
                }
            }
            Ev::Abandon { id } => {
                push(abandon_c(*id), &mut log, &mut l);
            }
            Ev::Write => {
                wal += 1;
                push(wal_c(wal), &mut log, &mut l);
            }
            Ev::Fail { owner, .. } => match arm {
                P8Arm::Head => {
                    let sweeper = owner % 3 + 1;
                    let props = head_sweep(&head_ledger, sweeper, *owner, 1, log.len() as Round + 1);
                    for e in props {
                        l.apply(&e);
                        log.push(e);
                    }
                }
                _ => {
                    push(abandon_c(sentinel(*owner)), &mut log, &mut l);
                }
            },
        }
        pos_round.push(log.len() as Round);
    }
    (out, log)
}

fn p8_ofl(evs: &[Ev], mut_noinc: bool) -> BTreeMap<usize, V> {
    let mut o = Ofl { mut_noinc, ..Ofl::default() };
    let mut round = 0;
    let mut pos_round: Vec<Round> = Vec::with_capacity(evs.len());
    let mut out = BTreeMap::new();
    for (p, ev) in evs.iter().enumerate() {
        match ev {
            Ev::Merge { id, base_pos, inc, .. } => {
                round += 1;
                if let Some(v) = o.apply(round, &OCmd::Merge { branch: *id, base_round: pos_round[*base_pos], inc: *inc }) {
                    out.insert(p, v);
                }
            }
            Ev::Write => {
                round += 1;
                o.apply(round, &OCmd::Other { moves: true });
            }
            Ev::Fail { owner, dead_inc } => {
                round += 1;
                o.apply(round, &OCmd::Fence { node: *owner, dead_inc: *dead_inc });
            }
            Ev::Fork { .. } | Ev::Abandon { .. } => {}
        }
        pos_round.push(round);
    }
    out
}

fn mode_p8(seed: u64) {
    let evs = gen_trace(seed, 100_000);
    let merges: Vec<usize> = evs.iter().enumerate().filter(|(_, e)| matches!(e, Ev::Merge { .. })).map(|(i, _)| i).collect();
    let planted: BTreeSet<usize> = evs.iter().enumerate().filter(|(_, e)| matches!(e, Ev::Merge { planted: true, .. })).map(|(i, _)| i).collect();
    let dups: BTreeSet<usize> = evs.iter().enumerate().filter(|(_, e)| matches!(e, Ev::Merge { dup: true, .. })).map(|(i, _)| i).collect();
    let fails = evs.iter().filter(|e| matches!(e, Ev::Fail { .. })).count();
    println!("# p8 seed={seed} events={} merges={} planted={} dups={} failovers={fails}", evs.len(), merges.len(), planted.len(), dups.len());

    let (vh, head_log) = p8_run(&evs, P8Arm::Head);
    let (vz, _) = p8_run(&evs, P8Arm::Zk);
    let (vm, _) = p8_run(&evs, P8Arm::M1);
    let (vmn, _) = p8_run(&evs, P8Arm::M1NoFence);
    let vo = p8_ofl(&evs, false);
    let von = p8_ofl(&evs, true);

    let applied_count = vh.values().filter(|v| **v == V::Applied).count();
    let reeval_count = vh.values().filter(|v| **v == V::ReEval).count();
    println!("HEAD verdicts={} applied={applied_count} reeval={reeval_count} log_entries={}", vh.len(), head_log.len());
    check("anti-vacuity", "HEAD", "every merge has a verdict; >=1 Applied; >=1 ReEval", vh.len() == merges.len() && applied_count > 0 && reeval_count > 0, format!("{}/{}", vh.len(), merges.len()));

    let class_div = |other: &BTreeMap<usize, V>| merges.iter().filter(|p| !planted.contains(p) && vh.get(p) != other.get(p)).count();
    let appl_div = |other: &BTreeMap<usize, V>| {
        merges
            .iter()
            .filter(|p| !planted.contains(p) && (vh.get(p) == Some(&V::Applied)) != (other.get(p) == Some(&V::Applied)))
            .count()
    };
    let planted_applied = |vv: &BTreeMap<usize, V>| planted.iter().filter(|p| vv.get(p) == Some(&V::Applied)).count();
    let dup_applied = |vv: &BTreeMap<usize, V>| dups.iter().filter(|p| vv.get(p) == Some(&V::Applied)).count();

    for (name, vv) in [("HEAD", &vh), ("ZK", &vz), ("M1", &vm), ("OFL", &vo)] {
        println!("ARM={name} planted_applied={} dup_applied={}", planted_applied(vv), dup_applied(vv));
        check("P8-planted", name, "0", planted_applied(vv) == 0, planted_applied(vv).to_string());
        check("P8-dup", name, "0", dup_applied(vv) == 0, dup_applied(vv).to_string());
    }
    check("P8-class", "ZK", "0", class_div(&vz) == 0, class_div(&vz).to_string());
    check("P8-class", "M1", "0", class_div(&vm) == 0, class_div(&vm).to_string());
    check("P8-applied", "OFL", "0", appl_div(&vo) == 0, appl_div(&vo).to_string());
    let nc_m1 = planted_applied(&vmn) + class_div(&vmn);
    let nc_ofl = planted_applied(&von) + appl_div(&von);
    check("P8-negctl", "M1-nofence", ">=1", nc_m1 >= 1, nc_m1.to_string());
    check("P8-negctl", "OFL-noinc", ">=1", nc_ofl >= 1, nc_ofl.to_string());

    // conform: the identical HEAD log through the copy, and through the copy mutant
    let conform = |mut_ge: bool| -> usize {
        let real = Arc::new(Mutex::new(BranchLedger::new()));
        let mut r = Head(real);
        let mut c = HeadCopy::new(false);
        c.mut_ge = mut_ge;
        let mut div = 0;
        for e in &head_log {
            if r.apply(e) != c.apply(e) {
                div += 1;
            }
        }
        let ids: BTreeSet<u64> = evs
            .iter()
            .filter_map(|e| match e {
                Ev::Fork { id, .. } => Some(*id),
                _ => None,
            })
            .collect();
        div + ids.iter().filter(|id| r.is_live(**id) != c.is_live(**id)).count()
    };
    let cd = conform(false);
    let cdm = conform(true);
    check("conform", "Copy", "0", cd == 0, cd.to_string());
    check("conform-negctl", "Copy->=", ">=1", cdm >= 1, cdm.to_string());
}

// ---------------------------------------------------------------------------------------------
// E-O2 part A
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum O2Kind {
    Head,
    Copy { m1: bool, m2: bool },
}

#[derive(Clone, Copy, Debug)]
struct O2Arm {
    name: &'static str,
    kind: O2Kind,
    barrier: bool,
    sweeper: u32,
    restart_n3: bool,
    failover: bool,
    mut_f: bool,
    mut_s: bool,
}

struct O2Fixture {
    log: Vec<Entry>,
    f: Round,
    r_c: Round,
    t1_ids: Vec<u64>,
    tail_ids: Vec<u64>,
    merged_ids: Vec<u64>,
}

const O2_K: u64 = 64;
const O2_S: u64 = 1000;

fn o2_term1(n: u64) -> O2Fixture {
    let mut log: Vec<Entry> = Vec::new();
    let push = |log: &mut Vec<Entry>, term: u64, c: Command| -> Round {
        let round = log.len() as Round + 1;
        log.push(Entry { term, round, command: c });
        round
    };
    let mut t1_ids = Vec::new();
    let mut merged_ids = Vec::new();
    let mut local = 0u64;
    let mut next = || {
        local += 1;
        cid(1, local)
    };
    push(&mut log, 1, Command::NoOp);
    for _ in 0..n / 2 {
        let id = next();
        t1_ids.push(id);
        push(&mut log, 1, fork_c(id));
    }
    push(&mut log, 1, wal_c(1));
    let pm = next();
    t1_ids.push(pm);
    let f = push(&mut log, 1, fork_c(pm));
    push(&mut log, 1, merge_c(pm, f - 2));
    for _ in 0..n / 2 {
        let id = next();
        t1_ids.push(id);
        push(&mut log, 1, fork_c(id));
    }
    for _ in 0..n / 10 {
        let id = next();
        merged_ids.push(id);
        let fr = push(&mut log, 1, fork_c(id));
        push(&mut log, 1, merge_c(id, fr));
    }
    let r_c = log.len() as Round;
    let mut tail_ids = Vec::new();
    for _ in 0..O2_K {
        let id = next();
        tail_ids.push(id);
        push(&mut log, 1, fork_c(id));
    }
    O2Fixture { log, f, r_c, t1_ids, tail_ids, merged_ids }
}

/// One node's ledger in an O2 arm: HEAD's real ledger, or the copy.
enum O2L {
    Head(Arc<Mutex<BranchLedger>>),
    Copy(HeadCopy),
}

impl O2L {
    fn fresh(kind: O2Kind, mut_f: bool) -> O2L {
        match kind {
            O2Kind::Head => O2L::Head(Arc::new(Mutex::new(BranchLedger::new()))),
            O2Kind::Copy { m1, .. } => {
                let mut c = HeadCopy::new(m1);
                c.mut_f = mut_f;
                O2L::Copy(c)
            }
        }
    }
    fn apply(&mut self, e: &Entry) -> Option<V> {
        match self {
            O2L::Head(l) => Head(l.clone()).apply(e),
            O2L::Copy(c) => c.apply(e),
        }
    }
    fn is_live(&self, id: u64) -> bool {
        match self {
            O2L::Head(l) => Head(l.clone()).is_live(id),
            O2L::Copy(c) => c.is_live(id),
        }
    }
    fn has(&self, id: u64) -> bool {
        match self {
            O2L::Head(l) => lock(l).get(ClusterBranchId(id)).is_some(),
            O2L::Copy(c) => c.br.contains_key(&id),
        }
    }
}

struct O2Out {
    c0: u64,
    c1: [u64; 3],
    c2: u64,
    c3: u64,
    c4: u64,
    anti_vacuity: bool,
}

fn o2_run(n: u64, arm: O2Arm) -> O2Out {
    let fx = o2_term1(n);
    let mut log = fx.log.clone();
    let mut_s = arm.mut_s;

    // The snapshot at floor F (M2): the copy's whole state, applied through F.
    let snapshot = |fx: &O2Fixture| -> HeadCopy {
        let (m1, _) = match arm.kind {
            O2Kind::Copy { m1, m2 } => (m1, m2),
            O2Kind::Head => (false, false),
        };
        let mut c = HeadCopy::new(m1);
        c.mut_f = arm.mut_f;
        for e in &fx.log[..fx.f as usize] {
            c.apply(e);
        }
        if mut_s {
            c.lbm = 0;
        }
        c
    };
    let m2 = matches!(arm.kind, O2Kind::Copy { m2: true, .. });
    let restarted = |fx: &O2Fixture| -> O2L {
        if m2 {
            O2L::Copy(snapshot(fx))
        } else {
            let mut l = O2L::fresh(arm.kind, arm.mut_f);
            // HEAD's restart: a new ledger, and `applied` seeded from the floor (node.rs :370-373).
            if let O2L::Copy(c) = &mut l {
                c.last_applied = fx.f;
            }
            if let O2L::Head(h) = &l {
                // A real BranchLedger cannot be seeded; it sees only rounds above F because only
                // those are handed to it below.
                let _ = h;
            }
            l
        }
    };

    let mut anti_vacuity = true;
    let mut c0 = 0u64;
    if arm.failover {
        // Term 2: NoOp(2) after the inherited tail.
        let noop2 = log.len() as Round + 1;
        log.push(Entry { term: 2, round: noop2, command: Command::NoOp });
        // The sweeper's view at the sweep.
        let sweeper_restarted = arm.sweeper == 3 && arm.restart_n3;
        let mut sw = if sweeper_restarted { restarted(&fx) } else { O2L::fresh(arm.kind, arm.mut_f) };
        let upto = if arm.barrier { noop2 } else { fx.r_c };
        let from = if sweeper_restarted { fx.f + 1 } else { 1 };
        for e in &log[(from - 1) as usize..upto as usize] {
            sw.apply(e);
        }
        if !arm.barrier {
            anti_vacuity = fx.tail_ids.iter().all(|id| {
                let r = log.iter().position(|e| matches!(&e.command, Command::Branch { op: BranchOp::Fork { child, .. } } if child == id));
                r.is_some() && !sw.has(*id)
            });
        }
        let next_round = log.len() as Round + 1;
        let sweep: Vec<Entry> = match (&sw, arm.kind) {
            (O2L::Head(h), _) => head_sweep(h, arm.sweeper, 1, 2, next_round),
            (O2L::Copy(_), O2Kind::Copy { m1: true, .. }) => {
                vec![Entry { term: 2, round: next_round, command: abandon_c(sentinel(1)) }]
            }
            (O2L::Copy(c), _) => c
                .live_ids_of(1)
                .into_iter()
                .enumerate()
                .map(|(i, id)| Entry { term: 2, round: next_round + i as Round, command: abandon_c(id) })
                .collect(),
        };
        c0 = sweep.len() as u64;
        log.extend(sweep);
        for i in 1..=O2_S {
            let r = log.len() as Round + 1;
            log.push(Entry { term: 2, round: r, command: fork_c(cid(2, i)) });
        }
        let r = log.len() as Round + 1;
        log.push(Entry { term: 3, round: r, command: Command::NoOp });
        let r = log.len() as Round + 1;
        log.push(Entry { term: 3, round: r, command: fork_c(cid(1, 1_000_000_000)) });
    }

    // Final ledgers: n1, n2 fresh over the whole log; n3 restarted or fresh.
    let mut nodes: Vec<O2L> = Vec::new();
    let mut verdicts: Vec<BTreeMap<Round, V>> = Vec::new();
    for k in 1..=3u32 {
        let restart = k == 3 && arm.restart_n3;
        let mut l = if restart { restarted(&fx) } else { O2L::fresh(arm.kind, arm.mut_f) };
        let from = if restart { fx.f + 1 } else { 1 };
        let mut vm = BTreeMap::new();
        for e in &log[(from - 1) as usize..] {
            if let Some(v) = l.apply(e) {
                vm.insert(e.round, v);
            }
        }
        nodes.push(l);
        verdicts.push(vm);
    }

    let c1 = [0, 1, 2].map(|k| {
        fx.t1_ids.iter().chain(fx.tail_ids.iter()).filter(|id| nodes[k].is_live(**id)).count() as u64
    });
    let c2 = if arm.failover {
        (0..3)
            .map(|k| {
                (1..=O2_S).filter(|i| !nodes[k].is_live(cid(2, *i))).count() as u64
                    + u64::from(!nodes[k].is_live(cid(1, 1_000_000_000)))
            })
            .max()
            .unwrap()
    } else {
        0
    };
    let rounds: BTreeSet<Round> = verdicts.iter().flat_map(|m| m.keys().copied()).collect();
    let c3 = rounds
        .iter()
        .filter(|r| {
            let a = verdicts[0].get(r);
            a != verdicts[1].get(r) || a != verdicts[2].get(r)
        })
        .count() as u64;
    let mut all_ids: Vec<u64> = fx.t1_ids.iter().chain(fx.tail_ids.iter()).chain(fx.merged_ids.iter()).copied().collect();
    if arm.failover {
        all_ids.extend((1..=O2_S).map(|i| cid(2, i)));
        all_ids.push(cid(1, 1_000_000_000));
    }
    let c4 = all_ids
        .iter()
        .filter(|id| {
            let a = nodes[0].is_live(**id);
            a != nodes[1].is_live(**id) || a != nodes[2].is_live(**id)
        })
        .count() as u64;
    O2Out { c0, c1, c2, c3, c4, anti_vacuity }
}

fn mode_o2(n: u64) {
    assert!(n % 10 == 0 && n >= 1000);
    let head = O2Kind::Head;
    let m1 = O2Kind::Copy { m1: true, m2: false };
    let m2 = O2Kind::Copy { m1: false, m2: true };
    let m12 = O2Kind::Copy { m1: true, m2: true };
    let a = |name, kind, barrier, sweeper, restart_n3| O2Arm { name, kind, barrier, sweeper, restart_n3, failover: true, mut_f: false, mut_s: false };
    let h = n / 2;
    // (arm, predicted C0, C1 [n1,n2,n3], C2, C3, C4) -- O2 §6.3, from the constants N, K = 64
    let rows: Vec<(O2Arm, u64, [u64; 3], u64, u64, u64)> = vec![
        (a("HEAD n2/none", head, false, 2, false), n + 1, [64, 64, 64], 0, 0, 0),
        (a("HEAD n2/n3-restarted", head, false, 2, true), n + 1, [64, 64, 64], 0, 1, 0),
        (a("HEAD n3-restarted-sweeps", head, false, 3, true), h, [h + 65, h + 65, 64], 0, 1, h + 1),
        (a("HEAD+barrier n2/none", head, true, 2, false), n + 65, [0, 0, 0], 0, 0, 0),
        (a("HEAD+barrier n3-sweeps", head, true, 3, true), h + 64, [h + 1, h + 1, 0], 0, 1, h + 1),
        (a("M1 n3-sweeps", m1, false, 3, true), 1, [0, 0, 0], 0, 1, 0),
        (a("M2 n3-sweeps", m2, false, 3, true), n + 1, [64, 64, 64], 0, 0, 0),
        (a("M1+M2 n2-sweeps", m12, false, 2, true), 1, [0, 0, 0], 0, 0, 0),
        (a("M1+M2 n3-sweeps", m12, false, 3, true), 1, [0, 0, 0], 0, 0, 0),
    ];
    for (arm, c0, c1, c2, c3, c4) in rows {
        let t0 = Instant::now();
        let o = o2_run(n, arm);
        let ok = o.c0 == c0 && o.c1 == c1 && o.c2 == c2 && o.c3 == c3 && o.c4 == c4;
        println!(
            "O2 N={n} arm=\"{}\" C0={} C1={:?} C2={} C3={} C4={} anti_vacuity={} ms={:.0}",
            arm.name, o.c0, o.c1, o.c2, o.c3, o.c4, o.anti_vacuity, t0.elapsed().as_secs_f64() * 1e3
        );
        check("O2-row", arm.name, &format!("C0={c0} C1={c1:?} C2={c2} C3={c3} C4={c4}"), ok, format!("C0={} C1={:?} C2={} C3={} C4={}", o.c0, o.c1, o.c2, o.c3, o.c4));
        if !arm.barrier {
            check("O2-anti-vacuity", arm.name, "tail in log, absent from sweeper ledger", o.anti_vacuity, o.anti_vacuity.to_string());
        }
    }
    let mut f = a("mutant-F (M1+M2, fence ignores position)", m12, false, 3, true);
    f.mut_f = true;
    let o = o2_run(n, f);
    check("O2-mutF", f.name, "C2=1", o.c2 == 1, o.c2.to_string());
    let mut s = a("mutant-S (M1+M2, snapshot without last_base_move)", m12, false, 3, true);
    s.mut_s = true;
    let o = o2_run(n, s);
    check("O2-mutS", s.name, "C3=1", o.c3 == 1, o.c3.to_string());
    let mut nc = a("negative control (HEAD, no failover, no restart)", head, false, 2, false);
    nc.failover = false;
    let o = o2_run(n, nc);
    check("O2-negctl", nc.name, "C0=0 C2=0 C3=0 C4=0", o.c0 == 0 && o.c2 == 0 && o.c3 == 0 && o.c4 == 0, format!("C0={} C2={} C3={} C4={}", o.c0, o.c2, o.c3, o.c4));
}

// ---------------------------------------------------------------------------------------------
// Timed: P1, P1-fire, A-S, A-Z (under lockrun)
// ---------------------------------------------------------------------------------------------

const B: u64 = 256;
const REPS: usize = 5;

/// Fill for P1: T forks, the first 9T/10 disposed as in W, then a pool of 2B live forks.
fn p1_fill(l: &mut dyn Ledger, t: u64) -> (Round, u64) {
    let mut round = run_w(t, t - t * 9 / 10, |e| {
        l.apply(&e);
    });
    let pool_start = t + 1;
    for i in 0..2 * B {
        round += 1;
        l.apply(&Entry { term: 1, round, command: fork_c(cid(1, pool_start + i)) });
    }
    (round, pool_start)
}

fn time_batch(l: &mut dyn Ledger, round: &mut Round, cmds: impl Iterator<Item = Command>, fire: Option<&Arc<Mutex<BranchLedger>>>) -> f64 {
    let batch: Vec<Command> = cmds.collect();
    let n = batch.len();
    let t0 = Instant::now();
    for c in batch {
        *round += 1;
        l.apply(&Entry { term: 1, round: *round, command: c });
        if let Some(h) = fire {
            std::hint::black_box(lock(h).live_owned_by(NodeId(1)).len());
        }
    }
    t0.elapsed().as_nanos() as f64 / n as f64
}

fn mode_timed() {
    // P1: arms interleaved ABBA across reps.
    let arms = ["HEAD", "ZK", "M1"];
    for &t in &[1_000u64, 10_000_000] {
        for rep in 0..REPS {
            let order: Vec<&str> = if rep % 2 == 0 { arms.to_vec() } else { arms.iter().rev().copied().collect() };
            for arm in order {
                let hl = Arc::new(Mutex::new(BranchLedger::new()));
                let mut l: Box<dyn Ledger> = match arm {
                    "HEAD" => Box::new(Head(hl.clone())),
                    "ZK" => Box::new(Zk::new()),
                    _ => Box::new(HeadCopy::new(true)),
                };
                let (mut round, pool) = p1_fill(l.as_mut(), t);
                let fork_base = pool + 2 * B;
                let ns_fork = time_batch(l.as_mut(), &mut round, (0..B).map(|i| fork_c(cid(1, fork_base + i))), None);
                let ns_merge = {
                    let mut r = round;
                    let batch: Vec<(u64, Round)> = (0..B).map(|i| (cid(1, pool + i), 0)).collect();
                    let t0 = Instant::now();
                    for (id, _) in batch {
                        let base = r;
                        r += 1;
                        l.apply(&Entry { term: 1, round: r, command: merge_c(id, base) });
                    }
                    let ns = t0.elapsed().as_nanos() as f64 / B as f64;
                    round = r;
                    ns
                };
                let ns_abandon = time_batch(l.as_mut(), &mut round, (0..B).map(|i| abandon_c(cid(1, pool + B + i))), None);
                let ns_tick = time_batch(l.as_mut(), &mut round, (0..B).map(|i| Command::LeaseTick { unix_millis: i }), None);
                println!("TIMED id=P1 arm={arm} T={t} rep={rep} batch={B} records={} ns_fork={ns_fork:.1} ns_merge={ns_merge:.1} ns_abandon={ns_abandon:.1} ns_leasetick={ns_tick:.1}", l.records());
            }
            // OFL: merges and a base-moving other; no T.
            let mut o = Ofl::default();
            let t0 = Instant::now();
            for i in 0..B {
                o.apply(i + 1, &OCmd::Merge { branch: cid(1, i + 1), base_round: i, inc: 0 });
            }
            let ns_m = t0.elapsed().as_nanos() as f64 / B as f64;
            let t0 = Instant::now();
            for i in 0..B {
                o.apply(B + i + 1, &OCmd::Other { moves: false });
            }
            let ns_o = t0.elapsed().as_nanos() as f64 / B as f64;
            println!("TIMED id=P1 arm=OFL T={t} rep={rep} batch={B} ns_merge={ns_m:.1} ns_leasetick={ns_o:.1}");
        }
    }
    // P1-fire: a planted linear term inside the timed loop (HEAD), T = 10^3 vs 10^6.
    for &t in &[1_000u64, 1_000_000] {
        for rep in 0..REPS {
            let hl = Arc::new(Mutex::new(BranchLedger::new()));
            let mut l = Head(hl.clone());
            let (mut round, pool) = p1_fill(&mut l, t);
            let ns = time_batch(&mut l, &mut round, (0..B).map(|i| abandon_c(cid(1, pool + i))), Some(&hl));
            println!("TIMED id=P1-fire arm=HEAD+planted-scan T={t} rep={rep} batch={B} ns_abandon={ns:.1}");
        }
    }
    // A-S: orphans_of at fixed N = 10^3, T = 10^3 vs 10^6.
    for &t in &[1_000u64, 1_000_000] {
        for rep in 0..REPS {
            let hl = Arc::new(Mutex::new(BranchLedger::new()));
            let mut l = Head(hl.clone());
            run_w(t, 1_000, |e| {
                l.apply(&e);
            });
            let t0 = Instant::now();
            let o = lock(&hl).orphans_of(NodeId(1));
            let ns = t0.elapsed().as_nanos();
            println!("TIMED id=A-S arm=HEAD N=1000 T={t} rep={rep} orphans={} ns={ns}", o.len());
        }
    }
    // A-Z: ZK's close-owner apply, and M1's fence apply, at N = 10^3 vs 10^6.
    for &n in &[1_000u64, 1_000_000] {
        for rep in 0..REPS {
            for arm in ["ZK", "M1"] {
                let mut l: Box<dyn Ledger> = if arm == "ZK" { Box::new(Zk::new()) } else { Box::new(HeadCopy::new(true)) };
                let last = run_w(n, n, |e| {
                    l.apply(&e);
                });
                let t0 = Instant::now();
                l.apply(&Entry { term: 2, round: last + 1, command: abandon_c(sentinel(1)) });
                let ns = t0.elapsed().as_nanos();
                println!("TIMED id=A-Z arm={arm} N={n} rep={rep} ns_single_apply={ns} touched={} live_after={}", l.touched_max(), l.live_ids_of(1).len());
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// adm: what a batch of concurrent merges admits. After OFL and group commit the log carries only
// merges, so this is O3's ceiling. HEAD: the real ledger's scalar `last_base_move` rule. PK: the
// published per-key certification (Database State Machine, Pedone et al. 1999; Hyder), in which a
// merge is Applied iff no key it read was written after its base. Each merge reads and writes one
// key drawn from K keys; every merge in a batch carries the head before the batch as its base.
// ---------------------------------------------------------------------------------------------

fn mode_adm() {
    let rounds = 64u64;
    for &k in &[1u64, 16, 256, 1024] {
        // HEAD, the real ledger
        let ledger = Arc::new(Mutex::new(BranchLedger::new()));
        let mut head = Head(ledger.clone());
        let mut round = 0;
        for i in 1..=k * rounds {
            round += 1;
            head.apply(&Entry { term: 1, round, command: fork_c(cid(1, i)) });
        }
        let mut applied = Vec::new();
        for b in 0..rounds {
            let base = round;
            let mut a = 0u64;
            for j in 0..k {
                round += 1;
                if head.apply(&Entry { term: 1, round, command: merge_c(cid(1, b * k + j + 1), base) }) == Some(V::Applied) {
                    a += 1;
                }
            }
            applied.push(a);
        }
        let (mn, mx) = (applied.iter().min().unwrap(), applied.iter().max().unwrap());
        println!("ADM arm=HEAD k={k} batches={rounds} applied_per_batch_min={mn} max={mx}");
        check("ADM-HEAD", "HEAD", "exactly 1 per batch", *mn == 1 && *mx == 1, format!("{mn}..{mx}"));
        // PK, per-key certification
        for &keys in &[10u64, 1_000, 1_000_000] {
            let mut rng = Rng(0xAD0 ^ k ^ keys);
            let mut last_write: BTreeMap<u64, Round> = BTreeMap::new();
            let mut round = 0;
            let mut total = 0u64;
            let mut expect = 0.0f64;
            for _ in 0..rounds {
                let base = round;
                for _ in 0..k {
                    round += 1;
                    let key = rng.below(keys);
                    if last_write.get(&key).copied().unwrap_or(0) <= base {
                        last_write.insert(key, round);
                        total += 1;
                    }
                }
                expect += keys as f64 * (1.0 - (1.0 - 1.0 / keys as f64).powf(k as f64));
            }
            let per = total as f64 / rounds as f64;
            let exp = expect / rounds as f64;
            println!("ADM arm=PK k={k} keys={keys} batches={rounds} applied_per_batch_mean={per:.2} birthday_expectation={exp:.2}");
            check("ADM-PK", "PK", &format!("within 10% of {exp:.2}"), (per - exp).abs() <= 0.1 * exp.max(1.0), format!("{per:.2}"));
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Item 3: retire-at-snapshot for M1 (Raft ch. 5 compaction plus retirement of dead records),
// with the fence map persisted. PREREG A13.
// ---------------------------------------------------------------------------------------------

fn mode_retire(t: u64, n: u64, n2: u64, wm: bool) {
    assert!(n <= t);
    println!("# retire T={t} N={n} N2={n2} wm={wm}");
    if wm {
        // A18 (R): the real HEAD ledger over the same trace, measured, so its figure is not inferred.
        let h0 = heap();
        let ledger = Arc::new(Mutex::new(BranchLedger::new()));
        let mut head = Head(ledger);
        let mut round = run_w(t, n, |e| {
            head.apply(&e);
        });
        for i in 1..=n2 {
            round += 1;
            head.apply(&Entry { term: 1, round, command: fork_c(cid(2, i)) });
        }
        round += 1;
        head.apply(&Entry { term: 2, round, command: abandon_c(sentinel(1)) });
        let h1 = heap();
        let recs = head.records();
        println!("RETIRE-HEAD T={t} N={n} N2={n2} records={recs} heap_bytes={} bytes_per_record={:.1}", h1 - h0, (h1 - h0) as f64 / recs as f64);
        check("R-HEAD-heap", "HEAD", "1.25e9..1.45e9 bytes", (1.25e9..=1.45e9).contains(&((h1 - h0) as f64)), (h1 - h0).to_string());
    }
    let h0 = heap();
    let mut m1 = HeadCopy::new(true);
    m1.use_wm = wm;
    let mut round = run_w(t, n, |e| {
        m1.apply(&e);
    });
    for i in 1..=n2 {
        round += 1;
        m1.apply(&Entry { term: 1, round, command: fork_c(cid(2, i)) });
    }
    round += 1;
    m1.apply(&Entry { term: 2, round, command: abandon_c(sentinel(1)) });
    let h1 = heap();
    let before = m1.records();
    let live1_before = m1.live_ids_of(1).len();
    let live2_before = m1.live_ids_of(2).len();
    let (snap, retired) = m1.snapshot_retire(u64::MAX, false);
    let h2 = heap();
    println!(
        "RETIRE T={t} N={n} N2={n2} records_before={before} records_after={} retired={retired} heap_ledger_bytes={} heap_snapshot_bytes={} live_n1={live1_before}->{} live_n2={live2_before}->{}",
        snap.records(),
        h1 - h0,
        h2 - h1,
        snap.live_ids_of(1).len(),
        snap.live_ids_of(2).len()
    );
    check("R-records-before", "M1", &format!("{}", t + n2 + 1), before as u64 == t + n2 + 1, before.to_string());
    check("R-records-after", "M1+retire", &format!("{}", n2 + 1), snap.records() as u64 == n2 + 1, snap.records().to_string());
    check("R-retired", "M1+retire", &t.to_string(), retired == t, retired.to_string());
    check("R-live", "M1+retire", &format!("n1 0, n2 {n2}"), snap.live_ids_of(1).is_empty() && snap.live_ids_of(2).len() as u64 == n2, format!("{} {}", snap.live_ids_of(1).len(), snap.live_ids_of(2).len()));
    let ratio = (h2 - h1) as f64 / (h1 - h0) as f64;
    let pred = (n2 + 1) as f64 / (t + n2 + 1) as f64;
    check("R-heap", "M1+retire", &format!("snapshot/ledger within 20% of {pred:.4}"), (ratio / pred - 1.0).abs() <= 0.2, format!("{ratio:.4}"));
    // The same tail through the unretired ledger and the restored snapshot must agree exactly.
    let mut a = m1;
    let mut b = snap;
    let mut tail = Vec::new();
    let base = round;
    for i in 1..=100u64 {
        tail.push(merge_c(cid(2, i), base)); // the first applies, the rest re-evaluate
    }
    tail.push(merge_c(cid(1, t), base)); // a zombie merge of a fenced n1 branch
    tail.push(merge_c(cid(1, 1), base)); // a merge of a merged or abandoned n1 branch
    tail.push(fork_c(cid(1, 2))); // a fork of a retired id: HEAD refuses it as a collision
    tail.push(fork_c(cid(1, t + 1))); // n1's new work after the fence: live
    tail.push(merge_c(cid(1, t + 1), base + 104)); // and its merge
    tail.push(abandon_c(cid(1, 3))); // an abandon of a retired id: refused
    let mut div = 0u64;
    for c in tail {
        round += 1;
        let e = Entry { term: 3, round, command: c };
        let (ra, rb) = (a.rejected, b.rejected);
        let (va, vb) = (a.apply(&e), b.apply(&e));
        if va != vb || (a.rejected - ra) != (b.rejected - rb) {
            div += 1;
        }
    }
    let ids: Vec<u64> = (1..=100).map(|i| cid(2, i)).chain([cid(1, 1), cid(1, 2), cid(1, 3), cid(1, t), cid(1, t + 1)]).collect();
    div += ids.iter().filter(|id| a.is_live(**id) != b.is_live(**id)).count() as u64;
    check("R-tail", "M1 vs M1+retire", "0 divergences", div == 0, div.to_string());
}

fn mode_s6(n: u64, budget: u64, wm: bool) {
    println!("# s6 N={n} budget={budget} wm={wm}");
    let mut m1 = HeadCopy::new(true);
    m1.use_wm = wm;
    let mut round = 0;
    for i in 1..=n {
        round += 1;
        m1.apply(&Entry { term: 1, round, command: fork_c(cid(1, i)) });
    }
    round += 1;
    m1.apply(&Entry { term: 2, round, command: abandon_c(sentinel(1)) });
    // The floor is ABOVE the fence (the case O2's fixture never reached).
    for arm in ["correct", "mutant-drop-fence"] {
        for b in [budget, u64::MAX] {
            let (snap, retired) = m1.snapshot_retire(b, arm != "correct");
            let live = snap.live_ids_of(1).len() as u64;
            let bs = if b == u64::MAX { "all".to_string() } else { b.to_string() };
            println!("S6 arm={arm} budget={bs} retired={retired} n1_live_after_restore={live}");
            match (arm, b == u64::MAX) {
                ("correct", _) => check("S6-correct", arm, "0", live == 0, live.to_string()),
                (_, false) => check("S6-mutant-caught", arm, &format!("{}", n - budget.min(n)), live == n - budget.min(n), live.to_string()),
                (_, true) => println!("S6 note: after a FULL retire the mutant leaves {live} live; the fence map is redundant only when every fenced record was retired"),
            }
        }
    }
}

/// A HEAD-protocol log that reaches every apply path, including the refusals.
fn gen_wide(seed: u64, len: usize) -> Vec<Command> {
    let mut rng = Rng(seed ^ 0x51DE);
    let mut next = [0u64; 4];
    let mut known: Vec<u64> = Vec::new();
    let mut out = Vec::with_capacity(len);
    let mut wal = 0u64;
    while out.len() < len {
        let r = rng.below(1000);
        let o = 1 + rng.below(3) as u32;
        let pick = |rng: &mut Rng, known: &Vec<u64>| -> u64 { if known.is_empty() { 0 } else { known[rng.below(known.len() as u64) as usize] } };
        let c = if r < 350 || known.is_empty() {
            next[o as usize] += 1;
            let id = cid(o, next[o as usize]);
            let parent = if rng.below(5) == 0 { pick(&mut rng, &known) } else { 0 };
            known.push(id);
            Command::Branch { op: BranchOp::Fork { child: id, parent, fork_epoch: 1, lease_millis: 900_000 } }
        } else if r < 500 {
            let head = out.len() as Round;
            merge_c(pick(&mut rng, &known), head.saturating_sub(rng.below(20)))
        } else if r < 620 {
            abandon_c(pick(&mut rng, &known))
        } else if r < 660 {
            Command::Branch { op: BranchOp::Reap { branch: pick(&mut rng, &known), generation: 1 } }
        } else if r < 800 {
            wal += 1;
            wal_c(wal)
        } else if r < 815 {
            abandon_c(sentinel(o))
        } else if r < 825 {
            fork_c(0) // a fork whose child is the trunk
        } else if r < 840 {
            fork_c(pick(&mut rng, &known)) // a collision with a recorded id
        } else if r < 850 {
            Command::Branch { op: BranchOp::Fork { child: cid(o, 900_000_000 + rng.below(1 << 20)), parent: cid(3, 800_000_000), fork_epoch: 1, lease_millis: 1 } } // unknown parent
        } else if r < 860 {
            abandon_c(cid(o, 700_000_000 + rng.below(1 << 20))) // unknown
        } else if r < 870 {
            Command::Branch { op: BranchOp::Reap { branch: cid(o, 600_000_000 + rng.below(1 << 20)), generation: 1 } } // unknown
        } else if r < 885 {
            merge_c(cid(o, 500_000_000 + rng.below(1 << 20)), out.len() as Round) // unknown
        } else if r < 930 {
            Command::LeaseTick { unix_millis: out.len() as u64 }
        } else if r < 960 {
            Command::NoOp
        } else {
            Command::Checkpoint
        };
        out.push(c);
    }
    out
}

fn head_category(e: &BranchEffect) -> String {
    match e {
        BranchEffect::Other { base_moved } => format!("Other(base_moved={base_moved})"),
        BranchEffect::AlreadyApplied => "AlreadyApplied".into(),
        BranchEffect::Forked(_) => "Forked".into(),
        BranchEffect::Merged(MergeVerdict::Applied { .. }) => "Merged(Applied)".into(),
        BranchEffect::Merged(MergeVerdict::ReEvaluate { .. }) => "Merged(ReEvaluate)".into(),
        BranchEffect::Merged(MergeVerdict::Refused { why, .. }) => {
            if why.contains("no committed round created") { "Merged(Refused:unknown)".into() } else { "Merged(Refused:not-live)".into() }
        }
        BranchEffect::Abandoned(_) => "Abandoned".into(),
        BranchEffect::Reaped { .. } => "Reaped".into(),
        BranchEffect::Rejected { why } => {
            let k = if why.contains("child is the trunk") {
                "fork-trunk"
            } else if why.contains("already created") {
                "fork-collision"
            } else if why.contains("forks") && why.contains("which is") {
                "fork-parent-not-live"
            } else if why.contains("forks") {
                "fork-parent-unknown"
            } else if why.contains("abandons") && why.contains("which is") {
                "abandon-not-live"
            } else if why.contains("abandons") {
                "abandon-unknown"
            } else if why.contains("again") {
                "reap-twice"
            } else {
                "reap-unknown"
            };
            format!("Rejected({k})")
        }
    }
}

fn mode_conformwide(seed: u64) {
    let log: Vec<Entry> = gen_wide(seed, 200_000)
        .into_iter()
        .enumerate()
        .map(|(i, command)| Entry { term: 1, round: i as Round + 1, command })
        .collect();
    // CW1: the real ledger against the copy, entry by entry.
    let real = Arc::new(Mutex::new(BranchLedger::new()));
    let mut copy = HeadCopy::new(false);
    let mut cats: BTreeMap<String, u64> = BTreeMap::new();
    let mut div = 0u64;
    for (i, e) in log.iter().enumerate() {
        // Every 9,973rd entry is delivered twice, so the AlreadyApplied path is reached too.
        let times = if i % 9973 == 0 { 2 } else { 1 };
        for _ in 0..times {
            let eff = lock(&real).apply(e);
            *cats.entry(head_category(&eff)).or_default() += 1;
            let hv = match &eff {
                BranchEffect::Merged(v) => Some(class(v)),
                _ => None,
            };
            let hr = matches!(eff, BranchEffect::Rejected { .. });
            let r0 = copy.rejected;
            let cv = copy.apply(e);
            if hv != cv || hr != (copy.rejected > r0) {
                div += 1;
            }
        }
    }
    let ids: BTreeSet<u64> = lock(&real).all().map(|b| b.id.0).collect();
    let head = Head(real);
    div += ids.iter().filter(|id| head.is_live(**id) != copy.is_live(**id)).count() as u64;
    println!("CW1 seed={seed} entries={} HEAD_categories={} {:?}", log.len(), cats.len(), cats);
    check("CW1", "Copy", "0 divergences", div == 0, div.to_string());
    check("CW1-coverage", "trace", "18 of HEAD's 18 effect categories", cats.len() == 18, cats.len().to_string());
    // CW2: M1 with no snapshot against M1 restored from snapshots every 2,000 rounds (budget all,
    // and budget 50), on the same log. The drop-fence mutant (budget 50) must diverge.
    for (label, budget, drop) in [("all", u64::MAX, false), ("b50", 50, false), ("b50-drop-fence", 50, true)] {
        let mut plain = HeadCopy::new(true);
        let mut snapped = HeadCopy::new(true);
        // A14 diagnostic: the kind of every divergence, and for a fork, whether the id existed in the
        // plain ledger before it and whether its local id was at or below the snapped hwm.
        let mut kinds: BTreeMap<String, u64> = BTreeMap::new();
        // Divergences that matter: a merge verdict, a fork accepted by one and refused by the other,
        // or liveness. Refusals of an abandon or reap of a DEAD id may differ and are counted apart:
        // retirement discards the terminal state (Merged vs Reaped) that decides them.
        let mut d = 0u64;
        let mut refusal_only = 0u64;
        for (i, e) in log.iter().enumerate() {
            let fork_child = match &e.command {
                Command::Branch { op: BranchOp::Fork { child, .. } } => Some(*child),
                _ => None,
            };
            let existed = fork_child.map(|c| plain.br.contains_key(&c));
            let under_hwm = fork_child.map(|c| snapped.retired_hwm.get(&owner_of(c)).is_some_and(|h| c & LOCAL_MASK <= *h));
            let (pr, sr) = (plain.rejected, snapped.rejected);
            let (pv, sv) = (plain.apply(e), snapped.apply(e));
            let (pd, sd) = (plain.rejected - pr, snapped.rejected - sr);
            let is_fork = fork_child.is_some();
            if pv != sv || (is_fork && pd != sd) {
                d += 1;
                let k = if is_fork {
                    format!("fork(existed_in_plain={:?},local_le_snapped_hwm={:?},plain_refused={})", existed, under_hwm, pd > 0)
                } else {
                    format!("verdict(plain={pv:?},snapped={sv:?})")
                };
                *kinds.entry(k).or_default() += 1;
            } else if pd != sd {
                refusal_only += 1;
            }
            if (i + 1) % 2000 == 0 {
                snapped = snapped.snapshot_retire(budget, drop).0;
            }
        }
        println!("CW2-diag seed={seed} arm={label} divergence_kinds={kinds:?}");
        let all: BTreeSet<u64> = plain.br.keys().copied().collect();
        d += all.iter().filter(|id| plain.is_live(**id) != snapped.is_live(**id)).count() as u64;
        println!("CW2 seed={seed} arm={label} divergences={d} refusal_only_differences={refusal_only} records_plain={} records_snapped={}", plain.records(), snapped.records());
        if drop {
            check("CW2-negctl", label, ">=1", d >= 1, d.to_string());
        } else {
            check("CW2", label, "0 divergences", d == 0, d.to_string());
        }
    }
    // CW3 (A14 F-wm): the same comparison with the apply-time fork watermark in both ledgers.
    for (label, budget, drop_fence, drop_wm) in [
        ("wm-all", u64::MAX, false, false),
        ("wm-b50", 50, false, false),
        ("wm-b50-drop-fence", 50, true, false),
        ("wm-all-drop-wm", u64::MAX, false, true),
    ] {
        let mut plain = HeadCopy::new(true);
        plain.use_wm = true;
        let mut snapped = HeadCopy::new(true);
        snapped.use_wm = true;
        let mut d = 0u64;
        let mut refusal_only = 0u64;
        for (i, e) in log.iter().enumerate() {
            let (pr, sr) = (plain.rejected, snapped.rejected);
            let (pv, sv) = (plain.apply(e), snapped.apply(e));
            let (pd, sd) = (plain.rejected - pr, snapped.rejected - sr);
            let is_fork = matches!(&e.command, Command::Branch { op: BranchOp::Fork { .. } });
            if pv != sv || (is_fork && pd != sd) {
                d += 1;
            } else if pd != sd {
                refusal_only += 1;
            }
            if (i + 1) % 2000 == 0 {
                snapped = snapped.snapshot_retire2(budget, drop_fence, drop_wm).0;
            }
        }
        let all: BTreeSet<u64> = plain.br.keys().copied().collect();
        d += all.iter().filter(|id| plain.is_live(**id) != snapped.is_live(**id)).count() as u64;
        println!("CW3 seed={seed} arm={label} divergences={d} refusal_only_differences={refusal_only} records_plain={} records_snapped={}", plain.records(), snapped.records());
        if drop_fence || drop_wm {
            check("CW3-negctl", label, ">=1", d >= 1, d.to_string());
        } else {
            check("CW3", label, "0 divergences", d == 0, d.to_string());
        }
    }
    // The protocol change F-wm makes, stated: forks HEAD accepts that M1+wm refuses.
    let real = Arc::new(Mutex::new(BranchLedger::new()));
    let mut wm = HeadCopy::new(true);
    wm.use_wm = true;
    let mut changed = 0u64;
    for e in &log {
        let is_fork = matches!(&e.command, Command::Branch { op: BranchOp::Fork { .. } });
        let head_ok = matches!(lock(&real).apply(e), BranchEffect::Forked(_));
        let r0 = wm.rejected;
        let before = wm.br.len();
        wm.apply(e);
        let wm_ok = is_fork && wm.rejected == r0 && wm.br.len() > before;
        if is_fork && head_ok && !wm_ok {
            changed += 1;
        }
    }
    println!("CW3 seed={seed} forks_HEAD_accepts_that_M1+wm_refuses={changed} (the stated protocol change; includes forks M1's fence refuses)");
}

// ---------------------------------------------------------------------------------------------
// A17 (1): what makes one M1 fence apply cost more at larger N? Counters (allocator calls, minor
// faults) and two arms that remove the first-leaf allocation: PRE (the fence map already holds a
// no-op entry for node 9) and REPEAT (a second fence for the same owner).
// ---------------------------------------------------------------------------------------------

#[repr(C)]
#[derive(Default)]
struct RUsage {
    utime: [i64; 2],
    stime: [i64; 2],
    maxrss: i64,
    ixrss: i64,
    idrss: i64,
    isrss: i64,
    minflt: i64,
    majflt: i64,
    nswap: i64,
    inblock: i64,
    oublock: i64,
    msgsnd: i64,
    msgrcv: i64,
    nsignals: i64,
    nvcsw: i64,
    nivcsw: i64,
}

unsafe extern "C" {
    fn getrusage(who: i32, usage: *mut RUsage) -> i32;
}

fn minflt() -> i64 {
    let mut u = RUsage::default();
    // SAFETY: `u` is a live, correctly sized `struct rusage` for the duration of the call; RUSAGE_SELF = 0.
    let rc = unsafe { getrusage(0, &mut u) };
    assert_eq!(rc, 0, "getrusage failed");
    u.minflt
}

/// A18 (F): the A-Z heap history, exactly as 72fa4b8:1452-1465 ran it: ZK over run_w(N, N), ZK's close applied, ZK dropped;
/// then M1 over run_w(N, N), boxed as `dyn Ledger`. `pre` puts a no-op fence (owner 9, round 0) in before the build.
fn az_build(n: u64, pre: bool) -> (Box<dyn Ledger>, Round) {
    az_build_owner(n, if pre { Some(9) } else { None })
}

/// The A-Z history with an optional fence record pre-inserted at round 0 before the build: owner 9 is A18's PRE (an
/// allocation-free proxy); owner 1 is A19's OWN (CockroachDB's layout: the dead owner's own record exists and the fence
/// updates it in place). A fence at round 0 fences nothing, since every fork is at round >= 1.
fn az_build_owner(n: u64, pre_owner: Option<u32>) -> (Box<dyn Ledger>, Round) {
    {
        let mut z: Box<dyn Ledger> = Box::new(Zk::new());
        let last = run_w(n, n, |e| {
            z.apply(&e);
        });
        z.apply(&Entry { term: 2, round: last + 1, command: abandon_c(sentinel(1)) });
        std::hint::black_box(z.live_ids_of(1).len());
    }
    let mut m = HeadCopy::new(true);
    if let Some(o) = pre_owner {
        m.fence.insert(o, 0);
    }
    let mut l: Box<dyn Ledger> = Box::new(m);
    let last = run_w(n, n, |e| {
        l.apply(&e);
    });
    (l, last)
}

/// The size of a `BTreeMap<u32, Round>` leaf: parent pointer 8, parent index 2, length 2, 11 keys of 4 (padded to 56), 11
/// values of 8. ALLOC allocates and first-writes exactly this, with no fence apply.
const FENCE_LEAF_BYTES: usize = 144;

/// A18 (F). `timed` prints ns per arm (run in one lockrun hold); otherwise the same sequences print allocator-call and
/// minor-fault deltas around the operation, with no timer (unlocked).
fn mode_fz(reps: u64, timed: bool) {
    println!("# fz reps={reps} timed={timed} (A18 (F)); arms AZ, PRE, ALLOC rotated per rep; N in 1e3..1e6");
    let arms = ["AZ", "PRE", "ALLOC"];
    for &n in &[1_000u64, 10_000, 100_000, 1_000_000] {
        for rep in 0..reps {
            for k in 0..3 {
                let arm = arms[(k + rep as usize) % 3];
                let (mut l, last) = az_build(n, arm == "PRE");
                if timed {
                    let ns = if arm == "ALLOC" {
                        let lay = Layout::from_size_align(FENCE_LEAF_BYTES, 8).unwrap();
                        let t0 = Instant::now();
                        // SAFETY: a non-zero-size layout; the block is written in full and freed below.
                        let p = unsafe { std::alloc::alloc(lay) };
                        unsafe { std::ptr::write_bytes(p, 0, FENCE_LEAF_BYTES) };
                        let ns = t0.elapsed().as_nanos();
                        std::hint::black_box(p);
                        unsafe { std::alloc::dealloc(p, lay) };
                        ns
                    } else {
                        let t0 = Instant::now();
                        l.apply(&Entry { term: 2, round: last + 1, command: abandon_c(sentinel(1)) });
                        t0.elapsed().as_nanos()
                    };
                    println!("FZ arm={arm} N={n} rep={rep} ns={ns} live_after={}", l.live_ids_of(1).len());
                } else {
                    let e = Entry { term: 2, round: last + 1, command: abandon_c(sentinel(1)) };
                    let lay = Layout::from_size_align(FENCE_LEAF_BYTES, 8).unwrap();
                    let f0 = minflt();
                    let a0 = ALLOCS.load(Ordering::Relaxed);
                    if arm == "ALLOC" {
                        // SAFETY: as above.
                        let p = unsafe { std::alloc::alloc(lay) };
                        unsafe { std::ptr::write_bytes(p, 0, FENCE_LEAF_BYTES) };
                        std::hint::black_box(p);
                        let a1 = ALLOCS.load(Ordering::Relaxed);
                        let f1 = minflt();
                        unsafe { std::alloc::dealloc(p, lay) };
                        println!("FZC arm={arm} N={n} rep={rep} allocs={} minflt={}", a1 - a0, f1 - f0);
                    } else {
                        l.apply(&e);
                        let a1 = ALLOCS.load(Ordering::Relaxed);
                        let f1 = minflt();
                        println!("FZC arm={arm} N={n} rep={rep} allocs={} minflt={} live_after={}", a1 - a0, f1 - f0, l.live_ids_of(1).len());
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// A19 (X), PREREG.md (registered 2026-10-01T05:22:22Z, before any A18 (F) data): the fence apply on the SOTA layout (OWN)
// from 10^4 to 10^7, with a forced-cold bound, a TLB-cold arm, and two planted controls. A closing arm: it cannot yield SURV.
// ---------------------------------------------------------------------------------------------

/// 512 MiB, above the 16 MiB L2 and the SLC (sysctl hw.perflevel0.l2cachesize); written outside the timer.
const X_BUF: usize = 512 << 20;

fn mode_x(reps: u64, timed: bool) {
    println!("# x reps={reps} timed={timed} (A19 (X)); arms AZ, OWN, CACHE-COLD, TLB-COLD, SQRT-PLANT, LIN-PLANT rotated per rep");
    // Allocated and pre-faulted once, so the cold passes write resident memory and fault nothing.
    let mut buf = vec![0u8; X_BUF];
    for i in (0..X_BUF).step_by(4096) {
        buf[i] = 1;
    }
    let arms = ["AZ", "OWN", "CACHE-COLD", "TLB-COLD", "SQRT-PLANT", "LIN-PLANT"];
    for &n in &[10_000u64, 100_000, 1_000_000, 3_000_000, 10_000_000] {
        for rep in 0..reps {
            for k in 0..arms.len() {
                let arm = arms[(k + rep as usize) % arms.len()];
                // The plants are measured where the falsifier reads: 10^6 and 10^7.
                if arm.ends_with("PLANT") && n != 1_000_000 && n != 10_000_000 {
                    continue;
                }
                let (mut l, last) = az_build_owner(n, if arm == "AZ" { None } else { Some(1) });
                match arm {
                    // Every 128-B line: evicts the caches.
                    "CACHE-COLD" => {
                        for i in (0..X_BUF).step_by(128) {
                            buf[i] = buf[i].wrapping_add(1);
                        }
                    }
                    // One write per 16 KiB page: flushes the TLB, touches at most 1/128 of the sets.
                    "TLB-COLD" => {
                        for i in (0..X_BUF).step_by(16384) {
                            buf[i] = buf[i].wrapping_add(1);
                        }
                    }
                    _ => {}
                }
                std::hint::black_box(&buf);
                let walk: u64 = match arm {
                    "SQRT-PLANT" => (n as f64).sqrt().ceil() as u64,
                    "LIN-PLANT" => n,
                    _ => 0,
                };
                if timed {
                    let t0 = Instant::now();
                    let mut hits = 0u64;
                    for i in 1..=walk {
                        if l.is_live(cid(1, i)) {
                            hits += 1;
                        }
                    }
                    l.apply(&Entry { term: 2, round: last + 1, command: abandon_c(sentinel(1)) });
                    let ns = t0.elapsed().as_nanos();
                    std::hint::black_box(hits);
                    println!("X arm={arm} N={n} rep={rep} ns={ns} walk={walk} live_after={}", l.live_ids_of(1).len());
                } else {
                    let e = Entry { term: 2, round: last + 1, command: abandon_c(sentinel(1)) };
                    let f0 = minflt();
                    let a0 = ALLOCS.load(Ordering::Relaxed);
                    let mut hits = 0u64;
                    for i in 1..=walk {
                        if l.is_live(cid(1, i)) {
                            hits += 1;
                        }
                    }
                    l.apply(&e);
                    let a1 = ALLOCS.load(Ordering::Relaxed);
                    let f1 = minflt();
                    std::hint::black_box(hits);
                    println!("XC arm={arm} N={n} rep={rep} allocs={} minflt={} walk={walk} live_after={}", a1 - a0, f1 - f0, l.live_ids_of(1).len());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// A17 (2): the liveness heartbeat in the model (CockroachDB node liveness), against plain M1.
// ---------------------------------------------------------------------------------------------

const HB_K: u64 = 10_000;
const HB_H: u64 = 1_000;
const HB_TTL: u64 = 4_500;
const HB_MO: u64 = 500;

fn hb_c(n: u32, e: u64, exp: u64) -> Command {
    Command::Branch { op: BranchOp::Fork { child: cid(n, LOCAL_MASK - 1), parent: 0, fork_epoch: e, lease_millis: exp } }
}
fn fence_ie_c(n: u32, e: u64, t: u64) -> Command {
    Command::Branch { op: BranchOp::Merge { branch: sentinel(n), base_round: (t << 16) | e } }
}
fn fork_e(child: u64, e: u64) -> Command {
    Command::Branch { op: BranchOp::Fork { child, parent: 0, fork_epoch: e, lease_millis: 900_000 } }
}

struct HbOut {
    live_n1: u64,
    fence_applied: u64,
    fence_refused: u64,
    hb_refused: u64,
    fork_refused: u64,
    zombie_live: bool,
    rejoin_live: bool,
}

/// One scenario on one arm. `hb` false = plain M1 (its fence is the Abandon sentinel, and it has no
/// heartbeats). Times are model units; the heartbeat interval is HB_H and a record lives HB_TTL.
fn hb_scenario(scen: &str, hb: bool, skew: u64, muts: (bool, bool, bool)) -> HbOut {
    let mut l = HeadCopy::new(true);
    if hb {
        l.hb = Some(Hb {
            max_offset: HB_MO,
            mut_fence_ignores_liveness: muts.0,
            mut_hb_ignores_epoch: muts.1,
            mut_fork_ignores_epoch: muts.2,
            ..Hb::default()
        });
    }
    let mut cmds: Vec<Command> = Vec::new();
    if hb {
        cmds.push(hb_c(1, 1, HB_TTL));
    }
    for i in 1..=HB_K {
        cmds.push(fork_e(cid(1, i), 1));
    }
    // n1 heartbeats at t = 1000, 2000, 3000 (then, in L2/L3, it stops).
    if hb {
        for t in [HB_H, 2 * HB_H, 3 * HB_H] {
            cmds.push(hb_c(1, 1, t + HB_TTL));
        }
    }
    let fence = |t: u64| if hb { fence_ie_c(1, 1, t) } else { abandon_c(sentinel(1)) };
    match scen {
        // Live: its record (expiration 7500) is far ahead of the fence's clock.
        "L1" => cmds.push(fence(3_500 + skew)),
        // Dead since t = 3000: expiration 7500 + max_offset 500 < 8001.
        "L2" => cmds.push(fence(8_001 + skew)),
        "L3" => {
            cmds.push(fence(8_001 + skew));
            // The zombie resumes at t = 9000 with its OLD epoch: a heartbeat and a fork.
            if hb {
                cmds.push(hb_c(1, 1, 9_000 + HB_TTL));
            }
            cmds.push(fork_e(cid(1, HB_K + 1), 1));
            // It reads its record, learns epoch 2, heartbeats and forks again.
            if hb {
                cmds.push(hb_c(1, 2, 9_500 + HB_TTL));
            }
            cmds.push(fork_e(cid(1, HB_K + 2), 2));
        }
        // Worst moment: the fence at true time 3999, one unit before the next heartbeat was due.
        "L4" => cmds.push(fence(3_999 + skew)),
        // A18 (H) L5: a driver stall. n1 is silent from its last heartbeat (t = 3000) for S = `skew` units, and the fence
        // comes at the end of the stall.
        "L5" => cmds.push(fence(3_000 + skew)),
        // A18 (H) L3m: L3 with a second fence right after the zombie's stale heartbeat. Correct: it applies (n1 never
        // heartbeated in epoch 2, expiration 7500 + 500 < 9600). Under HB-m2 the stale heartbeat extended the expiration to
        // 13500, so it is refused: caught by the fence OUTCOME, not by the counter the mutant disables.
        "L3m" => {
            cmds.push(fence(8_001));
            cmds.push(hb_c(1, 1, 9_000 + HB_TTL));
            cmds.push(fence_ie_c(1, 2, 9_600));
        }
        other => panic!("unknown scenario {other}"),
    }
    for (i, c) in cmds.into_iter().enumerate() {
        l.apply(&Entry { term: 1, round: i as u64 + 1, command: c });
    }
    let h = l.hb.clone().unwrap_or_default();
    HbOut {
        live_n1: l.live_ids_of(1).iter().filter(|id| **id & LOCAL_MASK <= HB_K).count() as u64,
        fence_applied: h.fence_applied,
        fence_refused: h.fence_refused,
        hb_refused: h.hb_refused,
        fork_refused: h.fork_epoch_refused,
        zombie_live: l.is_live(cid(1, HB_K + 1)),
        rejoin_live: l.is_live(cid(1, HB_K + 2)),
    }
}

fn hb_line(scen: &str, arm: &str, skew: u64, o: &HbOut) {
    println!(
        "HB scen={scen} arm={arm} skew={skew} pre_fence_live_n1={} fence_applied={} fence_refused={} hb_refused={} fork_epoch_refused={} zombie_fork_live={} rejoin_fork_live={}",
        o.live_n1, o.fence_applied, o.fence_refused, o.hb_refused, o.fork_refused, o.zombie_live, o.rejoin_live
    );
}

fn mode_hb() {
    println!("# hb (A17 (2), A17a): K={HB_K} H={HB_H} TTL={HB_TTL} max_offset={HB_MO}");
    let none = (false, false, false);
    // L1: a live node loses office.
    let h = hb_scenario("L1", true, 0, none);
    hb_line("L1", "HB", 0, &h);
    check("HB-L1", "HB", "fence refused, 10000 live", h.fence_refused == 1 && h.fence_applied == 0 && h.live_n1 == HB_K, format!("refused={} live={}", h.fence_refused, h.live_n1));
    let m = hb_scenario("L1", false, 0, none);
    hb_line("L1", "M1", 0, &m);
    check("HB-L1-M1-harm", "M1", "0 live (10000 live branches discarded)", m.live_n1 == 0, m.live_n1.to_string());
    // L2: a dead node.
    let h = hb_scenario("L2", true, 0, none);
    hb_line("L2", "HB", 0, &h);
    check("HB-L2", "HB", "fence applied, 0 live", h.fence_applied == 1 && h.live_n1 == 0, format!("applied={} live={}", h.fence_applied, h.live_n1));
    let m = hb_scenario("L2", false, 0, none);
    hb_line("L2", "M1", 0, &m);
    check("HB-L2-M1", "M1", "0 live", m.live_n1 == 0, m.live_n1.to_string());
    // L3: the zombie.
    let h = hb_scenario("L3", true, 0, none);
    hb_line("L3", "HB", 0, &h);
    check(
        "HB-L3",
        "HB",
        "stale hb refused 1, old-epoch fork refused 1, zombie dead, rejoin live, pre-fence 0 live",
        h.hb_refused == 1 && h.fork_refused == 1 && !h.zombie_live && h.rejoin_live && h.live_n1 == 0,
        format!("hb_refused={} fork_refused={} zombie={} rejoin={} pre={}", h.hb_refused, h.fork_refused, h.zombie_live, h.rejoin_live, h.live_n1),
    );
    let m = hb_scenario("L3", false, 0, none);
    hb_line("L3", "M1", 0, &m);
    check("HB-L3-M1", "M1", "zombie fork live (M1 accepts it after the fence)", m.zombie_live && m.rejoin_live && m.live_n1 == 0, format!("zombie={} rejoin={} pre={}", m.zombie_live, m.rejoin_live, m.live_n1));
    // L4: skew.
    for s in [0u64, 500, 1_000, 4_001, 4_002, 5_000] {
        let h = hb_scenario("L4", true, s, none);
        hb_line("L4", "HB", s, &h);
        let want = u64::from(s >= 4_002);
        check(&format!("HB-L4-s{s}"), "HB", &format!("live nodes fenced = {want}"), h.fence_applied == want, h.fence_applied.to_string());
    }
    // Mutants: each must be caught.
    let x = hb_scenario("L1", true, 0, (true, false, false));
    hb_line("L1", "HB-m1", 0, &x);
    check("HB-m1-caught", "HB-m1 fence ignores liveness", "0 live (the live node is fenced)", x.live_n1 == 0, x.live_n1.to_string());
    let x = hb_scenario("L3", true, 0, (false, true, false));
    hb_line("L3", "HB-m2", 0, &x);
    check("HB-m2-caught", "HB-m2 heartbeat ignores epoch", "stale hb refused 0", x.hb_refused == 0, x.hb_refused.to_string());
    let x = hb_scenario("L3", true, 0, (false, false, true));
    hb_line("L3", "HB-m3", 0, &x);
    check("HB-m3-caught", "HB-m3 fork ignores epoch", "zombie fork live", x.zombie_live, x.zombie_live.to_string());
    // A18 (H): L5 stalls, and the outcome-based HB-m2 check.
    for s in [2_000u64, 4_000, 6_000] {
        let h = hb_scenario("L5", true, s, none);
        hb_line("L5", "HB", s, &h);
        let fenced = s > HB_TTL + HB_MO;
        check(
            &format!("HB-L5-s{s}"),
            "HB",
            if fenced { "fence applied, 0 live" } else { "fence refused, 10000 live" },
            if fenced { h.fence_applied == 1 && h.live_n1 == 0 } else { h.fence_refused == 1 && h.live_n1 == HB_K },
            format!("applied={} refused={} live={}", h.fence_applied, h.fence_refused, h.live_n1),
        );
    }
    let m = hb_scenario("L5", false, 2_000, none);
    hb_line("L5", "M1", 2_000, &m);
    check("HB-L5-M1-harm", "M1", "0 live (a 2000-unit stall costs the live node its branches)", m.live_n1 == 0, m.live_n1.to_string());
    let h = hb_scenario("L3m", true, 0, none);
    hb_line("L3m", "HB", 0, &h);
    check("HB-L3m", "HB", "second fence applied (2 applied)", h.fence_applied == 2, h.fence_applied.to_string());
    let x = hb_scenario("L3m", true, 0, (false, true, false));
    hb_line("L3m", "HB-m2", 0, &x);
    check("HB-m2-caught-by-outcome", "HB-m2 heartbeat ignores epoch", "second fence refused (1 applied)", x.fence_applied == 1, x.fence_applied.to_string());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize| -> u64 { args.get(i).and_then(|s| s.parse().ok()).expect("numeric argument") };
    println!("# r11_dist_ledger {:?} size_of::<Entry>={} size_of::<Command>={}", &args[1..], std::mem::size_of::<Entry>(), std::mem::size_of::<Command>());
    let t0 = Instant::now();
    match args.get(1).map(|s| s.as_str()) {
        Some("counts") => mode_counts(arg(2), arg(3)),
        Some("p6") => mode_p6(),
        Some("p8") => mode_p8(arg(2)),
        Some("o2") => mode_o2(arg(2)),
        Some("timed") => mode_timed(),
        Some("adm") => mode_adm(),
        Some("retire") => mode_retire(arg(2), arg(3), arg(4), args.get(5).map(|x| x == "wm").unwrap_or(false)),
        Some("s6") => mode_s6(arg(2), arg(3), args.get(4).map(|x| x == "wm").unwrap_or(false)),
        Some("conformwide") => mode_conformwide(arg(2)),
        Some("fz") => mode_fz(arg(2), args.get(3).map(|x| x == "timed").unwrap_or(false)),
        Some("x") => mode_x(arg(2), args.get(3).map(|x| x == "timed").unwrap_or(false)),
        Some("hb") => mode_hb(),
        _ => {
            eprintln!("usage: r11_dist_ledger counts <T> <N> | p6 | p8 <seed> | o2 <N> | timed");
            std::process::exit(2);
        }
    }
    println!("# done wall_ms={:.0} heap_live_end={}", t0.elapsed().as_secs_f64() * 1e3, heap());
}
