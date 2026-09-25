//! r11-dist shared arms for part B, copied VERBATIM from `examples/r11_dist_ledger.rs` at 72fa4b8
//! (where they were conformance-checked against the real `BranchLedger`: `conform` = 0), with `pub`
//! added so the fleet harness can reach them. Not an example itself (a subdirectory without main.rs).
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use ferrodb::agent_sql::cluster::{moves_base, MergeVerdict};
use ferrodb::consensus::{BranchOp, Command, Entry, Round};

/// `cluster.rs` `LOCAL_BITS` / `LOCAL_MASK` (private there; the packing is `ClusterBranchId::of`).
pub const LOCAL_BITS: u32 = 40;
pub const LOCAL_MASK: u64 = (1u64 << LOCAL_BITS) - 1;

pub fn cid(node: u32, local: u64) -> u64 {
    ((node as u64) << LOCAL_BITS) | local
}
pub fn owner_of(id: u64) -> u32 {
    (id >> LOCAL_BITS) as u32
}
/// The fence / close-owner sentinel (O2 M1): an `Abandon` of a local id no node will ever mint.
pub fn sentinel(node: u32) -> u64 {
    cid(node, LOCAL_MASK)
}
pub fn is_sentinel(id: u64) -> bool {
    id != 0 && id & LOCAL_MASK == LOCAL_MASK
}
pub fn fork_c(child: u64) -> Command {
    Command::Branch { op: BranchOp::Fork { child, parent: 0, fork_epoch: 1, lease_millis: 900_000 } }
}
pub fn merge_c(branch: u64, base_round: Round) -> Command {
    Command::Branch { op: BranchOp::Merge { branch, base_round } }
}
pub fn abandon_c(branch: u64) -> Command {
    Command::Branch { op: BranchOp::Abandon { branch } }
}
pub fn wal_c(n: u64) -> Command {
    Command::WalBatch { start_lsn: n, bytes: Vec::new() }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum V {
    Applied,
    ReEval,
    Refused,
}

pub fn class(v: &MergeVerdict) -> V {
    match v {
        MergeVerdict::Applied { .. } => V::Applied,
        MergeVerdict::ReEvaluate { .. } => V::ReEval,
        MergeVerdict::Refused { .. } => V::Refused,
    }
}

pub trait Ledger {
    /// Apply one committed entry; `Some` for a merge's verdict class.
    fn apply(&mut self, e: &Entry) -> Option<V>;
    fn is_live(&self, id: u64) -> bool;
    /// Branch records held, the trunk counted as one where the arm keeps it.
    fn records(&self) -> usize;
    fn live_ids_of(&self, node: u32) -> Vec<u64>;
    /// Records touched by the single largest apply so far (HEAD: not instrumented, see `Head`).
    fn touched_max(&self) -> u64;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum St {
    Live,
    Merged(Round),
    Abandoned(Round),
    Reaped(Round, u32),
}

/// Same five fields and state width as `ReplicatedBranch`, so M1's heap is comparable with HEAD's.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub struct Rec {
    parent: u64,
    fork_epoch: u64,
    lease_millis: u64,
    forked_at: Round,
    st: St,
}

/// HEAD's rules copied (`cluster.rs` `apply` / `apply_branch` / `merge_verdict` at 7dc428f), plus
/// the M1 fence when `m1`, and two planted mutants for the controls.
#[derive(Clone)]
pub struct HeadCopy {
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
}

impl HeadCopy {
    pub fn new(m1: bool) -> HeadCopy {
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
        }
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
                if *child == 0 || self.br.contains_key(child) {
                    return None;
                }
                match self.br.get(parent) {
                    Some(p) if self.rec_live(*parent, p) => {}
                    _ => return None,
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
                None
            }
            BranchOp::Merge { branch, base_round } => {
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
                if let Some(r) = self.br.get(branch).copied() {
                    // HEAD checks `state.is_live()` only (not the fence): kept identical.
                    if r.st == St::Live {
                        self.br.get_mut(branch).unwrap().st = St::Abandoned(round);
                    }
                }
                None
            }
            BranchOp::Reap { branch, generation } => {
                if let Some(r) = self.br.get_mut(branch) {
                    if !matches!(r.st, St::Reaped(..)) {
                        r.st = St::Reaped(round, *generation);
                    }
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
pub struct Zk {
    recs: BTreeMap<u64, Rec>,
    by_owner: BTreeMap<u32, BTreeSet<u64>>,
    last_applied: Round,
    lbm: Round,
    touched: u64,
}

impl Zk {
    pub fn new() -> Zk {
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

