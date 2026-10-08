//! r11-dist DC1 and DC2 (PREREG A16, A16a): the election timer's first draws, and split votes in
//! virtual time, both on the REAL `Consensus` state machine. No socket, thread or wall clock is
//! involved: every number is an integer count or a virtual microsecond, so no timing window is
//! needed and machine load cannot move a result.
//!
//! The transport is MODELLED from its source, not run:
//! - A frame on an open link takes `WIRE_US`.
//! - Links are open from start, because the sender dials eagerly (`transport.rs:1985-1986`).
//! - The first frame on a link the peer closed for idleness waits for a redial. `dial` blocks for
//!   the acceptor's handshake reply (`transport.rs:2193`). The acceptor answers only after its
//!   nonblocking accept loop's next wake (`transport.rs:2338-2339`, `poll_interval` = 50 ms), so
//!   the wait is `Accept`'s, plus `HANDSHAKE_US`.
//! - Later frames on that link queue behind it (FIFO per link, as one TCP connection is).
//!
//! A node performs its actions in order on one thread, as `Node::drain` does:
//! - a `Persist` or a `PersistHardState` costs `FSYNC_US` before any later `Send`;
//! - a `Persist` feeds back `Persisted` with the node's term at perform time (`node.rs:799`).
//!
//! Ticks keep a fixed phase from creation (`node.rs:508-510`). The phases come from a per-salt
//! PRNG that is separate from the node seeds, as independent machines' phases would be.

use std::collections::{BTreeMap, BTreeSet};

use ferrodb::consensus::config::Config;
use ferrodb::consensus::{Action, Consensus, Event, Message, NodeId, Rng, Role, Round, Term};

pub const TICK_US: u64 = 50_000;
const WIRE_US: u64 = 100;
const HANDSHAKE_US: u64 = 300;
const FSYNC_US: u64 = 1_000;

fn cfg() -> Config {
    Config::new((1..=3).map(NodeId), 1, 0)
}

/// DC1: tick one fresh node alone until it pre-campaigns. The tick count is its first draw.
pub fn first_draw(id: NodeId, seed: u64) -> u32 {
    let mut c = Consensus::new(id, cfg(), seed);
    for t in 1..=100u32 {
        for a in c.step(Event::Tick) {
            if let Action::RoleChanged { role: Role::PreCandidate, .. } = a {
                return t;
            }
        }
    }
    panic!("node {id:?} did not pre-campaign within 100 ticks");
}

/// How long the first frame on an idle-closed link waits for the peer's accept.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Accept {
    /// HEAD with D224's fix: the accept loop's next wake, uniform over one `poll_interval`.
    Poll,
    /// F1: an accept that answers at once.
    Zero,
    /// The links are never closed (A12a's 3600-s arm).
    Open,
}

enum Ev {
    Tick(usize),
    Deliver(usize, Message),
    Persisted(usize, Term, Round),
}

pub struct Outcome {
    pub init_leader: Option<usize>,
    /// Neither survivor pre-campaigned before the stop, so each still holds its FIRST draw.
    pub survivors_first_draw: bool,
    pub new_leader: Option<usize>,
    pub split: bool,
    pub precands: [u32; 3],
    pub cands: [u32; 3],
    pub elect_us: u64,
}

struct Des {
    seq: u64,
    q: BTreeMap<(u64, u64), Ev>,
    n: Vec<Consensus>,
    up: [bool; 3],
    busy: [u64; 3],
    link_free: BTreeMap<(usize, usize), u64>,
    closed: BTreeSet<(usize, usize)>,
    rng: Rng,
    accept: Accept,
    /// (node, role, term, virtual time) for every `RoleChanged`.
    log: Vec<(usize, Role, Term, u64)>,
}

impl Des {
    fn push(&mut self, at: u64, ev: Ev) {
        self.seq += 1;
        self.q.insert((at, self.seq), ev);
    }

    fn step_node(&mut self, k: usize, at: u64, ev: Event) {
        let mut s = at;
        for a in self.n[k].step(ev) {
            match a {
                Action::Send(m) => {
                    let j = (m.to.0 - 1) as usize;
                    let mut arrive = s + WIRE_US;
                    if self.closed.remove(&(k, j)) {
                        let wait = match self.accept {
                            Accept::Poll => self.rng.next_u64() % TICK_US,
                            Accept::Zero | Accept::Open => 0,
                        };
                        arrive += wait + HANDSHAKE_US;
                    }
                    let f = self.link_free.entry((k, j)).or_insert(0);
                    arrive = arrive.max(*f);
                    *f = arrive;
                    self.push(arrive, Ev::Deliver(j, m));
                }
                Action::Persist { entries } => {
                    if let Some(last) = entries.last() {
                        s += FSYNC_US;
                        let term = self.n[k].term();
                        self.push(s, Ev::Persisted(k, term, last.round));
                    }
                }
                Action::PersistHardState { .. } => s += FSYNC_US,
                Action::RoleChanged { role, term, .. } => self.log.push((k, role, term, s)),
                _ => {}
            }
        }
        self.busy[k] = s;
    }

    fn run_until(&mut self, end: u64, stop: impl Fn(&Des) -> bool) -> u64 {
        let mut now = 0;
        while let Some((&(at, sq), _)) = self.q.iter().next() {
            if at > end || stop(self) {
                break;
            }
            let ev = self.q.remove(&(at, sq)).unwrap();
            now = at;
            match ev {
                Ev::Tick(k) => {
                    if !self.up[k] {
                        continue;
                    }
                    self.push(at + TICK_US, Ev::Tick(k));
                    let t = at.max(self.busy[k]);
                    self.step_node(k, t, Event::Tick);
                }
                Ev::Deliver(j, m) => {
                    if !self.up[j] {
                        continue;
                    }
                    let t = at.max(self.busy[j]);
                    self.step_node(j, t, Event::Recv(m));
                }
                Ev::Persisted(k, term, round) => {
                    if !self.up[k] {
                        continue;
                    }
                    let t = at.max(self.busy[k]);
                    self.step_node(k, t, Event::Persisted { term, round });
                }
            }
        }
        now
    }

    fn leader_among(&self, who: &[usize]) -> Option<usize> {
        who.iter().copied().find(|&k| self.up[k] && self.n[k].role() == Role::Leader)
    }
}

/// DC2: elect, hold 3 s, stop the leader at a random phase, close the survivors' mutual links
/// (unless `Open`), and run to the first survivor leader.
pub fn failover(seeds: [u64; 3], phase_seed: u64, accept: Accept) -> Outcome {
    let c = cfg();
    let mut phases = Rng::new(phase_seed);
    let mut d = Des {
        seq: 0,
        q: BTreeMap::new(),
        n: (0..3).map(|i| Consensus::new(NodeId(i as u32 + 1), c.clone(), seeds[i])).collect(),
        up: [true; 3],
        busy: [0; 3],
        link_free: BTreeMap::new(),
        closed: BTreeSet::new(),
        rng: Rng::new(phase_seed ^ 0xACCE_97AC_CE97_ACCE),
        accept,
        log: Vec::new(),
    };
    for k in 0..3 {
        let ph = phases.next_u64() % TICK_US;
        d.push(ph, Ev::Tick(k));
    }
    let all = [0usize, 1, 2];
    let t_elect = d.run_until(60_000_000, |d| d.leader_among(&all).is_some());
    let Some(l) = d.leader_among(&all) else {
        return Outcome {
            init_leader: None,
            survivors_first_draw: false,
            new_leader: None,
            split: false,
            precands: [0; 3],
            cands: [0; 3],
            elect_us: 0,
        };
    };
    d.run_until(t_elect + 3_000_000, |_| false);
    let surv: Vec<usize> = all.iter().copied().filter(|&k| k != l).collect();
    let survivors_first_draw =
        !d.log.iter().any(|&(k, r, _, _)| surv.contains(&k) && matches!(r, Role::PreCandidate | Role::Candidate));
    let t_stop = t_elect + 3_000_000 + phases.next_u64() % TICK_US;
    d.run_until(t_stop, |_| false);
    d.up[l] = false;
    if accept != Accept::Open {
        d.closed.insert((surv[0], surv[1]));
        d.closed.insert((surv[1], surv[0]));
    }
    let from = d.log.len();
    let t_new = d.run_until(t_stop + 60_000_000, |d| d.leader_among(&surv).is_some());
    let new_leader = d.leader_among(&surv);
    let mut precands = [0u32; 3];
    let mut cands = [0u32; 3];
    let mut cand_terms: [BTreeSet<Term>; 3] = Default::default();
    for &(k, r, t, _) in &d.log[from..] {
        match r {
            Role::PreCandidate => precands[k] += 1,
            Role::Candidate => {
                cands[k] += 1;
                cand_terms[k].insert(t);
            }
            _ => {}
        }
    }
    let split = cand_terms[surv[0]].intersection(&cand_terms[surv[1]]).next().is_some();
    Outcome {
        init_leader: Some(l),
        survivors_first_draw,
        new_leader,
        split,
        precands,
        cands,
        elect_us: t_new.saturating_sub(t_stop),
    }
}
