//! Who is in the cluster — and therefore what a majority is.
//!
//! This is one immutable value, deliberately. The alternative, a peer list beside a member count,
//! is a pair that has to change together, and **a majority counted against the wrong number elects
//! two leaders of one term** — a failure nothing later in the protocol can detect, because both
//! leaders behave correctly given what each believes the cluster to be.
//!
//! So a `Config` is replaced wholesale, never mutated in place.

use super::NodeId;

/// A configuration version paired with the term that created it.
///
/// **The pair, never the version alone.** A version is ambiguous across terms: every new leader's
/// first configuration change is version 1 *in its own term*, so a follower's stale acknowledgement
/// of some earlier term's version 1 would be counted toward the new leader's — and the leader would
/// then believe a change had reached a majority when it had not, which is the precondition it uses
/// to allow the *next* change. Two changes in flight over an unacknowledged one is how a
/// single-node membership change stops being safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct CfgAt {
    pub version: u64,
    pub term: u64,
}

/// The voter set.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    members: Vec<NodeId>,
    /// Members that receive the log but **do not vote and are not counted in any majority**.
    ///
    /// A node being added joins here first. Counting a node that holds none of the log toward a
    /// quorum enlarges the denominator without enlarging the set that can actually answer, which
    /// *reduces* availability at exactly the moment an operator believes they are increasing it.
    learners: Vec<NodeId>,
    pub version: u64,
    /// The term in which this configuration was created, so acknowledgements can be disambiguated.
    pub term: u64,
}

impl Config {
    /// A configuration a joining node holds before it has been told the real one. It contains
    /// nobody, so `contains(self)` is false and the node cannot campaign — which is the intent.
    pub fn empty() -> Self {
        Config { members: Vec::new(), learners: Vec::new(), version: 0, term: 0 }
    }

    /// Members are sorted and de-duplicated on construction.
    ///
    /// Both matter. Sorted, so two nodes given the same set in different orders produce equal
    /// configurations and comparison is a value comparison. De-duplicated, because a repeated id
    /// would inflate `len()` and therefore the quorum — one node counted twice is one vote counted
    /// twice, which is the two-leaders bug arriving through the front door.
    pub fn new(members: impl IntoIterator<Item = NodeId>, version: u64, term: u64) -> Self {
        let mut members: Vec<NodeId> = members.into_iter().collect();
        members.sort();
        members.dedup();
        Config { members, learners: Vec::new(), version, term }
    }

    pub fn with_learners(mut self, learners: impl IntoIterator<Item = NodeId>) -> Self {
        let mut l: Vec<NodeId> = learners.into_iter().collect();
        l.sort();
        l.dedup();
        // A node cannot be both. If it is a voter, that is the stronger statement and the learner
        // entry is dropped — silently promoting a voter to learner would shrink the quorum.
        l.retain(|n| !self.members.contains(n));
        self.learners = l;
        self
    }

    pub fn members(&self) -> &[NodeId] { &self.members }
    pub fn learners(&self) -> &[NodeId] { &self.learners }

    /// Voters only. Learners are excluded by construction.
    pub fn contains(&self, n: NodeId) -> bool { self.members.contains(&n) }

    /// Voters and learners — who to *send* to, as opposed to who to *count*.
    pub fn is_known(&self, n: NodeId) -> bool {
        self.members.contains(&n) || self.learners.contains(&n)
    }

    pub fn len(&self) -> usize { self.members.len() }
    pub fn is_empty(&self) -> bool { self.members.is_empty() }

    /// How many votes carry a decision: a strict majority of the voter set.
    ///
    /// `len/2 + 1` and not `len/2`, and integer division makes both odd and even sizes correct: 3
    /// needs 2, 4 needs 3, 5 needs 3. An even cluster gains no availability over the odd one below
    /// it, which is why odd sizes are the ones worth running.
    pub fn quorum(&self) -> usize { self.members.len() / 2 + 1 }

    /// Whether `votes` — already filtered to this configuration — carries a decision.
    ///
    /// An **empty configuration never has a quorum**, even though `0 / 2 + 1 == 1` would be
    /// satisfied by a single vote. A joining node holds an empty configuration, and without this
    /// it could elect itself leader of a cluster it has not yet been admitted to.
    pub fn has_quorum(&self, votes: usize) -> bool {
        !self.members.is_empty() && votes >= self.quorum()
    }

    pub fn at(&self) -> CfgAt { CfgAt { version: self.version, term: self.term } }

    /// The set this configuration would become with `n` added as a voter.
    pub fn adding(&self, n: NodeId, term: u64) -> Config {
        let mut m = self.members.clone();
        m.push(n);
        Config::new(m, self.version + 1, term)
            .with_learners(self.learners.iter().copied().filter(|x| *x != n))
    }

    /// The set this configuration would become with `n` removed.
    pub fn removing(&self, n: NodeId, term: u64) -> Config {
        let m = self.members.iter().copied().filter(|x| *x != n);
        Config::new(m, self.version + 1, term)
            .with_learners(self.learners.iter().copied().filter(|x| *x != n))
    }

    /// The set this configuration would become with `n` added as a non-voting learner.
    pub fn adding_learner(&self, n: NodeId, term: u64) -> Config {
        let mut l: Vec<NodeId> = self.learners.clone();
        l.push(n);
        Config::new(self.members.clone(), self.version + 1, term).with_learners(l)
    }
}
