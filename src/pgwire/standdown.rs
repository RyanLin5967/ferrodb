//! W4 check 3 — **how often does a shared-path attempt stand down?**
//!
//! # The question this exists to answer
//!
//! [`ServerContext::begin_read`](crate::pgwire::ServerContext::begin_read) takes a connection's
//! busy slot and then loads `writer_active`. If a writer is announced it clears the slot and
//! answers `None`, and the caller falls back to the exclusive catalog mutex. Its own doc says
//! standing down *"happens only while DDL is actually in flight"*.
//!
//! W4's whole premise is moving DML onto that shared path. **A statement that stands down takes
//! the exclusive mutex anyway**, so if the claim is wrong — if forks, MERGE or the lease scan
//! announce often enough that ordinary statements stand down routinely — W4 buys nothing,
//! whichever design option wins behind it.
//!
//! That is a **count**, not a timing experiment, which is the reason it can be run on a loaded
//! box: a ratio of two integers does not move when the machine is busy.
//!
//! # Why every field is an `Option`
//!
//! A harness that cannot tell *"this build was not instrumented"* from *"this build counted and
//! saw zero"* reports **"never stood down"** for a build that was not measuring. The two facts
//! are different and the type says so: with the `standdown_count` feature off every field is
//! `None`, and a caller that unwraps gets nothing to misread. Callers are expected to **refuse
//! and exit non-zero** on `None`.
//!
//! # Off by default, and why that is not a hedge
//!
//! The counters are relaxed atomic increments on the statement path. That is cheap but it is not
//! free, and the shared read path exists precisely because per-statement writes to shared words
//! were the wall D54 removed (`bench/d51_sharedword_probe.txt`: a shared relaxed *load* scales to
//! x7.823 over 16 threads). A counter is a shared *store*, which is the thing that does not. So
//! it is compiled out unless asked for:
//!
//! ```text
//! cargo build --release --features standdown_count --example w4_standdown_count
//! ```
//!
//! # Two recording points, and the difference is the layer
//!
//! * [`record_begin_read`] fires inside `begin_read` itself, so it counts **every** caller —
//!   including a harness that drives `ServerContext` directly, which several `examples/` do.
//! * [`record_pgwire_*`] fire at the single `begin_read` call site in
//!   [`crate::pgwire::extended`], so they count **only statements that arrived over the wire**.
//!
//! ⇒ `admitted + stood_down` equal to the sum of the pgwire buckets is a *check that the number
//! came from the pgwire layer*, not an assertion that it did. E.6 measured that driving
//! `AgentRuntime` directly and driving it through pgwire are different experiments; this makes
//! "which one is this?" answerable from the artifact rather than from the harness's own prose.
//!
//! # The announcer side
//!
//! Counting stand-downs alone cannot say *what announced*. [`record_drain`] counts every
//! `drain_readers` (i.e. every exclusive acquisition, from any caller — a statement, the lease
//! scan, or a `read_catalog` snapshot refresh) and [`record_pgwire_exclusive`] counts the ones a
//! wire statement made, with its verb. The residual is the non-statement announcer population,
//! which is the one a workload cannot control and the one that would otherwise make this
//! instrument non-discriminating.

/// A coarse label for reporting **only**.
///
/// ⚠ This is deliberately *not* a dispatch predicate. `try_run_read` returns `Option` rather than
/// exposing an `is_read()` precisely so that no second place can drift from it about what runs
/// where, and this must not become that second place: nothing in the engine branches on a
/// `Verb`. It exists so a stand-down count can be read as "which kinds of statement stood down"
/// instead of one undifferentiated number.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verb {
    Select,
    Explain,
    /// INSERT / UPDATE / DELETE — the statements W4 proposes to move onto the shared path.
    Dml,
    /// `BEGIN AGENT SESSION` — the fork.
    Fork,
    Merge,
    /// CREATE / DROP / ALTER — the case `begin_read`'s doc says is the only one that matters.
    Ddl,
    Other,
}

impl Verb {
    /// Stable order, and the index into every bucket array here.
    pub const ALL: [Verb; 7] =
        [Verb::Select, Verb::Explain, Verb::Dml, Verb::Fork, Verb::Merge, Verb::Ddl, Verb::Other];

    pub fn name(self) -> &'static str {
        match self {
            Verb::Select => "SELECT",
            Verb::Explain => "EXPLAIN",
            Verb::Dml => "DML",
            Verb::Fork => "FORK",
            Verb::Merge => "MERGE",
            Verb::Ddl => "DDL",
            Verb::Other => "OTHER",
        }
    }

    fn idx(self) -> usize {
        match self {
            Verb::Select => 0,
            Verb::Explain => 1,
            Verb::Dml => 2,
            Verb::Fork => 3,
            Verb::Merge => 4,
            Verb::Ddl => 5,
            Verb::Other => 6,
        }
    }
}

/// Everything the instrument knows, or `None` for every field when it was not compiled in.
#[derive(Clone, Debug, Default)]
pub struct Counts {
    /// `begin_read` returned `Some(ReadPass)` — from **any** caller.
    pub admitted: Option<u64>,
    /// `begin_read` returned `None` — a writer was announced, so the caller took the mutex.
    pub stood_down: Option<u64>,
    /// `drain_readers` ran: an exclusive acquisition from any caller.
    pub announced: Option<u64>,

    /// Admitted, counted at the pgwire call site only.
    pub wire_admitted: Option<Vec<(Verb, u64)>>,
    /// Stood down, counted at the pgwire call site only.
    pub wire_stood_down: Option<Vec<(Verb, u64)>>,
    /// Admitted **and** `try_run_read` answered `Some` — the statement really ran shared.
    pub wire_ran_shared: Option<Vec<(Verb, u64)>>,
    /// Took the exclusive path, for either reason. Each of these is one announcement.
    pub wire_exclusive: Option<Vec<(Verb, u64)>>,
}

impl Counts {
    /// Attempts = admitted + stood down. `None` if the build was not instrumented.
    pub fn attempts(&self) -> Option<u64> {
        Some(self.admitted? + self.stood_down?)
    }

    /// **The headline number: stand-downs as a fraction of attempts.**
    ///
    /// `None` when not instrumented. `Some(0.0)` is a measured zero, which is a different fact.
    /// A run with zero attempts answers `None` too: a ratio with no denominator has not been
    /// measured, and reporting it as 0.0 is exactly the "a run that collected nothing has not
    /// passed" failure.
    pub fn fraction(&self) -> Option<f64> {
        let a = self.attempts()?;
        if a == 0 {
            return None;
        }
        Some(self.stood_down? as f64 / a as f64)
    }

    /// Announcements `drain_readers` saw that no wire statement accounts for: the lease scan, and
    /// `read_catalog`'s snapshot refresh. Signed, because a negative value means the two recording
    /// points disagree and the accounting is broken rather than merely surprising.
    pub fn unattributed_announcements(&self) -> Option<i64> {
        let total = self.announced? as i64;
        let by_stmt: u64 = self.wire_exclusive.as_ref()?.iter().map(|(_, n)| n).sum();
        Some(total - by_stmt as i64)
    }

    /// Do the two recording points agree that every attempt came over the wire?
    ///
    /// `Some(true)` means this run's number is a **pgwire-layer** number. `Some(false)` means
    /// something drove `begin_read` without going through the wire, and the layer claim is void.
    pub fn is_pure_wire_layer(&self) -> Option<bool> {
        let wire: u64 = self.wire_admitted.as_ref()?.iter().map(|(_, n)| n).sum::<u64>()
            + self.wire_stood_down.as_ref()?.iter().map(|(_, n)| n).sum::<u64>();
        Some(wire == self.attempts()?)
    }
}

#[cfg(feature = "standdown_count")]
mod imp {
    use super::Verb;
    use std::sync::atomic::{AtomicU64, Ordering};

    const N: usize = Verb::ALL.len();

    // Relaxed throughout. These are a tally read once, after every worker thread has been joined,
    // so there is nothing for an ordering to publish: the join is the synchronisation edge. Using
    // SeqCst here would put a fence on the statement path to no purpose — and, worse, would change
    // the very interleaving the count is meant to observe.
    pub(super) static ADMITTED: AtomicU64 = AtomicU64::new(0);
    pub(super) static STOOD_DOWN: AtomicU64 = AtomicU64::new(0);
    pub(super) static ANNOUNCED: AtomicU64 = AtomicU64::new(0);

    #[allow(clippy::declare_interior_mutable_const)]
    const ZERO: AtomicU64 = AtomicU64::new(0);
    pub(super) static W_ADMITTED: [AtomicU64; N] = [ZERO; N];
    pub(super) static W_STOOD_DOWN: [AtomicU64; N] = [ZERO; N];
    pub(super) static W_RAN_SHARED: [AtomicU64; N] = [ZERO; N];
    pub(super) static W_EXCLUSIVE: [AtomicU64; N] = [ZERO; N];

    pub(super) fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(a: &[AtomicU64; N]) -> Vec<(Verb, u64)> {
        Verb::ALL.iter().map(|v| (*v, a[super::Verb::idx(*v)].load(Ordering::Relaxed))).collect()
    }
}

/// Called by `ServerContext::begin_read` with what it is about to return.
#[inline]
pub(crate) fn record_begin_read(_admitted: bool) {
    #[cfg(feature = "standdown_count")]
    imp::bump(if _admitted { &imp::ADMITTED } else { &imp::STOOD_DOWN });
}

/// Called by `ServerContext::drain_readers`: one exclusive acquisition, i.e. one announcement.
#[inline]
pub(crate) fn record_drain() {
    #[cfg(feature = "standdown_count")]
    imp::bump(&imp::ANNOUNCED);
}

/// Called at the pgwire `begin_read` call site with the outcome for a wire statement.
#[inline]
pub(crate) fn record_pgwire_attempt(_verb: Verb, _admitted: bool) {
    #[cfg(feature = "standdown_count")]
    imp::bump(&(if _admitted { &imp::W_ADMITTED } else { &imp::W_STOOD_DOWN })[_verb.idx()]);
}

/// Called when `try_run_read` answered `Some`: the statement really ran on the shared path.
#[inline]
pub(crate) fn record_pgwire_shared(_verb: Verb) {
    #[cfg(feature = "standdown_count")]
    imp::bump(&imp::W_RAN_SHARED[_verb.idx()]);
}

/// Called when a wire statement took the exclusive path, for either reason.
#[inline]
pub(crate) fn record_pgwire_exclusive(_verb: Verb) {
    #[cfg(feature = "standdown_count")]
    imp::bump(&imp::W_EXCLUSIVE[_verb.idx()]);
}

/// Zero every counter.
///
/// **For a bench harness, between a setup phase and a measured phase**, so that connection
/// warm-up — whose first statement always takes the catalog mutex to build a snapshot, and
/// therefore always announces — is not counted as part of the workload under test. It exists
/// because the alternative is subtracting an estimated setup cost from the result, and an
/// estimate in the denominator is how a count stops being load-immune.
///
/// ⚠ Not synchronised against statements in flight: call it with every worker joined. It is
/// compiled out entirely without `standdown_count`, so there is no reset in a normal build.
pub fn reset() {
    #[cfg(feature = "standdown_count")]
    {
        use std::sync::atomic::Ordering;
        for c in [&imp::ADMITTED, &imp::STOOD_DOWN, &imp::ANNOUNCED] {
            c.store(0, Ordering::Relaxed);
        }
        for a in [&imp::W_ADMITTED, &imp::W_STOOD_DOWN, &imp::W_RAN_SHARED, &imp::W_EXCLUSIVE] {
            for c in a.iter() {
                c.store(0, Ordering::Relaxed);
            }
        }
    }
}

/// The reporting label for a statement.
///
/// ⚠ Exhaustive on purpose: adding a `Stmt` variant must be a compile error here, not a silent
/// slide into `Other`. A label that quietly absorbs new statement kinds is how a count comes to
/// under-report the very thing that started announcing.
pub(crate) fn verb_of(stmt: &crate::parser::parser::Stmt) -> Verb {
    use crate::parser::parser::Stmt as S;
    match stmt {
        S::Select { .. } => Verb::Select,
        S::Explain(_) => Verb::Explain,
        S::Insert { .. } | S::Update { .. } | S::Delete { .. } => Verb::Dml,
        S::BeginAgentSession { .. } => Verb::Fork,
        S::Merge { .. } => Verb::Merge,
        S::CreateTable { .. }
        | S::DropTable { .. }
        | S::AlterTable { .. }
        | S::CreateIndex { .. }
        | S::CreateFullTextIndex { .. } => Verb::Ddl,
        S::Search { .. }
        | S::Join { .. }
        | S::Analyze { .. }
        | S::Begin
        | S::Commit
        | S::Rollback
        | S::Diff { .. }
        | S::Abandon { .. }
        | S::RevertMerge { .. }
        | S::Simulate { .. } => Verb::Other,
    }
}

/// Read the counters. Every field is `None` unless this crate was built with `standdown_count`.
pub fn counts() -> Counts {
    #[cfg(feature = "standdown_count")]
    {
        use std::sync::atomic::Ordering;
        Counts {
            admitted: Some(imp::ADMITTED.load(Ordering::Relaxed)),
            stood_down: Some(imp::STOOD_DOWN.load(Ordering::Relaxed)),
            announced: Some(imp::ANNOUNCED.load(Ordering::Relaxed)),
            wire_admitted: Some(imp::snapshot(&imp::W_ADMITTED)),
            wire_stood_down: Some(imp::snapshot(&imp::W_STOOD_DOWN)),
            wire_ran_shared: Some(imp::snapshot(&imp::W_RAN_SHARED)),
            wire_exclusive: Some(imp::snapshot(&imp::W_EXCLUSIVE)),
        }
    }
    #[cfg(not(feature = "standdown_count"))]
    Counts::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole point of the `Option`: an uninstrumented build must not read as a measured zero.
    ///
    /// This asserts the property that holds in **both** configurations — that `fraction()` is
    /// `None` exactly when there is nothing to divide — rather than asserting one configuration's
    /// numbers, because `cargo test` runs this file in whichever one it was built with.
    #[test]
    fn not_instrumented_is_not_a_measured_zero() {
        let c = Counts::default();
        assert!(c.admitted.is_none(), "a default Counts must not claim to have counted");
        assert!(c.stood_down.is_none());
        assert!(c.attempts().is_none(), "no attempts recorded is not zero attempts");
        assert!(c.fraction().is_none(), "an uninstrumented build has no fraction to report");
    }

    /// A measured zero IS a fact, and must survive as `Some(0.0)`.
    #[test]
    fn a_measured_zero_is_some() {
        let c = Counts {
            admitted: Some(100),
            stood_down: Some(0),
            announced: Some(0),
            ..Counts::default()
        };
        assert_eq!(c.attempts(), Some(100));
        assert_eq!(c.fraction(), Some(0.0));
    }

    /// Zero attempts is a collection failure, not a 0% stand-down rate.
    #[test]
    fn zero_attempts_has_no_fraction() {
        let c = Counts {
            admitted: Some(0),
            stood_down: Some(0),
            announced: Some(0),
            ..Counts::default()
        };
        assert_eq!(c.attempts(), Some(0));
        assert!(
            c.fraction().is_none(),
            "a run that attempted nothing has not measured a stand-down rate of 0"
        );
    }

    /// The layer check must be able to say NO, or it is decoration.
    #[test]
    fn layer_check_discriminates() {
        let wire_only = Counts {
            admitted: Some(9),
            stood_down: Some(1),
            announced: Some(1),
            wire_admitted: Some(vec![(Verb::Select, 9)]),
            wire_stood_down: Some(vec![(Verb::Select, 1)]),
            wire_ran_shared: Some(vec![(Verb::Select, 9)]),
            wire_exclusive: Some(vec![(Verb::Select, 1)]),
        };
        assert_eq!(wire_only.is_pure_wire_layer(), Some(true));

        // Same totals, but four attempts never went through the wire: a harness driving
        // `ServerContext` directly, which is the E.6 confusion this exists to catch.
        let mixed = Counts { wire_admitted: Some(vec![(Verb::Select, 5)]), ..wire_only.clone() };
        assert_eq!(mixed.is_pure_wire_layer(), Some(false));
    }

    /// The announcer residual must go negative when the two recording points disagree, rather
    /// than saturating at zero and reading as "everything is accounted for".
    #[test]
    fn unattributed_announcements_can_be_negative() {
        let c = Counts {
            announced: Some(3),
            wire_exclusive: Some(vec![(Verb::Merge, 10)]),
            ..Counts::default()
        };
        assert_eq!(c.unattributed_announcements(), Some(-7));
    }
}
