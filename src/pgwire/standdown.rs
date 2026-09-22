//! W4 check 3's instrument: how often does a statement **stand down** from the shared read path?
//!
//! # The question this exists to answer
//!
//! [`ServerContext::begin_read`](super::ServerContext::begin_read) takes a per-connection slot and
//! then checks `writer_active`. If a writer is announced it clears the slot and answers `None`,
//! and the caller falls back to the exclusive catalog mutex. `begin_read`'s own doc claims this
//! *"happens only while DDL is actually in flight"*.
//!
//! W4 proposes moving DML onto the shared path. **If that claim is wrong — if MERGE, forks or any
//! other exclusive acquirer announce often enough that an arriving statement routinely stands down
//! — then W4 buys nothing**, because a stood-down statement takes the exclusive mutex anyway. The
//! discriminating quantity is a ratio of two integers, not a duration, which is why this is a
//! counter and not a timer: an integer is immune to whatever else is running on the box.
//!
//! # `None` is not zero
//!
//! [`counts`] returns `Option<Counts>`. `None` means **this build was not instrumented** — the
//! `w4-standdown-count` feature is off and the counters do not exist. `Some(c)` with
//! `c.stood_down == 0` means the instrument ran and observed no stand-down. A harness that
//! collapsed those two into one number would report "never stood down" for a build that was never
//! measuring, which is the single most likely way this experiment produces a false pass. Callers
//! are expected to refuse on `None` rather than to default it.
//!
//! # Why `begin_read` is the right place, and what it measures about a design that does not exist
//!
//! `begin_read` is called by `pgwire::extended::Statement::execute` for **every** `Kind::Sql`
//! statement, before the statement's kind is ever consulted: `try_run_read` is what later answers
//! `None` for a DML. So the admitted/stood-down split at `begin_read` is already the
//! counterfactual W4 asks about — *if DML were admissible to the shared path, how often would the
//! barrier have refused it?* — measured on the shipped path rather than on a prototype.
//!
//! ⚠ **One asymmetry must be stated wherever these numbers are.** Today a DML takes the exclusive
//! path, so it *announces* for the whole statement. Under W4 it would not. A workload containing
//! DML therefore has a larger announcer set here than the same workload would have under W4, and
//! its stand-down fraction is a **today** number, not a W4 number. A workload with no DML —
//! forks, reads and MERGE — has the same announcer set in both worlds and transfers directly.
//!
//! # The announce counters
//!
//! A bare stand-down fraction says whether W4 survives but not what to do if it does not, so the
//! four sites that can announce a writer are counted separately:
//!
//! * [`Site::Parse`] — `Statement::parse_one` takes the catalog to infer parameter and column
//!   types. This fires once per `Parse` message in the extended protocol and **once per statement**
//!   in the simple query protocol.
//! * [`Site::ReadRefresh`] — `ServerContext::read_catalog` taking the mutex to rebuild a stale
//!   per-connection snapshot: first use of a connection, and after a schema change.
//! * [`Site::ExecExclusive`] — the exclusive execution path, which is every statement `try_run_read`
//!   refuses: all DML, MERGE, DDL and the fork statement.
//! * [`Site::LeaseScan`] — the reaper's lease thread (`impl RuntimeLock for ServerContext`), which
//!   takes the same mutex on a timer with no client involved. Tagged separately because "the
//!   reaper's cadence dominates" is a specific, actionable answer and must not hide inside a total.
//!
//! [`Counts::announce_total`] is incremented inside `ServerContext::catalog` itself, so it counts
//! **every** acquisition whether or not a site tag was added at the call site. `announce_total`
//! exceeding the sum of the four sites means an acquirer exists that this module does not know
//! about, and a harness should say so rather than divide by a number it cannot attribute.

/// What a statement would be under W4, which is what decides whether its stand-down costs
/// anything.
///
/// ⚠ **A global stand-down fraction answers the wrong question, and the first run of this
/// instrument proved it.** In an agent-realistic arm most attempts come from the fork statement
/// itself, which is not DML and stays on the exclusive path under W4 as well. A fork standing
/// down and then taking the mutex is what a fork does today; counting it makes W4 look doomed for
/// a reason W4 does not own. The population W4's payoff depends on is reads (today) and DML
/// (under W4), so the counters are split three ways and the headline is read from one of them.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Class {
    /// `try_run_read` admits it today — a plain or agent-session `SELECT` with no `AS OF`, or an
    /// `EXPLAIN`. This is the population that already benefits from the shared path, and a DML
    /// moved there by W4 would sit in exactly the same position, seeing exactly the same
    /// announcers. **This is the headline.**
    SharedToday,
    /// INSERT / UPDATE / DELETE: the statements W4 proposes to move.
    Dml,
    /// Exclusive under W4 too — the fork statement, MERGE, DDL, and everything else
    /// `try_run_read` refuses for reasons W4 does not change.
    ExclusiveEither,
}

/// Which bucket a statement falls in.
///
/// ⚠ **This is an INSTRUMENT, not a dispatcher, and the difference matters here.** `try_run_read`
/// deliberately returns an `Option` instead of exposing an `is_read()` predicate, precisely so no
/// second predicate can drift from what the executor does. This function IS a second predicate,
/// so it is never allowed to decide anything: it only labels a counter. And it is checked against
/// the real answer on every admitted statement by [`note_dispatch_agreement`], which counts
/// disagreements. A non-zero `classify_mismatch` means this labelling has drifted and the split
/// must not be trusted — the harness says so rather than dividing by it.
pub fn classify(stmt: &crate::parser::parser::Stmt) -> Class {
    use crate::parser::parser::Stmt;
    match stmt {
        // Mirrors `try_run_read`'s two Select arms: `as_of.is_none()` is load-bearing there and
        // is load-bearing here for the same reason.
        Stmt::Explain(_) => Class::SharedToday,
        Stmt::Select { from, .. } if from.as_of.is_none() => Class::SharedToday,
        Stmt::Insert { .. } | Stmt::Update { .. } | Stmt::Delete { .. } => Class::Dml,
        _ => Class::ExclusiveEither,
    }
}

/// Which caller announced a writer by taking the exclusive catalog mutex.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Site {
    /// `Statement::parse_one` — type inference and row description.
    Parse,
    /// `ServerContext::read_catalog` — rebuilding a stale read snapshot.
    ReadRefresh,
    /// The exclusive execution path in `Statement::execute`.
    ExecExclusive,
    /// The lease scan — `impl RuntimeLock for ServerContext` — which holds the exclusive catalog
    /// for the whole scan on a timer, with no client involved. This is the reaper's cadence, and
    /// it is tagged separately because a stand-down fraction that is really a reaper artefact is
    /// a different answer from one caused by client statements.
    LeaseScan,
}

/// One reading of the instrument. All counters are cumulative since process start or the last
/// [`reset`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// `begin_read` returned `Some(ReadPass)`.
    pub admitted: u64,
    /// `begin_read` returned `None`: a writer was announced and the caller fell back.
    pub stood_down: u64,
    /// Every acquisition of the exclusive catalog mutex, counted at the mutex.
    pub announce_total: u64,
    /// Acquisitions attributed to [`Site::Parse`].
    pub announce_parse: u64,
    /// Acquisitions attributed to [`Site::ReadRefresh`].
    pub announce_read_refresh: u64,
    /// Acquisitions attributed to [`Site::ExecExclusive`].
    pub announce_exec_exclusive: u64,
    /// Acquisitions attributed to [`Site::LeaseScan`].
    pub announce_lease_scan: u64,

    /// Admitted attempts by [`Class::SharedToday`] statements.
    pub admitted_shared_today: u64,
    /// Stand-downs by [`Class::SharedToday`] statements. **The headline numerator.**
    pub stood_down_shared_today: u64,
    /// Admitted attempts by [`Class::Dml`] statements.
    pub admitted_dml: u64,
    /// Stand-downs by [`Class::Dml`] statements.
    pub stood_down_dml: u64,
    /// Admitted attempts by [`Class::ExclusiveEither`] statements.
    pub admitted_exclusive: u64,
    /// Stand-downs by [`Class::ExclusiveEither`] statements. Counted, but W4's payoff does not
    /// depend on them: these statements take the exclusive mutex under W4 too.
    pub stood_down_exclusive: u64,

    /// Admitted statements where [`classify`] and `try_run_read` disagreed. Must be zero for the
    /// class split to mean anything.
    pub classify_mismatch: u64,
}

impl Counts {
    /// Attempts at the shared path: `admitted + stood_down`.
    pub fn attempts(&self) -> u64 {
        self.admitted + self.stood_down
    }

    /// Stand-downs as a fraction of attempts, or `None` if nothing was attempted.
    ///
    /// `None` here is the "a run that collected nothing has not passed" case and is deliberately
    /// not reported as 0.0: a harness whose workload never reached `begin_read` has measured
    /// nothing at all.
    pub fn stand_down_fraction(&self) -> Option<f64> {
        frac(self.stood_down, self.attempts())
    }

    /// Attempts by [`Class::SharedToday`] statements.
    pub fn attempts_shared_today(&self) -> u64 {
        self.admitted_shared_today + self.stood_down_shared_today
    }

    /// **The number W4 check 3 turns on**: how often a statement that the shared path already
    /// serves is refused it. A DML moved there by W4 would sit in the same position under the
    /// same announcers, so this is the fraction of W4's win that the barrier gives back.
    pub fn stand_down_fraction_shared_today(&self) -> Option<f64> {
        frac(self.stood_down_shared_today, self.attempts_shared_today())
    }

    /// Attempts by [`Class::Dml`] statements.
    pub fn attempts_dml(&self) -> u64 {
        self.admitted_dml + self.stood_down_dml
    }

    /// Stand-down fraction for DML — today's DML, which still takes the exclusive path after the
    /// attempt. Useful as a cross-check on the shared-today figure, not as a substitute for it.
    pub fn stand_down_fraction_dml(&self) -> Option<f64> {
        frac(self.stood_down_dml, self.attempts_dml())
    }

    /// Attempts by [`Class::ExclusiveEither`] statements.
    pub fn attempts_exclusive(&self) -> u64 {
        self.admitted_exclusive + self.stood_down_exclusive
    }

    /// Stand-down fraction for statements that are exclusive under W4 as well.
    pub fn stand_down_fraction_exclusive(&self) -> Option<f64> {
        frac(self.stood_down_exclusive, self.attempts_exclusive())
    }

    /// Attempts the class counters saw that the global counters did not, or vice versa.
    ///
    /// The global pair is incremented inside `begin_read` and the class counters at its one
    /// pgwire call site, so in a process whose only driver is pgwire they must agree. A
    /// difference means something called `begin_read` outside the statement path and the class
    /// split covers less than the total.
    pub fn class_coverage_gap(&self) -> i128 {
        self.attempts() as i128
            - (self.attempts_shared_today() + self.attempts_dml() + self.attempts_exclusive())
                as i128
    }

    /// Acquisitions this module could not attribute to a known site.
    ///
    /// Non-zero means an exclusive acquirer exists that the site tags do not cover, so the
    /// per-site breakdown is incomplete and must be labelled as such.
    pub fn announce_unattributed(&self) -> u64 {
        self.announce_total.saturating_sub(
            self.announce_parse
                + self.announce_read_refresh
                + self.announce_exec_exclusive
                + self.announce_lease_scan,
        )
    }
}

fn frac(num: u64, den: u64) -> Option<f64> {
    if den == 0 { None } else { Some(num as f64 / den as f64) }
}

#[cfg(feature = "w4-standdown-count")]
mod imp {
    use super::{Class, Counts, Site};
    use std::sync::atomic::{AtomicU64, Ordering};

    // Relaxed throughout, and that is not a shortcut. These counters are not synchronising
    // anything: nothing reads them to decide what to do, and the harness reads them after its
    // workload threads have been joined, which is itself the happens-before edge. Using SeqCst
    // here would put a fence on the very path being measured and change the thing under study.
    static ADMITTED: AtomicU64 = AtomicU64::new(0);
    static STOOD_DOWN: AtomicU64 = AtomicU64::new(0);
    static ANNOUNCE_TOTAL: AtomicU64 = AtomicU64::new(0);
    static ANNOUNCE_PARSE: AtomicU64 = AtomicU64::new(0);
    static ANNOUNCE_READ_REFRESH: AtomicU64 = AtomicU64::new(0);
    static ANNOUNCE_EXEC_EXCLUSIVE: AtomicU64 = AtomicU64::new(0);
    static ANNOUNCE_LEASE_SCAN: AtomicU64 = AtomicU64::new(0);
    static ADMITTED_SHARED_TODAY: AtomicU64 = AtomicU64::new(0);
    static STOOD_DOWN_SHARED_TODAY: AtomicU64 = AtomicU64::new(0);
    static ADMITTED_DML: AtomicU64 = AtomicU64::new(0);
    static STOOD_DOWN_DML: AtomicU64 = AtomicU64::new(0);
    static ADMITTED_EXCLUSIVE: AtomicU64 = AtomicU64::new(0);
    static STOOD_DOWN_EXCLUSIVE: AtomicU64 = AtomicU64::new(0);
    static CLASSIFY_MISMATCH: AtomicU64 = AtomicU64::new(0);

    #[inline]
    pub fn note_admitted() {
        ADMITTED.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn note_stood_down() {
        STOOD_DOWN.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn note_acquire() {
        ANNOUNCE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn note_site(site: Site) {
        match site {
            Site::Parse => &ANNOUNCE_PARSE,
            Site::ReadRefresh => &ANNOUNCE_READ_REFRESH,
            Site::ExecExclusive => &ANNOUNCE_EXEC_EXCLUSIVE,
            Site::LeaseScan => &ANNOUNCE_LEASE_SCAN,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn note_attempt(class: Class, admitted: bool) {
        match (class, admitted) {
            (Class::SharedToday, true) => &ADMITTED_SHARED_TODAY,
            (Class::SharedToday, false) => &STOOD_DOWN_SHARED_TODAY,
            (Class::Dml, true) => &ADMITTED_DML,
            (Class::Dml, false) => &STOOD_DOWN_DML,
            (Class::ExclusiveEither, true) => &ADMITTED_EXCLUSIVE,
            (Class::ExclusiveEither, false) => &STOOD_DOWN_EXCLUSIVE,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn note_dispatch_agreement(class: Class, shared_path_ran: bool) {
        if (class == Class::SharedToday) != shared_path_ran {
            CLASSIFY_MISMATCH.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn counts() -> Option<Counts> {
        Some(Counts {
            admitted: ADMITTED.load(Ordering::Relaxed),
            stood_down: STOOD_DOWN.load(Ordering::Relaxed),
            announce_total: ANNOUNCE_TOTAL.load(Ordering::Relaxed),
            announce_parse: ANNOUNCE_PARSE.load(Ordering::Relaxed),
            announce_read_refresh: ANNOUNCE_READ_REFRESH.load(Ordering::Relaxed),
            announce_exec_exclusive: ANNOUNCE_EXEC_EXCLUSIVE.load(Ordering::Relaxed),
            announce_lease_scan: ANNOUNCE_LEASE_SCAN.load(Ordering::Relaxed),
            admitted_shared_today: ADMITTED_SHARED_TODAY.load(Ordering::Relaxed),
            stood_down_shared_today: STOOD_DOWN_SHARED_TODAY.load(Ordering::Relaxed),
            admitted_dml: ADMITTED_DML.load(Ordering::Relaxed),
            stood_down_dml: STOOD_DOWN_DML.load(Ordering::Relaxed),
            admitted_exclusive: ADMITTED_EXCLUSIVE.load(Ordering::Relaxed),
            stood_down_exclusive: STOOD_DOWN_EXCLUSIVE.load(Ordering::Relaxed),
            classify_mismatch: CLASSIFY_MISMATCH.load(Ordering::Relaxed),
        })
    }

    pub fn reset() {
        for c in [
            &ADMITTED,
            &STOOD_DOWN,
            &ANNOUNCE_TOTAL,
            &ANNOUNCE_PARSE,
            &ANNOUNCE_READ_REFRESH,
            &ANNOUNCE_EXEC_EXCLUSIVE,
            &ANNOUNCE_LEASE_SCAN,
            &ADMITTED_SHARED_TODAY,
            &STOOD_DOWN_SHARED_TODAY,
            &ADMITTED_DML,
            &STOOD_DOWN_DML,
            &ADMITTED_EXCLUSIVE,
            &STOOD_DOWN_EXCLUSIVE,
            &CLASSIFY_MISMATCH,
        ] {
            c.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(not(feature = "w4-standdown-count"))]
mod imp {
    use super::{Class, Counts, Site};

    #[inline]
    pub fn note_attempt(_class: Class, _admitted: bool) {}
    #[inline]
    pub fn note_dispatch_agreement(_class: Class, _shared_path_ran: bool) {}
    #[inline]
    pub fn note_admitted() {}
    #[inline]
    pub fn note_stood_down() {}
    #[inline]
    pub fn note_acquire() {}
    #[inline]
    pub fn note_site(_site: Site) {}

    /// Not instrumented. See the module doc: this is a different fact from zero.
    pub fn counts() -> Option<Counts> {
        None
    }

    pub fn reset() {}
}

pub(crate) use imp::{note_acquire, note_admitted, note_site, note_stood_down};
pub(crate) use imp::{note_attempt, note_dispatch_agreement};

/// Read the instrument, or `None` if this build does not carry it.
///
/// See the module doc for why `None` and `Some(Counts { stood_down: 0, .. })` must not be
/// collapsed by a caller.
pub fn counts() -> Option<Counts> {
    imp::counts()
}

/// Zero every counter. A no-op in an uninstrumented build.
pub fn reset() {
    imp::reset()
}
