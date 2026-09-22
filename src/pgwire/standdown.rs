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
//! three sites that can announce a writer are counted separately:
//!
//! * [`Site::Parse`] — `Statement::parse_one` takes the catalog to infer parameter and column
//!   types. This fires once per `Parse` message in the extended protocol and **once per statement**
//!   in the simple query protocol.
//! * [`Site::ReadRefresh`] — `ServerContext::read_catalog` taking the mutex to rebuild a stale
//!   per-connection snapshot: first use of a connection, and after a schema change.
//! * [`Site::ExecExclusive`] — the exclusive execution path, which is every statement `try_run_read`
//!   refuses: all DML, MERGE, DDL and the fork statement.
//!
//! [`Counts::announce_total`] is incremented inside `ServerContext::catalog` itself, so it counts
//! **every** acquisition whether or not a site tag was added at the call site. `announce_total`
//! exceeding the sum of the three sites means an acquirer exists that this module does not know
//! about, and a harness should say so rather than divide by a number it cannot attribute.

/// Which caller announced a writer by taking the exclusive catalog mutex.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Site {
    /// `Statement::parse_one` — type inference and row description.
    Parse,
    /// `ServerContext::read_catalog` — rebuilding a stale read snapshot.
    ReadRefresh,
    /// The exclusive execution path in `Statement::execute`.
    ExecExclusive,
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
        let n = self.attempts();
        if n == 0 { None } else { Some(self.stood_down as f64 / n as f64) }
    }

    /// Acquisitions this module could not attribute to a known site.
    ///
    /// Non-zero means an exclusive acquirer exists that the site tags do not cover, so the
    /// per-site breakdown is incomplete and must be labelled as such.
    pub fn announce_unattributed(&self) -> u64 {
        self.announce_total.saturating_sub(
            self.announce_parse + self.announce_read_refresh + self.announce_exec_exclusive,
        )
    }
}

#[cfg(feature = "w4-standdown-count")]
mod imp {
    use super::{Counts, Site};
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
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    pub fn counts() -> Option<Counts> {
        Some(Counts {
            admitted: ADMITTED.load(Ordering::Relaxed),
            stood_down: STOOD_DOWN.load(Ordering::Relaxed),
            announce_total: ANNOUNCE_TOTAL.load(Ordering::Relaxed),
            announce_parse: ANNOUNCE_PARSE.load(Ordering::Relaxed),
            announce_read_refresh: ANNOUNCE_READ_REFRESH.load(Ordering::Relaxed),
            announce_exec_exclusive: ANNOUNCE_EXEC_EXCLUSIVE.load(Ordering::Relaxed),
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
        ] {
            c.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(not(feature = "w4-standdown-count"))]
mod imp {
    use super::{Counts, Site};

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
