//! Which `AgentRuntime` an agent statement is allowed to run on.
//!
//! # The defect this refuses
//!
//! `Session::new()` builds its OWN runtime — `AgentRuntime::new()`, which is
//! `with_catalog(LogBranchCatalog::in_memory(..))`: `storage: None`, `reaper: None`, a private
//! in-memory effect log, a private provenance store and private branch state
//! (`execution/session.rs:20`, `agent_sql/runtime.rs:653`).
//!
//! So a process that builds a real engine — a durable branch catalog, or an `ArenaPageStore` via
//! `AgentRuntime::with_storage` — hands it to a [`ServerContext`](crate::pgwire::ServerContext),
//! and then runs its statements on a `Session::new()` **constructs that engine and never touches
//! it**. Every agent statement lands on the private in-memory stub instead.
//!
//! The failure is silent in the worst way: the statements succeed, the output is plausible, and
//! nothing in it names which runtime produced it. Four banked benchmarks (D55, D56, D67, D68) were
//! measured this way *after* the trap was already written down in the project's handoff notes and
//! in a `⛔` comment block at the top of `examples/d75_live_branch_scaling.rs`. Documentation had
//! already been tried and had already failed, which is why this is a refusal in the engine.
//!
//! # What "designated" means
//!
//! Constructing a `ServerContext` is a process stating "this catalog, this buffer pool, this txn
//! manager and **this runtime** are the engine my statements run on". `ServerContext::new`
//! registers its runtime here; `run_agent_stmt` refuses any agent statement whose session runtime
//! is not one of the live registered ones.
//!
//! The registry holds `Weak`s. A `ServerContext` that has been dropped designates nothing, which
//! is deliberate: the question is whether a *live* designated runtime is being bypassed, not
//! whether one ever existed.
//!
//! # Stated blind spots
//!
//! Naming these here rather than in a commit message, because the next person to trust this guard
//! will read the guard.
//!
//! * **A process that builds a storage-backed runtime and no `ServerContext` designates nothing,
//!   so nothing is checked.** `examples/d71_point_update_curve.rs` was in exactly that state — it
//!   built no runtime at all and staged every write into `Session::new()`'s in-memory overlay.
//!   Registering at `AgentRuntime::with_storage` as well was considered and **rejected**: the lib
//!   unit-test binary builds storage-backed runtimes (`src/branch/lease_thread/tests.rs`) on
//!   threads that run in parallel with unrelated storage-less sessions, so that trigger would fire
//!   on correct code. A guard that cries wolf gets switched off, and then it guards nothing.
//! * **The registry is process-wide, not per-thread.** It has to be: the real server designates on
//!   the main thread and runs statements on connection threads (`pgwire/mod.rs:358`), so a
//!   thread-local registry would be empty exactly where it matters. The cost is that a process
//!   deliberately running agent statements on a second, throwaway runtime while a `ServerContext`
//!   is alive is refused. No caller in this repo does that. If one ever needs to, it must
//!   designate that runtime too — the check does not get loosened.
//! * **It does not check that the runtime has storage.** `storage: None` is a legitimate
//!   configuration (every unit test, `AgentRuntime::with_catalog` over a durable sidecar). The
//!   dangerous state is not "no storage", it is "the engine this process built is not the engine
//!   this statement is running on", and that is what is checked.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::agent_sql::runtime::AgentRuntime;
use crate::error::FerroError;

/// Has anything ever been designated? The fast path for every process that never builds a
/// `ServerContext` — which is every unit test and every `cargo test` binary in this repo bar two.
/// One relaxed atomic load per agent statement, no lock.
static ANY: AtomicBool = AtomicBool::new(false);

/// The live designated runtimes. A `Vec` and not a set: this holds one entry per `ServerContext`
/// alive in the process, which is one in a server and a handful in the worst test.
static DESIGNATED: Mutex<Vec<Weak<AgentRuntime>>> = Mutex::new(Vec::new());

/// Poisoning is not propagated. A thread that panicked while holding this lock left a `Vec` of
/// `Weak`s behind, which is not data anyone can corrupt into being unsafe — and taking every
/// *other* connection down on its next agent statement would be a far worse outcome than the one
/// being reported. Same reasoning as `ServerContext::catalog`.
fn registry() -> MutexGuard<'static, Vec<Weak<AgentRuntime>>> {
    match DESIGNATED.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Record `runtime` as an engine this process's statements are expected to run on.
///
/// Called from `ServerContext::new`, which is the one place a process says so.
pub(crate) fn designate(runtime: &Arc<AgentRuntime>) {
    let mut g = registry();
    // Self-cleaning, so a long-lived process that builds and drops contexts cannot grow this
    // without bound.
    g.retain(|w| w.strong_count() > 0);
    g.push(Arc::downgrade(runtime));
    // Released AFTER the push, so a reader that sees `true` sees the entry.
    ANY.store(true, Ordering::Release);
}

/// Refuse if a live `ServerContext` designated a runtime and this is not it.
///
/// Returns `Ok(())` when nothing is designated — a process with no `ServerContext` has made no
/// claim about which engine it meant, so there is nothing to contradict.
pub(crate) fn check(runtime: &Arc<AgentRuntime>) -> Result<(), FerroError> {
    if !ANY.load(Ordering::Acquire) {
        return Ok(());
    }
    let mut g = registry();
    // Upgrade to strong references and compare against THOSE. Comparing `Weak::as_ptr` would
    // compare an address that may belong to a freed allocation, and a reused address would let a
    // wrong runtime pass — a false negative in a guard, which is the direction that looks like
    // success.
    let live: Vec<Arc<AgentRuntime>> = g.iter().filter_map(Weak::upgrade).collect();
    g.retain(|w| w.strong_count() > 0);

    if live.is_empty() {
        // Every designating context has been dropped. Reset the fast path so a process that tore
        // its server down does not pay the lock on every later statement.
        //
        // ⛔ **STORED WHILE THE REGISTRY LOCK IS STILL HELD, and that is load-bearing.** `designate`
        // sets `ANY = true` under this same lock, after its push (`:91`). An earlier version of this
        // function dropped the guard FIRST and stored afterwards, which left this window:
        //
        //   T1 `check`: takes the lock, finds `live` empty, releases.
        //   T2 `designate`: takes the lock, registers a LIVE runtime, stores `ANY = true`.
        //   T1: stores `ANY = false`.
        //
        // The registry then holds a live designated runtime while `ANY` is false, so every later
        // `check` returns `Ok(())` at the fast path above without ever taking the lock — the guard
        // is silently and PERMANENTLY disabled for the process, since it re-arms only when another
        // `ServerContext` is built, which in a server is never. That is the same failure direction
        // the `Weak::as_ptr` note above names: a false negative in a guard, which looks like success.
        //
        // `live` is computed under this same hold, so keeping the store inside it costs nothing and
        // makes the pair atomic. `g` drops at the return below.
        ANY.store(false, Ordering::Release);
        return Ok(());
    }
    drop(g);
    if live.iter().any(|d| Arc::ptr_eq(d, runtime)) {
        return Ok(());
    }
    Err(FerroError::Internal(format!(
        "this agent statement is running on an AgentRuntime that no ServerContext designated. \
         {} ServerContext(s) in this process own a different runtime, so the branch catalog, page \
         store and effect log this process BUILT are not the ones this statement would touch: it \
         would run against a private in-memory stub and report plausible numbers from it. \
         This is almost always `Session::new()` where `Session::with_runtime(Arc::clone(&ctx.runtime))` \
         was meant — `Session::new` gives every session its own `AgentRuntime::new()` with \
         `storage: None`. `src/pgwire/mod.rs` is what the real server does, \
         `examples/d90_delta_vs_chunk.rs` is the worked shape for a harness, and \
         `ServerContext::session()` builds a correct one in one call. Refusing rather than \
         running, because running SUCCEEDS.",
        live.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A runtime nothing designated passes while the registry is empty, and is refused the moment
    /// a different one is designated. Both directions in one test, because a guard that has only
    /// been seen to refuse has not been shown to permit.
    ///
    /// ⚠ This test manipulates process-wide state, so it is deliberately the ONLY test in the lib
    /// binary that designates anything. The end-to-end proof — that a real `ServerContext` plus a
    /// real `Session::new()` is refused by `run()`, and a `Session::with_runtime` is not — lives in
    /// `tests/d101_designated_runtime.rs`, where it gets a process to itself.
    #[test]
    fn refuses_only_a_runtime_that_is_not_designated() {
        let undesignated = Arc::new(AgentRuntime::new());
        assert!(
            check(&undesignated).is_ok(),
            "nothing is designated yet, so nothing can be contradicted"
        );

        let designated = Arc::new(AgentRuntime::new());
        designate(&designated);

        assert!(check(&designated).is_ok(), "the designated runtime must be permitted");
        let err = check(&undesignated).expect_err("a different runtime must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("Session::with_runtime"),
            "the refusal must say what to do instead: {msg}"
        );

        // Drop the designation and the guard must fall silent again, rather than refusing every
        // statement in the process for the rest of its life.
        drop(designated);
        assert!(
            check(&undesignated).is_ok(),
            "a dropped ServerContext designates nothing"
        );
    }
}
