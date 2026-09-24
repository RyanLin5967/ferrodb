//! What a client holds between `BEGIN AGENT SESSION` and `MERGE` / `ABANDON`.
//!
//! Design authority: DESIGN.md section 0 — "the unit of isolation is an agent task, not a
//! transaction". One session is one branch and one `TxnFrame`, which is why several statements
//! by the same agent share a `TxnId` and why the dependency graph edges land on the task rather
//! than on individual statements.

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::branch::types::BranchId;
use crate::branch::BranchCatalog;
use crate::error::FerroError;
use crate::provenance::{ProvId, ProvenanceStore};
use crate::tel::ids::TxnId;

/// A fork that has happened in memory and **has not reached the disk yet**, plus the one call that
/// finishes it.
///
/// # Why this exists rather than an fsync inside the fork
///
/// `TableBranchCatalog` already implements leader/follower group commit: the first forker to reach
/// `CommitGroup::wait_durable` issues one fsync and everyone else waiting shares it. That machinery
/// was **completely inert over the wire** — `bench/d130_batch_vs_threads.txt` measured `f/sync`
/// at exactly **1.00 at every thread count from 1 to 128**, against an in-process control on the
/// same catalog rising to **17.12** — because pgwire holds `ServerContext::catalog()` for the
/// duration of a statement, so forkers serialised *in front of* the commit group and never met
/// inside it. Nothing about the fsync was slow; the callers simply arrived one at a time.
///
/// Handing the sync back to the caller as a value is what lets it happen **after** that wider lock
/// is released, which is the only change that lets a group form.
///
/// # The obligation, and why it is DISCHARGED rather than asserted
///
/// ⛔ **An earlier version of this type tried to enforce the obligation with `#[must_use]` plus a
/// `debug_assert!` in `Drop`, and BOTH were guards that could not fire.** It is recorded here
/// because the shape is this project's standing trap and it was reached again in a fresh design:
///
/// * `#[must_use]` fires on an unused **expression**. Every call site binds — `let (session,
///   durability) = …` — so the lint is satisfied, and `let (s, _) = …` would have defeated it in
///   silence.
/// * `debug_assert!` is compiled out under `--release`, and this crate has **no `[profile]` section
///   in `Cargo.toml` and no `.cargo/config.toml`**, so `debug-assertions` is off there. d130's own
///   artifact header reads `Finished 'release' profile`. ⇒ The assertion did not exist in the
///   binary that actually serves pgwire — the one place the hole mattered.
///
/// ⇒ ✅ **So `Drop` now DISCHARGES the obligation instead of complaining about it: a
/// `ForkDurability` that goes out of scope still carrying its ticket performs the sync itself.**
/// The dangerous state — a fork staged in the buffer pool that nothing ever syncs — is therefore
/// not representable, rather than merely asserted against. A forgotten `complete()` degrades to a
/// private, ungrouped fsync, which is exactly the pre-split behaviour: slower, never wrong.
///
/// [`fallback_syncs`] counts how often that net has been used. It is a plain atomic and works in
/// **release**, so it is a guard that can fire where the assertion could not, and
/// `tests/d159_fork_sync_is_deferred.rs` forces it to.
///
/// The safe spelling for anyone not holding a wider lock remains `AgentRuntime::begin_session_as`,
/// which completes it for you.
///
/// # D246: the run's record rides the same value
///
/// A fork that brings a NEW run also owes that run's provenance record. `intern` used to sync it
/// inside `begin_session_as_staged`, under `state` and inside pgwire's catalog guard, so every
/// statement waited behind one fsync per new run. Once pgserver opened the durable store (D246),
/// that was the same serialisation this type removed for the fork, reached through the provenance
/// store. So the run is interned PENDING (`ProvenanceStore::intern_pending`), and the obligation
/// to make it durable travels here: `complete()` awaits it after the fork's own sync,
/// group-committed outside the store's lock (`ProvenanceStore::await_run`), and `Drop` discharges
/// it like the fork's.
#[must_use = "a staged fork is not durable until `complete()` is called. Dropping it falls back to \
              a private, ungrouped fsync -- correct, but it forfeits the batching this type exists \
              for. NOTE: every current call site binds, so this lint cannot be relied on."]
pub struct ForkDurability {
    pub(crate) branches: Arc<dyn BranchCatalog>,
    pub(crate) seq: Option<u64>,
    /// The run this fork interned PENDING, and the store that owes its record (D246). Set on the
    /// line after the intern, so every later exit awaits it.
    pub(crate) run: Option<(Arc<dyn ProvenanceStore>, ProvId)>,
}

/// How many staged forks have been made durable by `Drop` rather than by an explicit
/// `complete()`.
///
/// **Non-zero is not a correctness failure — it is a lost batch.** The fallback syncs, so nothing
/// is unsafe; but a caller that reaches it is paying a private disk round-trip and defeating the
/// group commit this whole split exists to enable. The suite asserts it is zero across the normal
/// paths and non-zero when the net is deliberately tripped.
static FALLBACK_SYNCS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Read [`FALLBACK_SYNCS`]. Process-wide and monotonic; compare two readings, never the absolute.
pub fn fallback_syncs() -> u64 {
    FALLBACK_SYNCS.load(std::sync::atomic::Ordering::Relaxed)
}

impl ForkDurability {
    /// Wait for the shared sync covering this fork. **Call after releasing any lock wider than the
    /// runtime's own**, so concurrent forkers meet inside one fsync instead of queueing.
    pub fn complete(mut self) -> Result<(), FerroError> {
        // Taken, so the `Drop` that follows this call has nothing left to discharge and the
        // fallback counter is not touched. The error is returned to the caller, which is the whole
        // reason to prefer this over letting `Drop` do it: `Drop` cannot report one.
        let seq = self.seq.take();
        let run = self.run.take();
        let forked = self.branches.await_fork_durable(seq);
        // Awaited even when the fork's own sync failed: the run is in the store's index either
        // way, and its record must not be left for whichever later write happens to carry it.
        let interned = match run {
            Some((store, prov)) => store.await_run(prov),
            None => Ok(()),
        };
        forked.and(interned)
    }

    /// `complete()`, for the statement that has just opened `agent` on this fork
    /// (`BEGIN AGENT SESSION`): when the sync fails, the session is closed as well as the statement
    /// refused.
    ///
    /// Dispatch installs the session before any caller can complete the fork, so a failed
    /// `complete()?` left the connection inside a session whose `BEGIN` it had just refused, and a
    /// retried `BEGIN` was then refused as nested (D246 A4, review D-5). The branch itself is left
    /// to its lease, as a disconnected client's is.
    pub fn complete_for(self, agent: &mut Option<AgentSession>) -> Result<(), FerroError> {
        let completed = self.complete();
        if completed.is_err() {
            *agent = None;
        }
        completed
    }
}

impl Drop for ForkDurability {
    fn drop(&mut self) {
        if let Some(seq) = self.seq.take() {
            FALLBACK_SYNCS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // ⚠ STATED BLIND SPOT: a failure here cannot be reported — `Drop` has nowhere to put
            // it. That is strictly better than not syncing at all, and it is only reachable on a
            // path that already forgot `complete()`. On the normal path `complete()` returns the
            // error.
            let _ = self.branches.await_fork_durable(Some(seq));
        }
        // D246: the run's record, discharged the same way. [`fallback_syncs`] counts only the
        // fork's ticket, which every staged fork on a table catalog carries, so a forgotten
        // `complete()` there is counted once. On a catalog that does not split (no ticket) this
        // await is not counted. `await_run` refuses a poisoned file lock rather than panicking on
        // it; the only unwraps left on this path are `CommitGroup`'s, on a mutex no code panics
        // while holding.
        if let Some((store, prov)) = self.run.take() {
            let _ = store.await_run(prov);
        }
    }
}

impl std::fmt::Debug for ForkDurability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkDurability")
            .field("seq", &self.seq)
            .field("run", &self.run.as_ref().map(|(_, prov)| *prov))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSession {
    pub branch: BranchId,
    /// The name this branch answers to in SQL (`AS OF BRANCH b_3`).
    pub branch_name: String,
    pub agent_id: String,
    pub run_id: String,
    /// The interned run entity: which agent + run + model wrote every row on this branch.
    pub prov: ProvId,
    /// One frame per task, not per statement.
    pub txn: TxnId,
}

impl Display for AgentSession {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "agent session {} on {} (agent={} run={})",
            self.branch_name, self.branch, self.agent_id, self.run_id
        )
    }
}
