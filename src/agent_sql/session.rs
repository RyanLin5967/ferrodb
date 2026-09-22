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
use crate::provenance::ProvId;
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
#[must_use = "a staged fork is not durable until `complete()` is called. Dropping it falls back to \
              a private, ungrouped fsync -- correct, but it forfeits the batching this type exists \
              for. NOTE: every current call site binds, so this lint cannot be relied on."]
pub struct ForkDurability {
    pub(crate) branches: Arc<dyn BranchCatalog>,
    pub(crate) seq: Option<u64>,
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
        self.branches.await_fork_durable(seq)
    }
}

impl Drop for ForkDurability {
    fn drop(&mut self) {
        let Some(seq) = self.seq.take() else { return };
        FALLBACK_SYNCS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // ⚠ STATED BLIND SPOT: a failure here cannot be reported — `Drop` has nowhere to put it.
        // That is strictly better than not syncing at all, and it is only reachable on a path that
        // already forgot `complete()`. On the normal path `complete()` returns the error.
        let _ = self.branches.await_fork_durable(Some(seq));
    }
}

impl std::fmt::Debug for ForkDurability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkDurability").field("seq", &self.seq).finish()
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
