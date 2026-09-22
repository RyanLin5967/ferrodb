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
/// # The obligation
///
/// ⛔ **A `ForkDurability` that is dropped without `complete()` is a fork the client may have been
/// told about and a crash would lose.** `#[must_use]` makes ignoring the return value a warning,
/// and the debug assertion in `Drop` turns a path that stashes and forgets it into a test failure
/// rather than a silent durability hole. The safe spelling for anyone not holding a wider lock is
/// `AgentRuntime::begin_session_as`, which completes it for you and cannot forget.
#[must_use = "a staged fork is not durable until `complete()` is called; dropping this silently \
              loses the fsync the client was promised"]
pub struct ForkDurability {
    pub(crate) branches: Arc<dyn BranchCatalog>,
    pub(crate) seq: Option<u64>,
}

impl ForkDurability {
    /// Wait for the shared sync covering this fork. **Call after releasing any lock wider than the
    /// runtime's own**, so concurrent forkers meet inside one fsync instead of queueing.
    pub fn complete(mut self) -> Result<(), FerroError> {
        // Taken, so `Drop` sees `None` and does not fire its assertion — including on the error
        // path, where the fork is genuinely not durable but the caller is being told so.
        let seq = self.seq.take();
        self.branches.await_fork_durable(seq)
    }
}

impl Drop for ForkDurability {
    fn drop(&mut self) {
        debug_assert!(
            self.seq.is_none(),
            "a staged fork was dropped without `ForkDurability::complete()`: the branch is in the \
             buffer pool but no fsync covers it, so a crash here loses a branch the client was \
             told about"
        );
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
