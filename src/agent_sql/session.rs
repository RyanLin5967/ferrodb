//! What a client holds between `BEGIN AGENT SESSION` and `MERGE` / `ABANDON`.
//!
//! Design authority: DESIGN.md section 0 — "the unit of isolation is an agent task, not a
//! transaction". One session is one branch and one `TxnFrame`, which is why several statements
//! by the same agent share a `TxnId` and why the dependency graph edges land on the task rather
//! than on individual statements.

use std::fmt::{Display, Formatter};

use crate::branch::types::BranchId;
use crate::provenance::ProvId;
use crate::tel::ids::TxnId;

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
