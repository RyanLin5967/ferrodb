use std::sync::Arc;

use crate::agent_sql::runtime::AgentRuntime;
use crate::agent_sql::session::AgentSession;

pub struct Session {
    /// The open ordinary transaction, if any.
    pub current: Option<u64>,
    /// The open agent session, if any. An agent session is a *branch*, not a transaction: it
    /// outlives individual statements and its writes stay invisible to main until `MERGE`.
    pub agent: Option<AgentSession>,
    /// Shared with sibling sessions so two agents can fork, write and merge against the same
    /// branch catalog. `Session::new` gives a connection its own runtime; connections that must
    /// see each other's branches are built with `Session::with_runtime`.
    pub runtime: Arc<AgentRuntime>,
}

impl Session {
    pub fn new() -> Self {
        Self { current: None, agent: None, runtime: Arc::new(AgentRuntime::new()) }
    }

    /// A session sharing an existing agent runtime — one process, several agents.
    pub fn with_runtime(runtime: Arc<AgentRuntime>) -> Self {
        Self { current: None, agent: None, runtime }
    }
}

impl Default for Session {
    fn default() -> Self {
        Session::new()
    }
}
