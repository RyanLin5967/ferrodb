//! Statement dispatch: parsed agent SQL -> binder -> runtime.
//!
//! Design authority: DESIGN.md section 5.
//!
//! This is the only place that knows both the `Stmt` shapes and the runtime, which keeps the
//! parser free of branch identities and the runtime free of SQL text.

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::agent_sql::changeset::{ChangeSet, MergeReport};
use crate::agent_sql::runtime::ExecCtx;
use crate::agent_sql::session::AgentSession;
use crate::binder::binder::{Binder, BoundAgentStmt};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::error::FerroError;
use crate::execution::executor::Outcome;
use crate::execution::session::Session;
use crate::parser::parser::Stmt;
use crate::provenance::revert::{RevertMode, RevertPlan};
use crate::wal::txn::TxnManager;

/// What an agent statement returns. Structured throughout — `DIFF` and `MERGE` in particular are
/// data the caller can act on, never rendered text (DESIGN.md exit criteria 4 and 5).
#[derive(Debug, Clone)]
pub enum AgentOutput {
    SessionStarted(AgentSession),
    Diff(ChangeSet),
    Merge(MergeReport),
    Abandoned { branch: String },
    Revert(RevertPlan),
    Affected(usize),
}

impl Display for AgentOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentOutput::SessionStarted(s) => write!(f, "{}", s),
            AgentOutput::Diff(d) => write!(f, "{}", d),
            AgentOutput::Merge(m) => write!(f, "{}", m),
            AgentOutput::Abandoned { branch } => write!(f, "abandoned {}", branch),
            AgentOutput::Revert(p) => {
                if p.is_blocked() {
                    write!(
                        f,
                        "revert of {} HALTED: {} downstream transaction(s) depend on it: {:?}",
                        p.target,
                        p.blocked_by.len(),
                        p.blocked_by
                    )
                } else {
                    write!(
                        f,
                        "reverted {} ({} cascaded: {:?})",
                        p.target,
                        p.cascade.len(),
                        p.cascade
                    )
                }
            }
            AgentOutput::Affected(n) => write!(f, "({} row{} affected on branch)", n, if *n == 1 { "" } else { "s" }),
        }
    }
}

/// True for statements this module owns.
pub fn is_agent_stmt(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::BeginAgentSession { .. }
        | Stmt::Diff { .. }
        | Stmt::Merge { .. }
        | Stmt::Abandon { .. }
        | Stmt::RevertMerge { .. } => true,
        Stmt::Select { from, .. } => from.as_of.is_some(),
        _ => false,
    }
}

/// Bind and run one agent statement.
pub fn run_agent_stmt(
    stmt: Stmt,
    catalog: &mut Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: &mut Session,
) -> Result<Outcome, FerroError> {
    let runtime = session.runtime.clone();
    let current = session.agent.as_ref().map(|a| a.branch);
    let bound = Binder::new(catalog).bind_agent(&stmt, runtime.as_ref(), current)?;
    let mut ctx = ExecCtx { catalog, bp, txn };

    match bound {
        BoundAgentStmt::BeginAgentSession { agent_id, run_id, parent } => {
            if session.agent.is_some() {
                return Err(FerroError::Txn(
                    "an agent session is already open on this connection".into(),
                ));
            }
            if session.current.is_some() {
                return Err(FerroError::Txn(
                    "cannot begin an agent session inside a transaction block".into(),
                ));
            }
            let s = runtime.begin_session(&agent_id, run_id.as_deref(), parent)?;
            session.agent = Some(s.clone());
            Ok(Outcome::Agent(AgentOutput::SessionStarted(s)))
        }
        BoundAgentStmt::Diff { branch } => {
            Ok(Outcome::Agent(AgentOutput::Diff(runtime.diff(&mut ctx, branch)?)))
        }
        BoundAgentStmt::Merge { branch } => {
            let report = runtime.merge(&mut ctx, branch)?;
            // A conflicting merge publishes nothing and leaves the branch alive, so the agent can
            // fix the violated predicate and merge again.
            if report.applied_to_target && current == Some(branch) {
                session.agent = None;
            }
            Ok(Outcome::Agent(AgentOutput::Merge(report)))
        }
        BoundAgentStmt::Abandon { branch } => {
            runtime.abandon(branch)?;
            let name = session
                .agent
                .as_ref()
                .filter(|a| a.branch == branch)
                .map(|a| a.branch_name.clone())
                .unwrap_or_else(|| branch.to_string());
            if current == Some(branch) {
                session.agent = None;
            }
            Ok(Outcome::Agent(AgentOutput::Abandoned { branch: name }))
        }
        BoundAgentStmt::RevertMerge { merge_id, mode } => {
            let plan = runtime.revert_merge(&mut ctx, &merge_id, mode)?;
            debug_assert!(matches!(plan.mode, RevertMode::Halt | RevertMode::Cascade));
            Ok(Outcome::Agent(AgentOutput::Revert(plan)))
        }
        BoundAgentStmt::SelectAsOf { branch, stmt } => {
            let rows = runtime.select(&mut ctx, branch, &stmt, current)?;
            Ok(Outcome::Rows(rows))
        }
    }
}

/// Run a statement issued *inside* an agent session.
///
/// Reads see the branch's own uncommitted state; writes land in the branch's buffer and are
/// invisible to main and to sibling branches until `MERGE` (exit criterion 2).
pub fn run_in_session(
    stmt: Stmt,
    catalog: &mut Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: &mut Session,
) -> Result<Outcome, FerroError> {
    let runtime = session.runtime.clone();
    let branch = match &session.agent {
        Some(a) => a.branch,
        None => return Err(FerroError::Branch("no agent session on this connection".into())),
    };
    let mut ctx = ExecCtx { catalog, bp, txn };
    match stmt {
        s @ Stmt::Select { .. } => {
            let rows = runtime.select(&mut ctx, branch, &s, Some(branch))?;
            Ok(Outcome::Rows(rows))
        }
        s => Ok(Outcome::Affected(runtime.write(&mut ctx, branch, s)?)),
    }
}
