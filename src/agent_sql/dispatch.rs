//! Statement dispatch: parsed agent SQL -> binder -> runtime.
//!
//! Design authority: DESIGN.md section 5.
//!
//! This is the only place that knows both the `Stmt` shapes and the runtime, which keeps the
//! parser free of branch identities and the runtime free of SQL text.

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::agent_sql::changeset::{ChangeSet, MergeReport};
use crate::agent_sql::simulate::SimulationReport;
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
    /// `SIMULATE` — every candidate's score and what became of it.
    Simulation(Box<SimulationReport>),
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
            AgentOutput::Simulation(s) => write!(f, "{}", s),
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
        | Stmt::RevertMerge { .. }
        | Stmt::Simulate { .. } => true,
        Stmt::Select { from, .. } => from.as_of.is_some(),
        _ => false,
    }
}

/// `ALTER TABLE` inside an open agent session — B11.
///
/// Recorded on the branch as a **pending** schema edit rather than applied to the shared catalog.
/// Applying it immediately would be the one thing an agent session exists to prevent: a branch's
/// writes are invisible to main and its siblings until `MERGE`, and a schema is the most visible
/// write there is — every other connection would see the column appear the moment one agent typed
/// the statement, and abandoning that agent's branch would not take it away.
///
/// The edit is published at `MERGE`, after the schema merge has decided that it composes with
/// whatever the target's shape has become in the meantime.
pub fn run_agent_alter(
    table: String,
    action: crate::parser::parser::AlterAction,
    catalog: &mut Catalog,
    _txn: Arc<TxnManager>,
    session: &mut Session,
) -> Result<Outcome, FerroError> {
    let branch = session
        .agent
        .as_ref()
        .map(|a| a.branch)
        .ok_or_else(|| FerroError::Txn("no agent session is open".into()))?;
    let runtime = session.runtime.clone();
    runtime.stage_schema_edit(catalog, branch, &table, &action)?;
    Ok(Outcome::Agent(AgentOutput::Affected(0)))
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
        BoundAgentStmt::BeginAgentSession { agent_id, run_id, model, parent } => {
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
            let model = model.as_ref().map(|(n, v)| (n.as_str(), v.as_str()));
            let s = runtime.begin_session_with_model(&agent_id, run_id.as_deref(), model, parent)?;
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
        BoundAgentStmt::Simulate { base, plan } => {
            // **The same refusal `BEGIN AGENT SESSION` makes, for a sharper reason: SIMULATE
            // PUBLISHES.** `executor::run` dispatches agent statements before it reaches the
            // transaction arms, and admitting a candidate publishes it in `publish_evaluation`'s
            // OWN transaction. So inside `BEGIN ... ROLLBACK` the admissions would commit and the
            // ROLLBACK could not undo them: the client would see a rolled-back block that
            // permanently changed the database. A statement that quietly escapes the enclosing
            // transaction is worse than one that refuses to run inside it.
            if session.current.is_some() {
                return Err(FerroError::Txn(
                    "cannot SIMULATE inside a transaction block: admitting a candidate publishes                      it in its own transaction, so a ROLLBACK here would not undo it. COMMIT or                      ROLLBACK first."
                        .into(),
                ));
            }
            let report = runtime.simulate(&mut ctx, base, &plan)?;
            Ok(Outcome::Agent(AgentOutput::Simulation(Box::new(report))))
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
