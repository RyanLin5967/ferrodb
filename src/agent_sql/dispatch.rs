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
use crate::binder::binder::{Binder, BoundAgentStmt, BoundColumn};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::{DataType, Value};
use crate::catalog::system_views::NamedRows;
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

/// Column helpers for [`AgentOutput::to_rows`]. `qualifier` is the statement that produced the
/// result, so a client sees `merge.outcome` rather than a bare `outcome` when it asks for
/// qualifiers.
fn col(qualifier: &str, name: &str, data_type: DataType, nullable: bool) -> BoundColumn {
    BoundColumn { qualifier: qualifier.into(), name: name.into(), data_type, nullable }
}

/// A `u64` that does not fit `i64`, as exact digits.
///
/// `RowId` is `fnv64` of the primary key for any non-integer key, so it is uniformly distributed over
/// the whole `u64` range and about half of them are negative as `i64`. `Value::Decimal` holds digits
/// verbatim and goes out as `numeric`, so nothing is misreported. Same decision, same reason, as
/// `catalog::system_views`.
fn u64_digits(v: u64) -> Value {
    Value::Decimal(v.to_string())
}

fn text(v: impl Into<String>) -> Value {
    Value::Varchar(v.into())
}

impl AgentOutput {
    /// The typed form of an agent statement's result: named, declared columns and rows.
    ///
    /// # Why this exists beside `Display`
    ///
    /// `Display` renders a sentence for a person at a terminal. Until B9 the **wire** used
    /// `format!("{self:?}")` in a single `text` column, which is a Rust `Debug` literal: a client
    /// received one opaque string and could not read a field out of it without parsing Rust. That
    /// contradicted the thing `DIFF` and `MERGE` are for — DESIGN.md exit criteria 4 and 5 call them
    /// "structured throughout ... never rendered text" — because the structure stopped at the socket.
    ///
    /// The two renderings are deliberately not one: collapsing them would either give the CLI a table
    /// where a sentence is wanted, or give a driver a sentence where columns are wanted.
    ///
    /// # Every variant returns at least one row, except one
    ///
    /// `Diff` returns one row per changed row and therefore **zero rows for an empty changeset**,
    /// which is the honest answer: nothing changed, and the column list still arrives so a client can
    /// tell that from a failure. Every other variant returns exactly one row, and `Merge` returns one
    /// row per merged row *plus* a one-row form when there were none — because a merge always has a
    /// verdict, and `applied_to_target = false` with no rows is the single most important thing a
    /// merge can say (a gate held it, so the target is untouched). A shape that returned zero rows
    /// there would drop the verdict.
    pub fn to_rows(&self) -> NamedRows {
        match self {
            AgentOutput::SessionStarted(s) => {
                let q = "session";
                NamedRows::new(
                    vec![
                        col(q, "branch_id", DataType::BigInt, false),
                        col(q, "generation", DataType::Integer, false),
                        col(q, "branch_name", DataType::Varchar(32), false),
                        col(q, "agent_id", DataType::Varchar(64), false),
                        col(q, "run_id", DataType::Varchar(64), false),
                        col(q, "prov_id", DataType::Integer, false),
                        col(q, "txn_id", DataType::BigInt, false),
                    ],
                    vec![vec![
                        Value::BigInt(s.branch.id as i64),
                        Value::Integer(s.branch.generation as i32),
                        text(s.branch_name.clone()),
                        text(s.agent_id.clone()),
                        text(s.run_id.clone()),
                        Value::Integer(s.prov.0 as i32),
                        Value::BigInt(s.txn.0 as i64),
                    ]],
                )
            }
            AgentOutput::Diff(d) => {
                let q = "diff";
                let columns = vec![
                    col(q, "from_branch", DataType::Varchar(32), false),
                    col(q, "into_branch", DataType::Varchar(32), false),
                    col(q, "table_name", DataType::Varchar(64), false),
                    col(q, "row_id", DataType::Decimal, false),
                    col(q, "change", DataType::Varchar(8), false),
                    col(q, "outcome", DataType::Varchar(32), false),
                    col(q, "ops", DataType::Integer, false),
                    col(q, "op_kinds", DataType::Varchar(128), true),
                    col(q, "guards", DataType::Integer, false),
                    col(q, "guard_predicates", DataType::Varchar(512), true),
                ];
                let rows = d
                    .rows
                    .iter()
                    .map(|r| {
                        let kinds: Vec<&str> = r.ops.iter().map(|o| o.kind.name()).collect();
                        let preds: Vec<String> =
                            r.guards.iter().map(|g| g.violated_predicate()).collect();
                        vec![
                            text(d.from.to_string()),
                            text(d.to.to_string()),
                            text(r.table.clone()),
                            u64_digits(r.row.0),
                            text(r.kind.to_string()),
                            text(r.outcome.to_string()),
                            Value::Integer(r.ops.len() as i32),
                            if kinds.is_empty() { Value::Null } else { text(kinds.join(",")) },
                            Value::Integer(r.guards.len() as i32),
                            if preds.is_empty() { Value::Null } else { text(preds.join("; ")) },
                        ]
                    })
                    .collect();
                NamedRows::new(columns, rows)
            }
            AgentOutput::Merge(m) => {
                let q = "merge";
                let columns = vec![
                    col(q, "merge_id", DataType::Varchar(16), false),
                    col(q, "from_branch", DataType::Varchar(32), false),
                    col(q, "into_branch", DataType::Varchar(32), false),
                    col(q, "outcome", DataType::Varchar(24), false),
                    col(q, "applied_to_target", DataType::Boolean, false),
                    col(q, "blind_writes", DataType::Integer, false),
                    // NULL on the summary-only row: a merge with no per-row outcomes still has a
                    // verdict, and the verdict is what the columns above carry.
                    col(q, "table_name", DataType::Varchar(64), true),
                    col(q, "row_id", DataType::Decimal, true),
                    col(q, "row_outcome", DataType::Varchar(24), true),
                    col(q, "violated_predicate", DataType::Varchar(512), true),
                ];
                let head = |extra: Vec<Value>| {
                    let mut v = vec![
                        text(m.merge_id.clone()),
                        text(m.from.to_string()),
                        text(m.into.to_string()),
                        text(m.outcome.name()),
                        Value::Boolean(m.applied_to_target),
                        Value::Integer(m.blind_writes.len() as i32),
                    ];
                    v.extend(extra);
                    v
                };
                let rows = if m.rows.is_empty() {
                    vec![head(vec![Value::Null, Value::Null, Value::Null, Value::Null])]
                } else {
                    m.rows
                        .iter()
                        .map(|r| {
                            let preds = r.violated_predicates();
                            head(vec![
                                text(r.table.clone()),
                                u64_digits(r.row.0),
                                text(r.outcome.name()),
                                if preds.is_empty() { Value::Null } else { text(preds.join("; ")) },
                            ])
                        })
                        .collect()
                };
                NamedRows::new(columns, rows)
            }
            AgentOutput::Abandoned { branch } => NamedRows::new(
                vec![col("abandon", "branch", DataType::Varchar(32), false)],
                vec![vec![text(branch.clone())]],
            ),
            AgentOutput::Revert(p) => {
                let q = "revert";
                let ids = |v: &[crate::tel::ids::TxnId]| {
                    if v.is_empty() {
                        Value::Null
                    } else {
                        text(v.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(","))
                    }
                };
                NamedRows::new(
                    vec![
                        col(q, "target_txn", DataType::BigInt, false),
                        col(q, "mode", DataType::Varchar(8), false),
                        // The load-bearing column. `Halt` with dependents changed NOTHING, and a
                        // client that could not tell that apart from a completed revert would report
                        // work as undone that is still there.
                        col(q, "blocked", DataType::Boolean, false),
                        col(q, "blocked_by", DataType::Varchar(256), true),
                        col(q, "cascaded", DataType::Varchar(256), true),
                    ],
                    vec![vec![
                        Value::BigInt(p.target.0 as i64),
                        text(match p.mode {
                            RevertMode::Halt => "HALT",
                            RevertMode::Cascade => "CASCADE",
                        }),
                        Value::Boolean(p.is_blocked()),
                        ids(&p.blocked_by),
                        ids(&p.cascade),
                    ]],
                )
            }
            AgentOutput::Affected(n) => NamedRows::new(
                vec![col("branch", "rows_affected", DataType::BigInt, false)],
                vec![vec![Value::BigInt(*n as i64)]],
            ),
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
