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
            // The seventh variant. B9 wrote `to_rows` as a six-arm exhaustive match with no
            // wildcard, and `Simulation` arrived later on this branch — so the compiler, not a
            // reviewer, is what forced this arm to exist. That is the shape B9 intended: a new
            // `AgentOutput` cannot reach the wire as a silently-missing row.
            //
            // ONE ROW PER CANDIDATE, not one row summarising the report. `SimulationReport`'s own
            // doc says it is "Structured, never rendered text", and a `SIMULATE` whose result
            // collapsed to a single blob would lose exactly what it is for: which candidates were
            // admitted, and which were scored admissible and then refused when an earlier
            // admission moved the base. `error` and `merge_id` are nullable because a candidate
            // that failed to run is never scored and never admitted, and only an admitted one has
            // a merge id.
            AgentOutput::Simulation(r) => {
                let q = "simulation";
                NamedRows::new(
                    vec![
                        col(q, "candidate", DataType::Varchar(64), false),
                        col(q, "branch_name", DataType::Varchar(32), false),
                        col(q, "run_id", DataType::Varchar(64), false),
                        col(q, "rows_written", DataType::BigInt, false),
                        col(q, "rows_read", DataType::BigInt, false),
                        col(q, "admitted", DataType::Boolean, false),
                        col(q, "refused_after_recheck", DataType::Boolean, false),
                        col(q, "merge_id", DataType::Varchar(64), true),
                        col(q, "error", DataType::Varchar(256), true),
                    ],
                    r.candidates
                        .iter()
                        .map(|c| {
                            vec![
                                text(c.name.clone()),
                                text(c.branch_name.clone()),
                                text(c.run_id.clone()),
                                Value::BigInt(c.rows_written as i64),
                                Value::BigInt(c.rows_read as i64),
                                Value::Boolean(c.admitted),
                                Value::Boolean(c.refused_after_recheck()),
                                c.merge_id.clone().map(text).unwrap_or(Value::Null),
                                c.error.clone().map(text).unwrap_or(Value::Null),
                            ]
                        })
                        .collect(),
                )
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_sql::changeset::{ChangeOutcome, RowChange, RowChangeKind, RowMergeOutcome};
    use crate::branch::types::BranchId;
    use crate::provenance::ProvId;
    use crate::tel::ids::{RowId, TableId, TxnId};
    use crate::tel::merge::MergeOutcome;

    fn session() -> AgentSession {
        AgentSession {
            branch: BranchId::new(3, 0),
            branch_name: "b_3".into(),
            agent_id: "a".into(),
            run_id: "r".into(),
            prov: ProvId(1),
            txn: TxnId(7),
        }
    }

    fn row_change() -> RowChange {
        RowChange {
            table: "t".into(),
            tbl: TableId(1),
            row: RowId(u64::MAX),
            kind: RowChangeKind::Update,
            ops: Vec::new(),
            before: None,
            after: None,
            guards: Vec::new(),
            outcome: ChangeOutcome::Pending,
        }
    }

    fn merge_report(rows: Vec<RowMergeOutcome>) -> MergeReport {
        MergeReport {
            merge_id: "m_1".into(),
            from: BranchId::new(3, 0),
            into: BranchId::TRUNK,
            outcome: MergeOutcome::Clean,
            rows,
            blind_writes: Vec::new(),
            // B11's field, which B9's fixture predates. Empty rather than populated: this helper
            // builds a ROW-merge report for the `to_rows` shape tests, and a schema report here
            // would assert a schema merge that never happened.
            schema: Vec::new(),
            applied_to_target: false,
        }
    }

    /// One row per variant, so nothing is covered by inspection alone.
    fn every_variant() -> Vec<(&'static str, AgentOutput)> {
        vec![
            ("SessionStarted", AgentOutput::SessionStarted(session())),
            ("Diff (empty)", AgentOutput::Diff(ChangeSet {
                from: BranchId::new(3, 0), to: BranchId::TRUNK, rows: Vec::new(),
            })),
            ("Diff (one row)", AgentOutput::Diff(ChangeSet {
                from: BranchId::new(3, 0), to: BranchId::TRUNK, rows: vec![row_change()],
            })),
            ("Merge (no rows)", AgentOutput::Merge(merge_report(Vec::new()))),
            ("Merge (one row)", AgentOutput::Merge(merge_report(vec![RowMergeOutcome {
                table: "t".into(),
                tbl: TableId(1),
                row: RowId(u64::MAX),
                outcome: MergeOutcome::Clean,
                applied: Vec::new(),
                discarded: Vec::new(),
                conflicts: Vec::new(),
            }]))),
            ("Abandoned", AgentOutput::Abandoned { branch: "b_3".into() }),
            ("Revert (halted)", AgentOutput::Revert(RevertPlan {
                target: TxnId(4), mode: RevertMode::Halt,
                blocked_by: vec![TxnId(5)], cascade: Vec::new(),
            })),
            ("Revert (cascaded)", AgentOutput::Revert(RevertPlan {
                target: TxnId(4), mode: RevertMode::Cascade,
                blocked_by: Vec::new(), cascade: vec![TxnId(5), TxnId(6)],
            })),
            ("Affected", AgentOutput::Affected(3)),
        ]
    }

    /// **Every row is exactly as wide as the declared column list.**
    ///
    /// The breaking shape is a variant whose rows and columns are built in two separate places — which
    /// is every variant here. A row one value short shifts every value after it under the wrong column
    /// NAME for the rest of the result, and the wire cannot detect that: `DataRow` carries a count, so
    /// a short row is a well-formed message a client reads as valid data. The integration test covers
    /// three variants because those are the three a SQL session produces; this covers all of them,
    /// including the two `Revert` shapes and the empty/populated `Diff` and `Merge` pairs.
    #[test]
    fn every_agent_output_row_matches_its_declared_column_count() {
        for (what, out) in every_variant() {
            let t = out.to_rows();
            assert!(!t.columns.is_empty(), "{what} declared no columns");
            for (i, r) in t.rows.iter().enumerate() {
                assert_eq!(
                    r.len(),
                    t.columns.len(),
                    "{what} row {i} has {} values against {} columns",
                    r.len(),
                    t.columns.len()
                );
            }
            let mut names: Vec<&str> = t.columns.iter().map(|c| c.name.as_str()).collect();
            names.sort_unstable();
            let before = names.len();
            names.dedup();
            assert_eq!(before, names.len(), "{what} declares a duplicate column name");
        }
    }

    /// **A verdict is never dropped for want of a row to hang it on.**
    ///
    /// `MERGE` with no per-row outcomes still has to report `applied_to_target`, and a merge the gate
    /// held is exactly that shape — nothing published, target untouched. A per-row-only rendering
    /// returns zero rows there and the single most important thing the statement can say is gone.
    /// `DIFF` is the deliberate exception: no changes means no rows, and the column list is what tells
    /// a client that apart from a failure.
    #[test]
    fn only_an_empty_diff_returns_no_rows() {
        for (what, out) in every_variant() {
            let t = out.to_rows();
            if what == "Diff (empty)" {
                assert!(t.is_empty(), "an untouched branch reported a change");
                assert!(t.column_index("change").is_some(), "{:?}", t.header());
            } else {
                assert!(!t.is_empty(), "{what} returned no rows, so its result is unreadable");
            }
        }
        // The held-merge shape specifically: one row, the verdict present, the row columns NULL.
        let t = AgentOutput::Merge(merge_report(Vec::new())).to_rows();
        assert_eq!(t.rows.len(), 1);
        let at = t.column_index("applied_to_target").expect("declared");
        assert!(matches!(t.rows[0][at], Value::Boolean(false)), "{:?}", t.rows[0][at]);
        let at = t.column_index("table_name").expect("declared");
        assert!(matches!(t.rows[0][at], Value::Null), "{:?}", t.rows[0][at]);
    }

    /// A `RowId` above `i64::MAX` — what `fnv64` of a varchar key produces about half the time — must
    /// reach a client as its digits, not as a negative number.
    #[test]
    fn a_row_id_past_i64_max_is_not_reported_as_negative() {
        assert!((u64::MAX as i64) < 0, "the cast this avoids");
        let t = AgentOutput::Diff(ChangeSet {
            from: BranchId::new(3, 0),
            to: BranchId::TRUNK,
            rows: vec![row_change()],
        })
        .to_rows();
        let at = t.column_index("row_id").expect("declared");
        match &t.rows[0][at] {
            Value::Decimal(d) => assert_eq!(d, "18446744073709551615"),
            other => panic!("a row id past i64::MAX rendered as {other:?}"),
        }
    }
}
