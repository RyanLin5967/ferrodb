//! The agent-session runtime behind the SQL surface.
//!
//! Design authority: DESIGN.md sections 1-3 and exit criteria 2, 3, 4, 5, 6, 7, 9, 10.
//!
//! One agent task = one branch = one `TxnFrame`. `BEGIN AGENT SESSION` forks a branch and interns
//! the run; every write the session makes lands in that branch's private buffer, never in the
//! shared tables, so it is invisible to main and to sibling branches until `MERGE` publishes it
//! (exit criterion 2). `SELECT ... AS OF BRANCH b` reads that buffer, which is how another
//! branch's *uncommitted* state becomes visible on request (exit criterion 3).
//!
//! **What is real here and what is a stand-in**, stated so nobody reads more into a green test
//! than it proves:
//! - Branch metadata, forking and reaping go through the shared `BranchCatalog` trait. The
//!   in-memory implementation holds no pages, so nothing here demonstrates the *page-count*
//!   criteria (1 and 8) — those are the durable branch engine's.
//! - A branch's uncommitted rows live in an in-memory per-branch buffer, which is the write
//!   buffer the design calls for ("probed before descent") in row terms rather than page terms.
//!   The copy-on-write page store replaces it without the SQL layer changing shape.
//! - `RowId` is derived from the primary key by [`row_id_of`] because no layer mints surrogate
//!   row ids yet. The design is explicit that the PK is a constraint and not identity; when the
//!   storage layer mints real surrogates this function is the single place to change.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::agent_sql::changeset::{
    ChangeOutcome, ChangeSet, MergeReport, RowChange, RowChangeKind, RowMergeOutcome,
};
use crate::branch::catalog::LogBranchCatalog;
use crate::tel::MemEffectLog;
use crate::agent_sql::merge_engine::{
    apply_op, check_guards, compose_ops, invert, resolve_cell, CellMerge, CellResolution,
    CellState, PolicyTable,
};
use crate::agent_sql::session::AgentSession;
use crate::binder::binder::{Binder, Scope};
use crate::branch::types::{BranchId, CommitHash, LeaseDeadline};
use crate::branch::BranchCatalog;

/// Root page the trunk branch starts at. The CoW store publishes a real root over this on the
/// first write; until then it is only an identity for the trunk record.
const TRUNK_ROOT_PAGE: u32 = 1;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::execution::executor::evaluate;
use crate::parser::parser::{Expr, Stmt, TableRef};
use crate::parser::scanner::TokenType;
use crate::planner::plan::{plan, Plan};
use crate::provenance::readset::{AccessShape, PredicateSummary, VersionRef};
use crate::provenance::revert::{DependencyGraphBuilder, RevertMode, RevertPlan};
use crate::provenance::{ProvId, RunEntity};
use crate::storage::heap_file_manager::RecordId;
use crate::tel::frame::TxnFrame;
use crate::tel::guard::{ArithOp, CmpOp, Guard, GuardExpr};
use crate::tel::ids::{ColId, RowId, TableId, TxnId};
use crate::tel::merge::{ConflictKind, ConflictReport, MergeOutcome, MergePolicy};
use crate::tel::op::{Delta, Op, OpKind};
use crate::tel::EffectLog;
use crate::wal::txn::{ReadView, TxnManager};

/// Default lease on an agent branch. Leases are non-cooperative: expiry does not require the
/// client to call anything (DESIGN.md exit criterion 8).
pub const DEFAULT_LEASE_MILLIS: u64 = 15 * 60 * 1000;

/// Everything the runtime needs to reach the shared tables.
pub struct ExecCtx<'a> {
    pub catalog: &'a mut Catalog,
    pub bp: Arc<BufferPoolManager>,
    pub txn: Arc<TxnManager>,
}

/// Table identity, derived from the table name (FNV-1a).
///
/// The catalog stores tables by name and mints no ids; hashing the name is stable across
/// processes, which an assignment counter would not be.
pub fn table_id(name: &str) -> TableId {
    let mut h: u32 = 0x811c_9dc5;
    for b in name.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    TableId(h)
}

fn fnv64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Row identity from the primary key.
///
/// A stand-in for a surrogate minted at insert: the design is explicit that the PK is a
/// *constraint*, not identity, so updating a PK here would look like a delete plus an insert.
/// Every caller goes through this function so there is one place to change.
pub fn row_id_of(row: &[Value]) -> RowId {
    match row.first() {
        Some(Value::Integer(i)) => RowId(*i as i64 as u64),
        Some(Value::Varchar(s)) => RowId(fnv64(s.as_bytes())),
        Some(Value::Boolean(b)) => RowId(fnv64(&[*b as u8])),
        Some(Value::Float(f)) => RowId(fnv64(&f.to_bits().to_be_bytes())),
        Some(Value::Null) | None => RowId(0),
    }
}

/// The state of one row on a branch.
#[derive(Debug, Clone, PartialEq)]
enum RowState {
    Present(Vec<Value>),
    Deleted,
}

/// One agent task's private workspace: its uncommitted rows, its frame, its read-set.
struct Workspace {
    name: String,
    /// The interned run: which agent + run + model owns every write on this branch.
    prov: ProvId,
    txn: TxnId,
    /// Apply-sequence of the target at fork time. Anything applied after this is concurrent with
    /// us, which is what makes the three-way comparison well defined.
    fork_seq: u64,
    rows: BTreeMap<(u32, u64), RowState>,
    /// Image at first touch = the fork-point value. `None` means the row did not exist.
    base_rows: BTreeMap<(u32, u64), Option<Vec<Value>>>,
    tables: BTreeMap<u32, String>,
    frame: TxnFrame,
    reads: Vec<crate::provenance::readset::ReadSet>,
}

impl Workspace {
    fn key(tbl: TableId, row: RowId) -> (u32, u64) {
        (tbl.0, row.0)
    }
}

/// One effect this runtime published to the shared tables.
#[derive(Debug, Clone)]
struct AppliedOp {
    seq: u64,
    txn: TxnId,
    table: String,
    tbl: TableId,
    row: RowId,
    col: Option<ColId>,
    kind: OpKind,
    /// Value before the op landed, for inversion by `REVERT`.
    before: Option<Value>,
    /// Whole-row image before the op landed, for inverting `RowCreate` / `RowDelete`.
    before_row: Option<Vec<Value>>,
}

/// What one `MERGE` published, so `REVERT` can find it again.
#[derive(Debug, Clone)]
struct MergeRecord {
    branch: BranchId,
    txns: Vec<TxnId>,
}

#[derive(Default)]
struct State {
    workspaces: BTreeMap<u64, Workspace>,
    names: BTreeMap<String, BranchId>,
    runs: BTreeMap<u32, RunEntity>,
    next_prov: u32,
    next_txn: u64,
    next_merge: u64,
    apply_seq: u64,
    applied: Vec<AppliedOp>,
    merges: BTreeMap<String, MergeRecord>,
    /// Which run last published each row, surviving the merge that published it.
    ///
    /// Without this, criterion 9 could only be answered for a row on a *live* branch: `run_of`
    /// reads the workspace, and `seal` drops the workspace the instant the merge succeeds — so
    /// the question "which agent wrote this row" became unanswerable at exactly the moment the
    /// row became visible to anyone else. The map is keyed by row, not by branch, because that
    /// is the question being asked.
    row_author: BTreeMap<(u32, u64), ProvId>,
    versions: BTreeMap<(u32, u64), VersionRef>,
    dep: DependencyGraphBuilder,
    policy: PolicyTable,
}

/// Resolves a branch name written in SQL (`b_3`) to a live `BranchId`.
pub trait BranchResolver {
    fn resolve_branch(&self, name: &str) -> Result<BranchId, FerroError>;
}

pub struct AgentRuntime {
    branches: Arc<dyn BranchCatalog>,
    /// Where captured frames go. One frame per agent task, re-appended as the task grows, so a
    /// merge engine on the other side of this trait sees exactly what the SQL layer captured.
    log: Arc<dyn EffectLog>,
    state: Mutex<State>,
}

impl Default for AgentRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentRuntime {
    /// Build over the real branch engine.
    ///
    /// `LogBranchCatalog` is the durable implementation: an append-only record log, generation
    /// counters so a reaped id can never be mistaken for a live one, and id release on reap.
    /// `MemBranchCatalog` remains for callers that explicitly want the simplified stand-in, but
    /// it is no longer what an agent session gets by default.
    ///
    /// The record log is memory-backed here because `Session::new` carries no path. A caller
    /// with somewhere to put it uses `LogBranchCatalog::open` and `with_catalog`.
    pub fn new() -> Self {
        AgentRuntime::with_catalog(Arc::new(LogBranchCatalog::in_memory(TRUNK_ROOT_PAGE)))
    }

    /// Build over any `BranchCatalog` — the durable branch engine drops in here.
    pub fn with_catalog(branches: Arc<dyn BranchCatalog>) -> Self {
        AgentRuntime::with_parts(branches, Arc::new(MemEffectLog::new()))
    }

    /// Build over any `BranchCatalog` and any `EffectLog`.
    pub fn with_parts(branches: Arc<dyn BranchCatalog>, log: Arc<dyn EffectLog>) -> Self {
        AgentRuntime { branches, log, state: Mutex::new(State::default()) }
    }

    pub fn branches(&self) -> &Arc<dyn BranchCatalog> {
        &self.branches
    }

    /// The captured Typed Effect Log. `MERGE` and `DIFF` both read from here through the shared
    /// traits, so the log is on the live path rather than a side record.
    pub fn log(&self) -> &Arc<dyn EffectLog> {
        &self.log
    }

    /// Declare a column's concurrent-write policy. Absent a declaration the policy is `REJECT`.
    pub fn set_policy(&self, table: &str, col: ColId, policy: MergePolicy) {
        self.state.lock().unwrap().policy.set(table_id(table), col, policy);
    }

    // ---- BEGIN AGENT SESSION ---------------------------------------------------------------

    /// Fork a branch for one agent task and intern its run.
    ///
    /// The fork is one metadata record plus one epoch appended to the parent. Provenance is
    /// interned once per run, not stamped per row (exit criterion 9).
    pub fn begin_session(
        &self,
        agent_id: &str,
        run_id: Option<&str>,
        parent: BranchId,
    ) -> Result<AgentSession, FerroError> {
        self.begin_session_with_model(agent_id, run_id, None, parent)
    }

    /// As [`AgentRuntime::begin_session`], but recording the model behind the run.
    ///
    /// Criterion 9 names the model explicitly, so it is carried rather than defaulted: a caller
    /// that declares none gets the literal string `unspecified`, which reads as "never declared"
    /// instead of attributing the write to a model nobody named.
    pub fn begin_session_with_model(
        &self,
        agent_id: &str,
        run_id: Option<&str>,
        model: Option<(&str, &str)>,
        parent: BranchId,
    ) -> Result<AgentSession, FerroError> {
        if agent_id.trim().is_empty() {
            return Err(FerroError::Bind("agent id must not be empty".into()));
        }
        let record = self
            .branches
            .fork(parent, LeaseDeadline::from_now(DEFAULT_LEASE_MILLIS))?;
        let branch = record.branch_id;
        let mut state = self.state.lock().unwrap();

        state.next_prov += 1;
        let prov = ProvId(state.next_prov);
        state.next_txn += 1;
        let txn = TxnId(state.next_txn);
        let run = run_id.unwrap_or("<unnamed>").to_string();
        let (model_name, model_version) = model.unwrap_or(("unspecified", "unspecified"));
        let entity = RunEntity::new(
            prov,
            agent_id,
            run.clone(),
            model_name,
            model_version,
            [0u8; 32],
            LeaseDeadline::now_millis(),
            parent,
        );
        state.runs.insert(prov.0, entity);

        let name = format!("b_{}", branch.id);
        state.names.insert(name.clone(), branch);
        let fork_seq = state.apply_seq;
        // Forking from a branch that is itself an open agent task: the child's visible state *is*
        // the parent's state at fork time, uncommitted rows included, exactly as the child's root
        // page is the parent's root page. Taking a snapshot rather than a link is what keeps the
        // parent's *later* writes invisible to the child, and keeps the read path from walking
        // the parent chain — the one pattern DESIGN.md rules out outright.
        let (rows, base_rows, tables) = match state.workspaces.get(&parent.id) {
            Some(p) => (p.rows.clone(), p.base_rows.clone(), p.tables.clone()),
            None => (BTreeMap::new(), BTreeMap::new(), BTreeMap::new()),
        };
        state.workspaces.insert(
            branch.id,
            Workspace {
                name: name.clone(),
                prov,
                txn,
                fork_seq,
                rows,
                base_rows,
                tables,
                frame: TxnFrame::new(txn, branch, CommitHash::ZERO, 0, 1),
                reads: Vec::new(),
            },
        );
        Ok(AgentSession {
            branch,
            branch_name: name,
            agent_id: agent_id.to_string(),
            run_id: run,
            prov,
            txn,
        })
    }

    /// The interned run behind a branch: which agent + run + model wrote here.
    ///
    /// Answers only for a *live* branch — the workspace is dropped when the branch merges or is
    /// abandoned. For a row that has already been published, ask [`AgentRuntime::who_wrote_row`].
    pub fn run_of(&self, branch: BranchId) -> Option<RunEntity> {
        let state = self.state.lock().unwrap();
        let prov = state.workspaces.get(&branch.id)?.prov;
        state.runs.get(&prov.0).cloned()
    }

    /// Exit criterion 9: which agent + run + model wrote a given row.
    ///
    /// Answers for a row in the shared tables — that is, one some merge published — and keeps
    /// answering after the writing branch is gone. A row nobody attributed (seeded before any
    /// agent ran) returns `None`, never a guess.
    pub fn who_wrote_row(&self, table: &str, row: RowId) -> Option<RunEntity> {
        let state = self.state.lock().unwrap();
        let prov = *state.row_author.get(&(table_id(table).0, row.0))?;
        state.runs.get(&prov.0).cloned()
    }

    /// Every attributed row of `table`, as `(row, run)`, ordered by row id.
    pub fn authors_of(&self, table: &str) -> Vec<(RowId, RunEntity)> {
        let state = self.state.lock().unwrap();
        let tbl = table_id(table).0;
        state
            .row_author
            .iter()
            .filter(|((t, _), _)| *t == tbl)
            .filter_map(|((_, r), p)| state.runs.get(&p.0).map(|e| (RowId(*r), e.clone())))
            .collect()
    }

    // ---- reads -----------------------------------------------------------------------------

    /// Every row of `table` as `branch` sees it: the shared table overlaid with that branch's
    /// uncommitted buffer.
    fn visible_rows(
        &self,
        ctx: &mut ExecCtx,
        branch: Option<BranchId>,
        table: &str,
    ) -> Result<Vec<(RowId, Vec<Value>)>, FerroError> {
        let base = scan_table(table, ctx)?;
        let tbl = table_id(table);
        let mut rows: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
        for r in base {
            rows.insert(row_id_of(&r).0, r);
        }
        if let Some(b) = branch {
            let state = self.state.lock().unwrap();
            if let Some(ws) = state.workspaces.get(&b.id) {
                for ((t, row), st) in &ws.rows {
                    if *t != tbl.0 {
                        continue;
                    }
                    match st {
                        RowState::Present(v) => {
                            rows.insert(*row, v.clone());
                        }
                        RowState::Deleted => {
                            rows.remove(row);
                        }
                    }
                }
            }
        }
        Ok(rows.into_iter().map(|(k, v)| (RowId(k), v)).collect())
    }

    /// Execute a single-table SELECT against a branch's visible state, recording the read-set.
    ///
    /// Read-set form is chosen by **access shape**, never by size: a point lookup on the primary
    /// key retains exact versions (which is what gives `REVERT ... CASCADE` exact causal edges),
    /// a scan retains a predicate summary (which is what gives phantom coverage).
    pub fn select(
        &self,
        ctx: &mut ExecCtx,
        branch: BranchId,
        stmt: &Stmt,
        reader: Option<BranchId>,
    ) -> Result<Vec<Vec<Value>>, FerroError> {
        let (from, columns, where_clause, joins) = match stmt {
            Stmt::Select { from, columns, where_clause, joins } => {
                (from, columns, where_clause, joins)
            }
            _ => return Err(FerroError::Bind("expected a SELECT".into())),
        };
        if !joins.is_empty() {
            return Err(FerroError::Bind(
                "AS OF BRANCH does not support joins yet".into(),
            ));
        }
        let entry = ctx
            .catalog
            .get_table(&from.name)
            .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", from.name)))?;
        let schema = entry.schema.clone();
        let qualifier = from.alias.clone().unwrap_or_else(|| from.name.clone());
        let scope = table_scope(&qualifier, &schema)?;
        let binder = Binder::new(ctx.catalog);
        let bound_where = match where_clause {
            Some(w) => Some(binder.bind_expr(w.clone(), &scope)?),
            None => None,
        };
        let (proj, _out) = binder.bind_projection(columns.clone(), &scope)?;

        let rows = self.visible_rows(ctx, Some(branch), &from.name)?;
        let mut matched: Vec<(RowId, Vec<Value>)> = Vec::new();
        for (rid, row) in rows {
            let keep = match &bound_where {
                Some(p) => matches!(evaluate(p, &row)?, Value::Boolean(true)),
                None => true,
            };
            if keep {
                matched.push((rid, row));
            }
        }

        // Record the read-set against the *reading* session, if there is one.
        if let Some(reader_branch) = reader {
            let shape = access_shape(where_clause.as_ref(), &schema);
            self.record_read(reader_branch, table_id(&from.name), shape, &matched, where_clause.as_ref());
        }

        let mut out = Vec::with_capacity(matched.len());
        for (_, row) in matched {
            let mut projected = Vec::with_capacity(proj.len());
            for e in &proj {
                projected.push(evaluate(e, &row)?);
            }
            out.push(projected);
        }
        Ok(out)
    }

    fn record_read(
        &self,
        reader: BranchId,
        tbl: TableId,
        shape: AccessShape,
        matched: &[(RowId, Vec<Value>)],
        where_clause: Option<&Expr>,
    ) {
        let mut state = self.state.lock().unwrap();
        let txn = match state.workspaces.get(&reader.id) {
            Some(ws) => ws.txn,
            None => return,
        };
        let versions: Vec<VersionRef> = matched
            .iter()
            .map(|(rid, _)| {
                state
                    .versions
                    .get(&(tbl.0, rid.0))
                    .copied()
                    .unwrap_or(VersionRef {
                        tbl,
                        row: *rid,
                        rid: RecordId { page_id: 0, slot_num: 0 },
                        begin_ts: 0,
                    })
            })
            .collect();
        let summary = PredicateSummary {
            tbl,
            col: None,
            lo: crate::provenance::readset::Bound::Unbounded,
            hi: crate::provenance::readset::Bound::Unbounded,
            residual: where_clause.map(|w| w.to_sql()),
            rows_observed: matched.len() as u64,
        };
        let mut builder = crate::provenance::readset::ReadSetBuilder::new();
        builder.observe(shape, versions.clone(), Some(summary));
        let sets = builder.finish();
        for v in &versions {
            if shape.form() == crate::provenance::readset::ReadSetForm::ExactVersions {
                state.dep.record_read(txn, *v);
            }
        }
        if let Some(ws) = state.workspaces.get_mut(&reader.id) {
            ws.reads.extend(sets);
        }
    }

    // ---- writes on a branch ----------------------------------------------------------------

    /// Route a DML statement into a branch's private buffer.
    ///
    /// Nothing here touches the shared tables: that is exit criterion 2. The typed effect and the
    /// guard that admitted it are captured at the same moment, because the guard is the one thing
    /// no log of values can reconstruct afterwards.
    pub fn write(
        &self,
        ctx: &mut ExecCtx,
        branch: BranchId,
        stmt: Stmt,
    ) -> Result<usize, FerroError> {
        match stmt {
            Stmt::Update { table, assignments, where_clause } => {
                self.branch_update(ctx, branch, &table, assignments, where_clause)
            }
            Stmt::Insert { table, values } => self.branch_insert(ctx, branch, &table, values),
            Stmt::Delete { table, where_clause } => {
                self.branch_delete(ctx, branch, &table, where_clause)
            }
            _ => Err(FerroError::Bind(
                "only INSERT / UPDATE / DELETE run inside an agent session".into(),
            )),
        }
    }

    fn branch_update(
        &self,
        ctx: &mut ExecCtx,
        branch: BranchId,
        table: &str,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    ) -> Result<usize, FerroError> {
        let entry = ctx
            .catalog
            .get_table(table)
            .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", table)))?;
        let schema = entry.schema.clone();
        let tbl = table_id(table);
        let scope = table_scope(table, &schema)?;
        // Bind everything before touching the tables: the binder borrows the catalog and the scan
        // needs it mutably.
        let (bound_where, resolved) = {
            let binder = Binder::new(ctx.catalog);
            let bw = match &where_clause {
                Some(w) => Some(binder.bind_expr(w.clone(), &scope)?),
                None => None,
            };
            let mut resolved: Vec<(usize, Expr, crate::binder::binder::BoundExpr)> = Vec::new();
            for (name, expr) in assignments {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == name)
                    .ok_or_else(|| FerroError::Bind(format!("unknown column: {}", name)))?;
                let bound = binder.bind_expr(expr.clone(), &scope)?;
                resolved.push((idx, expr, bound));
            }
            (bw, resolved)
        };

        let rows = self.visible_rows(ctx, Some(branch), table)?;
        let mut touched = 0usize;
        for (rid, row) in rows {
            if let Some(p) = &bound_where {
                if !matches!(evaluate(p, &row)?, Value::Boolean(true)) {
                    continue;
                }
            }
            let mut new_row = row.clone();
            let mut ops: Vec<Op> = Vec::new();
            for (idx, expr, bound) in &resolved {
                let new_value = evaluate(bound, &row)?;
                let kind = op_kind_for(*idx, expr, &schema, &row, &new_value)?;
                ops.push(
                    Op::new(tbl, rid, Some(ColId(*idx as u32)), kind)
                        .with_witness(row[*idx].clone()),
                );
                new_row[*idx] = new_value;
            }
            let guard = match &where_clause {
                Some(w) => Some(guard_from_expr(w, tbl, rid, &schema)?),
                None => None,
            };
            self.stage(branch, tbl, table, rid, Some(row), RowState::Present(new_row), ops, guard)?;
            touched += 1;
        }
        Ok(touched)
    }

    fn branch_insert(
        &self,
        ctx: &mut ExecCtx,
        branch: BranchId,
        table: &str,
        values: Vec<Expr>,
    ) -> Result<usize, FerroError> {
        let entry = ctx
            .catalog
            .get_table(table)
            .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", table)))?;
        let schema = entry.schema.clone();
        let tbl = table_id(table);
        let binder = Binder::new(ctx.catalog);
        let empty = Scope::new();
        let mut row = Vec::with_capacity(values.len());
        for v in values {
            row.push(evaluate(&binder.bind_expr(v, &empty)?, &[])?);
        }
        if row.len() != schema.columns.len() {
            return Err(FerroError::Bind(format!(
                "INSERT has {} values but {} has {} columns",
                row.len(),
                table,
                schema.columns.len()
            )));
        }
        let rid = row_id_of(&row);
        let existing = self
            .visible_rows(ctx, Some(branch), table)?
            .into_iter()
            .find(|(r, _)| *r == rid);
        if existing.is_some() {
            return Err(FerroError::Contraint(format!(
                "duplicate primary key in {}",
                table
            )));
        }
        let op = Op::new(tbl, rid, None, OpKind::RowCreate(row.clone()));
        self.stage(branch, tbl, table, rid, None, RowState::Present(row), vec![op], None)?;
        Ok(1)
    }

    fn branch_delete(
        &self,
        ctx: &mut ExecCtx,
        branch: BranchId,
        table: &str,
        where_clause: Option<Expr>,
    ) -> Result<usize, FerroError> {
        let entry = ctx
            .catalog
            .get_table(table)
            .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", table)))?;
        let schema = entry.schema.clone();
        let tbl = table_id(table);
        let scope = table_scope(table, &schema)?;
        let binder = Binder::new(ctx.catalog);
        let bound_where = match &where_clause {
            Some(w) => Some(binder.bind_expr(w.clone(), &scope)?),
            None => None,
        };
        let rows = self.visible_rows(ctx, Some(branch), table)?;
        let mut n = 0;
        for (rid, row) in rows {
            if let Some(p) = &bound_where {
                if !matches!(evaluate(p, &row)?, Value::Boolean(true)) {
                    continue;
                }
            }
            let guard = match &where_clause {
                Some(w) => Some(guard_from_expr(w, tbl, rid, &schema)?),
                None => None,
            };
            let op = Op::new(tbl, rid, None, OpKind::RowDelete);
            self.stage(branch, tbl, table, rid, Some(row), RowState::Deleted, vec![op], guard)?;
            n += 1;
        }
        Ok(n)
    }

    /// Put one row change into the branch's buffer and append its ops and guard to the frame.
    fn stage(
        &self,
        branch: BranchId,
        tbl: TableId,
        table: &str,
        row: RowId,
        before: Option<Vec<Value>>,
        after: RowState,
        ops: Vec<Op>,
        guard: Option<Guard>,
    ) -> Result<(), FerroError> {
        let frame = {
            let mut state = self.state.lock().unwrap();
            let ws = state.workspaces.get_mut(&branch.id).ok_or_else(|| {
                FerroError::Branch(format!("no agent session on branch {}", branch))
            })?;
            let key = Workspace::key(tbl, row);
            ws.base_rows.entry(key).or_insert(before);
            ws.tables.insert(tbl.0, table.to_string());
            ws.rows.insert(key, after);
            for op in ops {
                ws.frame.push_op(op);
            }
            if let Some(g) = guard {
                ws.frame.push_guard(g);
            }
            ws.frame.clone()
        };
        // Re-appending the task's frame replaces it rather than adding a second copy: `Add` is
        // not idempotent and two copies of one frame would double-count.
        self.log.append(&frame)?;
        Ok(())
    }

    // ---- DIFF ------------------------------------------------------------------------------

    /// The structured changeset a branch would merge. Exit criterion 4.
    pub fn diff(&self, ctx: &mut ExecCtx, branch: BranchId) -> Result<ChangeSet, FerroError> {
        let (target, rows_meta) = {
            let state = self.state.lock().unwrap();
            let ws = state.workspaces.get(&branch.id).ok_or_else(|| {
                FerroError::Branch(format!("no agent session on branch {}", branch))
            })?;
            let target = self.branches.get(branch)?.parent_id.unwrap_or(BranchId::TRUNK);
            let meta: Vec<(u32, u64, String, Option<Vec<Value>>, RowState, Vec<Op>, Vec<Guard>, bool)> =
                ws.rows
                    .iter()
                    .map(|((t, r), st)| {
                        let table = ws.tables.get(t).cloned().unwrap_or_default();
                        let before = ws.base_rows.get(&(*t, *r)).cloned().flatten();
                        let ops: Vec<Op> = ws
                            .frame
                            .ops
                            .iter()
                            .filter(|o| o.tbl.0 == *t && o.row.0 == *r)
                            .cloned()
                            .collect();
                        let guards: Vec<Guard> = ws
                            .frame
                            .guards
                            .iter()
                            .filter(|g| {
                                g.expr
                                    .referenced_cells()
                                    .iter()
                                    .any(|(gt, gr, _)| gt.0 == *t && gr.0 == *r)
                            })
                            .cloned()
                            .collect();
                        let concurrent = state
                            .applied
                            .iter()
                            .any(|a| a.seq > ws.fork_seq && a.tbl.0 == *t && a.row.0 == *r);
                        (*t, *r, table, before, st.clone(), ops, guards, concurrent)
                    })
                    .collect();
            (target, meta)
        };

        let mut rows = Vec::with_capacity(rows_meta.len());
        for (t, r, table, before, after, ops, guards, concurrent) in rows_meta {
            let (kind, after_img) = match (&before, &after) {
                (_, RowState::Deleted) => (RowChangeKind::Delete, None),
                (None, RowState::Present(v)) => (RowChangeKind::Insert, Some(v.clone())),
                (Some(_), RowState::Present(v)) => (RowChangeKind::Update, Some(v.clone())),
            };
            rows.push(RowChange {
                table,
                tbl: TableId(t),
                row: RowId(r),
                kind,
                ops,
                before,
                after: after_img,
                guards,
                outcome: if concurrent {
                    ChangeOutcome::PendingConcurrent
                } else {
                    ChangeOutcome::Pending
                },
            });
        }
        let _ = ctx;
        Ok(ChangeSet { from: target, to: branch, rows })
    }

    // ---- MERGE -----------------------------------------------------------------------------

    /// Three-way merge of a branch into its parent. Exit criteria 5, 6 and 7.
    ///
    /// Composition first, guards second, verdict third. A conflicting merge publishes **nothing**
    /// and leaves the branch alive so the agent can retry with the returned predicate.
    pub fn merge(&self, ctx: &mut ExecCtx, branch: BranchId) -> Result<MergeReport, FerroError> {
        let target = self.branches.get(branch)?.parent_id.unwrap_or(BranchId::TRUNK);
        let snapshot = {
            let state = self.state.lock().unwrap();
            let ws = state.workspaces.get(&branch.id).ok_or_else(|| {
                FerroError::Branch(format!("no agent session on branch {}", branch))
            })?;
            WorkspaceSnapshot {
                txn: ws.txn,
                prov: ws.prov,
                fork_seq: ws.fork_seq,
                rows: ws.rows.clone(),
                base_rows: ws.base_rows.clone(),
                tables: ws.tables.clone(),
                ops: ws.frame.ops.clone(),
                guards: ws.frame.guards.clone(),
            }
        };

        // Current shared state for every table this branch touched.
        let mut current: BTreeMap<(u32, u64), Vec<Value>> = BTreeMap::new();
        let mut schemas: BTreeMap<u32, Schema> = BTreeMap::new();
        for (t, name) in &snapshot.tables {
            let entry = ctx
                .catalog
                .get_table(name)
                .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", name)))?;
            schemas.insert(*t, entry.schema.clone());
            for row in scan_table(name, ctx)? {
                current.insert((*t, row_id_of(&row).0), row);
            }
        }

        let mut row_outcomes: Vec<RowMergeOutcome> = Vec::new();
        let mut pending_writes: Vec<PendingWrite> = Vec::new();
        // The state guards are re-checked against; see the comment where it is filled in.
        let mut admit_state = CellState::new();
        let policy_snapshot = { self.state.lock().unwrap().policy.clone() };

        for ((t, r), after) in &snapshot.rows {
            let table = snapshot.tables.get(t).cloned().unwrap_or_default();
            let tbl = TableId(*t);
            let row = RowId(*r);
            let schema = schemas.get(t).cloned().unwrap_or_else(|| Schema::new(Vec::new()));
            let before = snapshot.base_rows.get(&(*t, *r)).cloned().flatten();
            let now = current.get(&(*t, *r)).cloned();
            let mut applied_ops: Vec<Op> = Vec::new();
            let mut discarded = Vec::new();
            let mut conflicts: Vec<ConflictReport> = Vec::new();
            let mut composed: Vec<Op> = Vec::new();
            // **The state a guard is re-evaluated against**: the target as it stands at merge
            // time, which already carries every concurrent branch's composed effect, and which is
            // exactly what this branch's ops are about to be applied to.
            //
            // This is the reading that makes the bounded counter work as DESIGN.md describes it.
            // `UPDATE qty = qty - 12 WHERE qty >= 12` on a base of 20: merged solo the guard sees
            // 20 and holds; merged after a concurrent -12 it sees 8 and fails, returning
            // `qty >= 12` to the agent. Checking it against the *post*-op image instead would
            // reject the solo merge too, because a precondition is not a postcondition.
            let admit_image = match (before.as_ref(), after) {
                // An insert has no prior image, so its own new row is what a guard can refer to.
                (None, RowState::Present(v)) => Some(v.clone()),
                _ => now.clone().or_else(|| before.clone()),
            };

            match (before.as_ref(), after) {
                // insert
                (None, RowState::Present(v)) => {
                    if now.is_some() {
                        conflicts.push(ConflictReport {
                            kind: ConflictKind::ContradictoryAssign,
                            tbl,
                            row,
                            col: None,
                            violated_guard: None,
                            ours: Some(Op::new(tbl, row, None, OpKind::RowCreate(v.clone()))),
                            theirs: None,
                            detail: "the target already has a row with this key".into(),
                        });
                    } else {
                        applied_ops.push(Op::new(tbl, row, None, OpKind::RowCreate(v.clone())));
                        pending_writes.push(PendingWrite::Insert {
                            table: table.clone(),
                            row: v.clone(),
                        });
                    }
                }
                // delete
                (Some(b), RowState::Deleted) => match &now {
                    Some(n) if n != b => conflicts.push(ConflictReport {
                        kind: ConflictKind::DeleteVsWrite,
                        tbl,
                        row,
                        col: None,
                        violated_guard: None,
                        ours: Some(Op::new(tbl, row, None, OpKind::RowDelete)),
                        theirs: None,
                        detail: "the row was written on the target after this branch forked".into(),
                    }),
                    Some(_) => {
                        applied_ops.push(Op::new(tbl, row, None, OpKind::RowDelete));
                        pending_writes.push(PendingWrite::Delete {
                            table: table.clone(),
                            key: b[0].clone(),
                        });
                    }
                    None => {
                        // already gone on the target: nothing to publish
                    }
                },
                // update
                (Some(b), RowState::Present(v)) => {
                    let mut new_row = now.clone().unwrap_or_else(|| b.clone());
                    for idx in 0..v.len().min(b.len()) {
                        if v[idx] == b[idx] {
                            continue;
                        }
                        let col = ColId(idx as u32);
                        let ours = compose_ops(
                            &snapshot
                                .ops
                                .iter()
                                .filter(|o| o.tbl == tbl && o.row == row && o.col == Some(col))
                                .map(|o| o.kind.clone())
                                .collect::<Vec<_>>(),
                        )
                        .unwrap_or(OpKind::Assign(v[idx].clone()));
                        let theirs = self.concurrent_op(tbl, row, col, snapshot.fork_seq, &now, b, idx);
                        let cell = CellMerge {
                            tbl,
                            row,
                            col,
                            base: Some(b[idx].clone()),
                            target: now.as_ref().map(|n| n[idx].clone()).or(Some(b[idx].clone())),
                            ours,
                            theirs,
                        };
                        match resolve_cell(&cell, branch, &policy_snapshot)? {
                            CellResolution::Clean { value, op } => {
                                new_row[idx] = value;
                                applied_ops.push(op);
                            }
                            CellResolution::Commuting { value, op } => {
                                new_row[idx] = value;
                                applied_ops.push(op.clone());
                                composed.push(op);
                            }
                            CellResolution::Lossy { value, op, discarded: d } => {
                                new_row[idx] = value;
                                applied_ops.push(op);
                                discarded.push(d);
                            }
                            CellResolution::Conflict(c) => conflicts.push(c),
                        }
                    }
                    if conflicts.is_empty() {
                        pending_writes.push(PendingWrite::Update {
                            table: table.clone(),
                            schema: schema.clone(),
                            key: new_row[0].clone(),
                            row: new_row.clone(),
                            before: now.clone().unwrap_or_else(|| b.clone()),
                        });
                    }
                }
                (None, RowState::Deleted) => {}
            }

            if let Some(img) = &admit_image {
                for (idx, val) in img.iter().enumerate() {
                    admit_state.set(tbl, row, ColId(idx as u32), val.clone());
                }
            }

            let outcome = if !conflicts.is_empty() {
                MergeOutcome::Conflict(conflicts.clone())
            } else if !discarded.is_empty() {
                MergeOutcome::ResolvedWithLoss {
                    applied: applied_ops.clone(),
                    discarded: discarded.clone(),
                }
            } else if !composed.is_empty() {
                MergeOutcome::Commuting { composed }
            } else {
                MergeOutcome::Clean
            };
            row_outcomes.push(RowMergeOutcome {
                table,
                tbl,
                row,
                outcome,
                applied: applied_ops,
                discarded,
                conflicts,
            });
        }

        // Guards are re-checked **after** composition, against the state the merge would produce.
        let guard_conflicts = check_guards(&snapshot.guards, &admit_state);
        for c in guard_conflicts {
            match row_outcomes.iter_mut().find(|r| r.tbl == c.tbl && r.row == c.row) {
                Some(r) => {
                    r.conflicts.push(c);
                    r.outcome = MergeOutcome::Conflict(r.conflicts.clone());
                    r.applied.clear();
                }
                None => row_outcomes.push(RowMergeOutcome {
                    table: snapshot.tables.get(&c.tbl.0).cloned().unwrap_or_default(),
                    tbl: c.tbl,
                    row: c.row,
                    outcome: MergeOutcome::Conflict(vec![c.clone()]),
                    applied: Vec::new(),
                    discarded: Vec::new(),
                    conflicts: vec![c],
                }),
            }
        }

        let outcome = MergeReport::aggregate(&row_outcomes);
        let merge_id = {
            let mut state = self.state.lock().unwrap();
            state.next_merge += 1;
            format!("m_{}", state.next_merge)
        };

        if outcome.is_conflict() {
            // Nothing is published and the branch stays alive: the agent has the violated
            // predicate and can retry.
            return Ok(MergeReport {
                merge_id,
                from: branch,
                into: target,
                outcome,
                rows: row_outcomes,
                applied_to_target: false,
            });
        }

        // Publish every row in ONE transaction. Row-at-a-time commits would leave a merge that
        // failed halfway visible on the target, which is exactly the state a merge exists to
        // avoid: the report says the merge landed or it says it did not.
        let publish_txn = ctx.txn.begin()?;
        for w in pending_writes {
            if let Err(e) = w.apply_in(ctx, publish_txn) {
                ctx.txn.abort(publish_txn)?;
                return Err(e);
            }
        }
        ctx.txn.commit(publish_txn)?;
        self.record_applied(branch, snapshot.txn, &row_outcomes, &snapshot, &merge_id);
        self.seal(branch)?;

        Ok(MergeReport {
            merge_id,
            from: branch,
            into: target,
            outcome,
            rows: row_outcomes,
            applied_to_target: true,
        })
    }

    /// The composed effect the target absorbed on this cell since we forked, if any.
    ///
    /// Prefers the recorded ops (which name the algebra element), and falls back to comparing the
    /// image: a value that moved with no recorded op is treated as an opaque `Assign`, which is
    /// the conservative reading.
    fn concurrent_op(
        &self,
        tbl: TableId,
        row: RowId,
        col: ColId,
        fork_seq: u64,
        now: &Option<Vec<Value>>,
        base: &[Value],
        idx: usize,
    ) -> Option<OpKind> {
        let state = self.state.lock().unwrap();
        let kinds: Vec<OpKind> = state
            .applied
            .iter()
            .filter(|a| a.seq > fork_seq && a.tbl == tbl && a.row == row && a.col == Some(col))
            .map(|a| a.kind.clone())
            .collect();
        if !kinds.is_empty() {
            return compose_ops(&kinds).ok();
        }
        match now {
            Some(n) if n.get(idx) != base.get(idx) => {
                n.get(idx).cloned().map(OpKind::Assign)
            }
            _ => None,
        }
    }

    fn record_applied(
        &self,
        branch: BranchId,
        txn: TxnId,
        rows: &[RowMergeOutcome],
        snapshot: &WorkspaceSnapshot,
        merge_id: &str,
    ) {
        let mut state = self.state.lock().unwrap();
        for r in rows {
            for op in &r.applied {
                state.apply_seq += 1;
                let seq = state.apply_seq;
                let before = snapshot
                    .ops
                    .iter()
                    .find(|o| o.tbl == op.tbl && o.row == op.row && o.col == op.col)
                    .and_then(|o| o.witness.clone());
                let before_row = snapshot
                    .base_rows
                    .get(&(op.tbl.0, op.row.0))
                    .cloned()
                    .flatten();
                state.applied.push(AppliedOp {
                    seq,
                    txn,
                    table: r.table.clone(),
                    tbl: op.tbl,
                    row: op.row,
                    col: op.col,
                    kind: op.kind.clone(),
                    before,
                    before_row,
                });
                // The version this merge produced, so a later reader's read-set names it exactly.
                let v = VersionRef {
                    tbl: op.tbl,
                    row: op.row,
                    rid: RecordId { page_id: 0, slot_num: 0 },
                    begin_ts: seq,
                };
                state.versions.insert((op.tbl.0, op.row.0), v);
                state.dep.record_write(txn, v);
                // Authorship of the published row, kept past `seal` (exit criterion 9).
                state.row_author.insert((op.tbl.0, op.row.0), snapshot.prov);
            }
        }
        state.merges.insert(
            merge_id.to_string(),
            MergeRecord { branch, txns: vec![txn] },
        );
    }

    // ---- ABANDON ---------------------------------------------------------------------------

    /// Drop a branch and everything buffered on it.
    ///
    /// The buffered writes were never in the shared tables, so an abandoned agent task costs
    /// exactly one metadata record. This is the cooperative form of what the lease reaper does
    /// with no client cooperation at all.
    pub fn abandon(&self, branch: BranchId) -> Result<(), FerroError> {
        self.seal(branch)
    }

    fn seal(&self, branch: BranchId) -> Result<(), FerroError> {
        {
            let mut state = self.state.lock().unwrap();
            if let Some(ws) = state.workspaces.remove(&branch.id) {
                state.names.remove(&ws.name);
            }
        }
        // Reap through the `BranchCatalog` trait only, so this works against the durable engine
        // as written: mark the record reaped (which bumps the generation, making the old id a
        // hard error) and drop our fork epoch from the parent's live-children array so the
        // parent's pages stop being pinned on our behalf.
        let mut record = self.branches.get(branch)?;
        if let Some(parent) = record.parent_id {
            if let Ok(mut p) = self.branches.get(parent) {
                if p.remove_live_child(record.fork_epoch) {
                    self.branches.put(&p)?;
                }
            }
        }
        record.mark_reaped();
        self.branches.put(&record)?;
        Ok(())
    }

    // ---- REVERT ----------------------------------------------------------------------------

    /// Plan (and under `Cascade`, perform) a causal revert of a merge.
    ///
    /// Halt is the default and reverts nothing: the caller is shown the dependency tree first.
    /// Under `Cascade` the downstream transactions are undone before the target, which is the only
    /// order that leaves a consistent state.
    pub fn revert_merge(
        &self,
        ctx: &mut ExecCtx,
        merge_id: &str,
        mode: RevertMode,
    ) -> Result<RevertPlan, FerroError> {
        let (targets, rec_branch, graph) = {
            let state = self.state.lock().unwrap();
            let rec = state
                .merges
                .get(merge_id)
                .ok_or_else(|| FerroError::Merge(format!("unknown merge {}", merge_id)))?;
            (rec.txns.clone(), rec.branch, state.dep.build())
        };
        let target = *targets
            .first()
            .ok_or_else(|| {
                FerroError::Merge(format!(
                    "merge {} of branch {} recorded no transaction",
                    merge_id, rec_branch
                ))
            })?;
        let plan = graph.plan_revert(target, mode);
        if plan.is_blocked() {
            return Ok(plan);
        }
        let mut order: Vec<TxnId> = plan.cascade.clone();
        order.push(target);
        for txn in order {
            self.undo_txn(ctx, txn)?;
        }
        Ok(plan)
    }

    fn undo_txn(&self, ctx: &mut ExecCtx, txn: TxnId) -> Result<(), FerroError> {
        let ops: Vec<AppliedOp> = {
            let state = self.state.lock().unwrap();
            let mut v: Vec<AppliedOp> =
                state.applied.iter().filter(|a| a.txn == txn).cloned().collect();
            v.sort_by(|a, b| b.seq.cmp(&a.seq));
            v
        };
        for a in ops {
            let entry = ctx
                .catalog
                .get_table(&a.table)
                .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", a.table)))?;
            let schema = entry.schema.clone();
            match (&a.kind, a.col) {
                (OpKind::RowCreate(row), _) => {
                    PendingWrite::Delete { table: a.table.clone(), key: row[0].clone() }
                        .apply(ctx)?;
                }
                (OpKind::RowDelete, _) => {
                    let row = a.before_row.clone().ok_or_else(|| {
                        FerroError::Merge("cannot revert a delete with no before-image".into())
                    })?;
                    PendingWrite::Insert { table: a.table.clone(), row }.apply(ctx)?;
                }
                (kind, Some(col)) => {
                    let inverse = invert(kind, a.before.as_ref())?;
                    let rows = scan_table(&a.table, ctx)?;
                    let cur = rows
                        .into_iter()
                        .find(|r| row_id_of(r) == a.row)
                        .ok_or_else(|| {
                            FerroError::Merge(format!(
                                "row {} is gone; cannot revert {}",
                                a.row,
                                kind.name()
                            ))
                        })?;
                    let idx = col.0 as usize;
                    let mut new_row = cur.clone();
                    new_row[idx] = apply_op(cur.get(idx), &inverse)?;
                    PendingWrite::Update {
                        table: a.table.clone(),
                        schema: schema.clone(),
                        key: new_row[0].clone(),
                        row: new_row,
                        before: cur,
                    }
                    .apply(ctx)?;
                }
                (kind, None) => {
                    return Err(FerroError::Merge(format!(
                        "cannot revert whole-row op {}",
                        kind.name()
                    )))
                }
            }
        }
        Ok(())
    }
}

impl BranchResolver for AgentRuntime {
    fn resolve_branch(&self, name: &str) -> Result<BranchId, FerroError> {
        let state = self.state.lock().unwrap();
        state
            .names
            .get(name)
            .copied()
            .ok_or_else(|| FerroError::Branch(format!("unknown branch: {}", name)))
    }
}

struct WorkspaceSnapshot {
    txn: TxnId,
    /// The run behind this task, carried into the merge so authorship outlives the workspace.
    prov: ProvId,
    fork_seq: u64,
    rows: BTreeMap<(u32, u64), RowState>,
    base_rows: BTreeMap<(u32, u64), Option<Vec<Value>>>,
    tables: BTreeMap<u32, String>,
    ops: Vec<Op>,
    guards: Vec<Guard>,
}

/// A write the merge will publish to the shared tables, expressed as ordinary SQL so it goes
/// through the same executor, WAL and indexes as any other write.
enum PendingWrite {
    Insert { table: String, row: Vec<Value> },
    Update { table: String, schema: Schema, key: Value, row: Vec<Value>, before: Vec<Value> },
    Delete { table: String, key: Value },
}

impl PendingWrite {
    /// Publish inside an already-open transaction.
    fn apply_in(self, ctx: &mut ExecCtx, txn_id: u64) -> Result<usize, FerroError> {
        let stmt = self.into_stmt(ctx)?;
        let stmt = match stmt {
            Some(s) => s,
            None => return Ok(0),
        };
        apply_dml_in(stmt, ctx, txn_id)
    }

    /// Publish in a transaction of its own.
    fn apply(self, ctx: &mut ExecCtx) -> Result<usize, FerroError> {
        let stmt = match self.into_stmt(ctx)? {
            Some(s) => s,
            None => return Ok(0),
        };
        apply_dml(stmt, ctx)
    }

    /// `None` when the write turned out to be a no-op.
    fn into_stmt(self, ctx: &mut ExecCtx) -> Result<Option<Stmt>, FerroError> {
        let stmt = match self {
            PendingWrite::Insert { table, row } => Stmt::Insert {
                table,
                values: row.iter().map(value_expr).collect(),
            },
            PendingWrite::Update { table, schema, key, row, before } => {
                let mut assignments = Vec::new();
                for (i, c) in schema.columns.iter().enumerate() {
                    if row.get(i) != before.get(i) {
                        assignments.push((c.name.clone(), value_expr(&row[i])));
                    }
                }
                if assignments.is_empty() {
                    return Ok(None);
                }
                let pk = schema.columns[0].name.clone();
                Stmt::Update {
                    table,
                    assignments,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::ColumnRef { table: None, column: pk }),
                        operator: TokenType::Equal,
                        right: Box::new(value_expr(&key)),
                    }),
                }
            }
            PendingWrite::Delete { table, key } => {
                let pk = ctx
                    .catalog
                    .get_table(&table)
                    .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", table)))?
                    .schema
                    .columns[0]
                    .name
                    .clone();
                Stmt::Delete {
                    table,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::ColumnRef { table: None, column: pk }),
                        operator: TokenType::Equal,
                        right: Box::new(value_expr(&key)),
                    }),
                }
            }
        };
        Ok(Some(stmt))
    }
}

/// Run a DML statement against the shared tables in its own transaction.
fn apply_dml(stmt: Stmt, ctx: &mut ExecCtx) -> Result<usize, FerroError> {
    let txn_id = ctx.txn.begin()?;
    match apply_dml_in(stmt, ctx, txn_id) {
        Ok(n) => {
            ctx.txn.commit(txn_id)?;
            Ok(n)
        }
        Err(e) => {
            ctx.txn.abort(txn_id)?;
            Err(e)
        }
    }
}

/// Run a DML statement inside an already-open transaction. The caller owns commit and abort.
fn apply_dml_in(stmt: Stmt, ctx: &mut ExecCtx, txn_id: u64) -> Result<usize, FerroError> {
    let snapshot = ctx.txn.snapshot_of(txn_id)?;
    let view = Arc::new(ReadView { snapshot, txn_id });
    match plan(stmt, ctx.catalog, ctx.bp.clone(), Some((ctx.txn.clone(), txn_id)), view)? {
        Plan::Write(mut op) => op.execute(ctx.catalog),
        Plan::Read(_) => Err(FerroError::Bind("expected a write plan".into())),
    }
}

/// Every row of a table as the shared (merged) state has it.
pub fn scan_table(table: &str, ctx: &mut ExecCtx) -> Result<Vec<Vec<Value>>, FerroError> {
    let view = Arc::new(ReadView { snapshot: ctx.txn.read_snapshot(), txn_id: 0 });
    let stmt = Stmt::Select {
        from: TableRef::plain(table.to_string(), None),
        columns: vec![Expr::ColumnRef { table: None, column: "*".into() }],
        where_clause: None,
        joins: Vec::new(),
    };
    match plan(stmt, ctx.catalog, ctx.bp.clone(), None, view)? {
        Plan::Read(mut root) => {
            let mut out = Vec::new();
            while let Some(next) = root.next() {
                out.push(next?.1);
            }
            Ok(out)
        }
        Plan::Write(_) => Err(FerroError::Bind("expected a read plan".into())),
    }
}

fn table_scope(qualifier: &str, schema: &Schema) -> Result<Scope, FerroError> {
    let mut scope = Scope::new();
    scope.add_table(qualifier, schema)?;
    Ok(scope)
}

/// A literal expression for a value, so merged state can be republished as ordinary SQL.
fn value_expr(v: &Value) -> Expr {
    let neg = |lex: String| Expr::UnaryOp {
        operator: TokenType::Minus,
        right: Box::new(Expr::Literal { value_type: TokenType::Number, value: lex }),
    };
    match v {
        Value::Integer(i) if *i < 0 => neg(i.unsigned_abs().to_string()),
        Value::Integer(i) => Expr::Literal { value_type: TokenType::Number, value: i.to_string() },
        Value::Float(f) if *f < 0.0 => neg(format!("{:?}", -f)),
        Value::Float(f) => Expr::Literal { value_type: TokenType::Number, value: format!("{:?}", f) },
        Value::Varchar(s) => Expr::Literal { value_type: TokenType::String, value: s.clone() },
        Value::Boolean(true) => Expr::Literal { value_type: TokenType::True, value: "true".into() },
        Value::Boolean(false) => Expr::Literal { value_type: TokenType::False, value: "false".into() },
        Value::Null => Expr::Literal { value_type: TokenType::Null, value: "null".into() },
    }
}

/// Which algebra element an assignment meant.
///
/// `qty = qty - 5` is an `Add(-5)`, and that is exactly the distinction a log of before/after
/// images cannot make: the same images are produced by `qty = 15`, which does **not** compose
/// with a concurrent decrement.
fn op_kind_for(
    col: usize,
    expr: &Expr,
    schema: &Schema,
    row: &[Value],
    new_value: &Value,
) -> Result<OpKind, FerroError> {
    if let Expr::BinaryOp { left, operator, right } = expr {
        let same_col = matches!(
            &**left,
            Expr::ColumnRef { column, .. } if schema.columns.get(col).map(|c| &c.name) == Some(column)
        );
        if same_col {
            if let Expr::Literal { value_type: TokenType::Number, value } = &**right {
                let delta = if value.contains('.') {
                    let f: f64 = value
                        .parse()
                        .map_err(|_| FerroError::Bind(format!("invalid float: {}", value)))?;
                    Delta::Float(f)
                } else {
                    let i: i64 = value
                        .parse()
                        .map_err(|_| FerroError::Bind(format!("invalid integer: {}", value)))?;
                    Delta::Int(i)
                };
                match operator {
                    TokenType::Plus => return Ok(OpKind::Add(delta)),
                    TokenType::Minus => return Ok(OpKind::Add(delta.negate())),
                    _ => {}
                }
            }
        }
    }
    let _ = row;
    Ok(OpKind::Assign(new_value.clone()))
}

/// Turn a WHERE clause into a re-evaluable guard bound to one row.
///
/// Guards are the one thing no log of values can reconstruct, so they are captured here at the
/// moment the write is admitted, with the SQL text kept verbatim for handing back on violation.
pub fn guard_from_expr(
    expr: &Expr,
    tbl: TableId,
    row: RowId,
    schema: &Schema,
) -> Result<Guard, FerroError> {
    let g = guard_expr(expr, tbl, row, schema)?;
    Ok(Guard::holds(g).with_source(expr.to_sql()))
}

fn guard_expr(
    expr: &Expr,
    tbl: TableId,
    row: RowId,
    schema: &Schema,
) -> Result<GuardExpr, FerroError> {
    Ok(match expr {
        Expr::Grouping(inner) => guard_expr(inner, tbl, row, schema)?,
        Expr::Literal { value_type, value } => {
            GuardExpr::Literal(Binder::literal_value(*value_type, value.clone())?)
        }
        Expr::ColumnRef { column, .. } => {
            let idx = schema
                .columns
                .iter()
                .position(|c| &c.name == column)
                .ok_or_else(|| FerroError::Bind(format!("unknown column in guard: {}", column)))?;
            GuardExpr::col(tbl, row, ColId(idx as u32))
        }
        Expr::UnaryOp { operator, right } => {
            let r = guard_expr(right, tbl, row, schema)?;
            match operator {
                TokenType::Not | TokenType::Bang => GuardExpr::Not(Box::new(r)),
                TokenType::Minus => GuardExpr::arith(
                    GuardExpr::Literal(Value::Integer(0)),
                    ArithOp::Sub,
                    r,
                ),
                other => {
                    return Err(FerroError::Bind(format!(
                        "unsupported unary operator in guard: {:?}",
                        other
                    )))
                }
            }
        }
        Expr::BinaryOp { left, operator, right } => {
            let l = guard_expr(left, tbl, row, schema)?;
            let r = guard_expr(right, tbl, row, schema)?;
            match operator {
                TokenType::Equal => GuardExpr::cmp(l, CmpOp::Eq, r),
                TokenType::BangEqual => GuardExpr::cmp(l, CmpOp::Ne, r),
                TokenType::Less => GuardExpr::cmp(l, CmpOp::Lt, r),
                TokenType::LessEqual => GuardExpr::cmp(l, CmpOp::Le, r),
                TokenType::Greater => GuardExpr::cmp(l, CmpOp::Gt, r),
                TokenType::GreaterEqual => GuardExpr::cmp(l, CmpOp::Ge, r),
                TokenType::And => GuardExpr::And(vec![l, r]),
                TokenType::Or => GuardExpr::Or(vec![l, r]),
                TokenType::Plus => GuardExpr::arith(l, ArithOp::Add, r),
                TokenType::Minus => GuardExpr::arith(l, ArithOp::Sub, r),
                TokenType::Star => GuardExpr::arith(l, ArithOp::Mul, r),
                TokenType::Slash => GuardExpr::arith(l, ArithOp::Div, r),
                other => {
                    return Err(FerroError::Bind(format!(
                        "unsupported operator in guard: {:?}",
                        other
                    )))
                }
            }
        }
    })
}

/// Classify a read by its **shape**, which is the only admissible input to the read-set form.
/// Size is deliberately not consulted: coarsening scattered point reads into one interval covers
/// most of the table by `k = 3`.
fn access_shape(where_clause: Option<&Expr>, schema: &Schema) -> AccessShape {
    let pk = match schema.columns.first() {
        Some(c) => c.name.clone(),
        None => return AccessShape::FullScan,
    };
    match where_clause {
        Some(Expr::BinaryOp { left, operator: TokenType::Equal, right }) => {
            let points_at_pk = matches!(&**left, Expr::ColumnRef { column, .. } if *column == pk)
                && matches!(&**right, Expr::Literal { .. });
            if points_at_pk {
                AccessShape::IndexLookup
            } else {
                AccessShape::FullScan
            }
        }
        _ => AccessShape::FullScan,
    }
}
