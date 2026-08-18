//! B9 — read-only system views over the agent-isolation layer.
//!
//! Design authority: DESIGN.md sections 1, 2 and 4, and exit criteria 8 and 9.
//!
//! # What these are, and what they are not
//!
//! Every row every view here produces is materialised from an API that was **already public and
//! already enumerable** before this module existed: `Catalog::tables`,
//! [`BranchCatalog::all_branches`](crate::branch::BranchCatalog::all_branches),
//! [`AgentRuntime::run_of`], [`AgentRuntime::authors_of`],
//! [`AgentRuntime::quarantined_branches`], [`AgentRuntime::quarantine_reason`] and
//! [`AgentRuntime::run_activity`]. Nothing here keeps a ledger, stamps a row, or records an event.
//! It is presentation, and it is deliberately the *only* thing it is: an observability surface that
//! kept its own bookkeeping would be a second source of truth about what the agent layer did, and
//! the first thing to go wrong with a second source of truth is that it disagrees with the first.
//!
//! The corollary is that a view is exactly as durable as the thing behind it, which is not the same
//! for all five and is stated per view below rather than left to be discovered. `ferro_branches`
//! and `ferro_quarantine`'s membership come off the durable branch record log; a quarantine
//! *reason* and everything in `ferro_runs` / `ferro_run_activity` live in the runtime's in-memory
//! state and are gone after a restart. A view that quietly rounded "I do not know" up to "there is
//! nothing" would be worse than no view.
//!
//! # Why these are not tables
//!
//! There is no `TableEntry` for a system view and no heap behind it. `TableEntry`
//! (`src/catalog/catalog_page.rs`) is `{name, first_directory_page_id, primary_index_root,
//! time_travel_root, schema, indexes}` — every field a real page id — so a view cannot be
//! represented as one without inventing pages for it. Instead the executor recognises the name
//! before any of the ordinary routes are tried, materialises the rows here, and then runs the
//! query's own `WHERE` and projection over them through the **existing** [`Filter`] and
//! [`Projection`] operators. That reuse is the point: `SELECT reason FROM ferro_quarantine WHERE
//! branch_id = 3` gets the engine's comparison and evaluation semantics, not a second
//! implementation of them that can drift.
//!
//! # Real columns on the wire
//!
//! The output columns come from [`LogicalPlan::output_schema`], which is why this module builds a
//! logical plan it never lowers. Before B9 the column *names and types* in `BoundColumn` were dead
//! weight — every call site used `output_schema()` only for `.len()`, so pgwire had nothing to
//! advertise and named columns `column1..N`. A view whose columns arrive as `column1..column10` is
//! not a typed result a client can consume, so the schema is threaded from the plan to the wire
//! and [`Outcome::Table`](crate::execution::executor::Outcome::Table) is the shape that carries it.

use crate::agent_sql::runtime::AgentRuntime;
use crate::binder::binder::{Binder, BoundColumn, Scope};
use crate::branch::types::BranchState;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::{Column, DataType, Value};
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::execution::executor::Executor;
use crate::execution::filter::Filter;
use crate::execution::projection::Projection;
use crate::parser::parser::Stmt;
use crate::planner::logical_plan::LogicalPlan;
use crate::storage::heap_file_manager::RecordId;

/// Rows that know their own columns.
///
/// The shape a result needs to reach a client as typed rows: names and declared types in emission
/// order, alongside the values. `Vec<Vec<Value>>` on its own cannot answer "what is column 3
/// called", which is why pgwire had to invent `column3` for every result until this existed.
///
/// **The columns are held independently of the rows, and that is load-bearing rather than
/// incidental.** A result with columns and no rows is not the same thing as a broken one: an empty
/// `ferro_quarantine` still announces `branch_id, generation, branch, reason` with `SELECT 0`, so a
/// client can tell "nothing is held" from "this view does not work". A shape that derived its field
/// list from the first row — which is what the pre-existing `Outcome::Rows` path in pgwire does —
/// sends zero fields and zero rows for both.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedRows {
    pub columns: Vec<BoundColumn>,
    pub rows: Vec<Vec<Value>>,
}

impl NamedRows {
    pub fn new(columns: Vec<BoundColumn>, rows: Vec<Vec<Value>>) -> Self {
        NamedRows { columns, rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Position of a column by name, for a caller reading a specific field out of a view.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Every row's value in column `name`, in row order.
    pub fn column(&self, name: &str) -> Option<Vec<&Value>> {
        let at = self.column_index(name)?;
        Some(self.rows.iter().filter_map(|r| r.get(at)).collect())
    }

    /// Assert-friendly rendering used by tests and by the CLI: the declared column names, then one
    /// line per row.
    pub fn header(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

/// The read-only views. An **allowlist**, matched by exact name: a denylist over table names would
/// only catch the ones somebody already thought of, and there is no default that can be wrong here
/// because a name that is not one of these five is simply not a view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemView {
    /// Every branch the durable catalog knows, live or not.
    Branches,
    /// The interned run behind each live agent branch: which agent + run + model (criterion 9).
    Runs,
    /// Which run wrote each published row, per table.
    RowAuthors,
    /// Branches held by the verification gate, **with the reason they are held**.
    Quarantine,
    /// Per-run write and read counters for work in flight.
    RunActivity,
}

/// Every view name, in the order [`SystemView::ALL`] lists them. Exported so a test cannot iterate
/// a stale copy of the list and silently skip a view.
pub const VIEW_NAMES: [&str; 5] = [
    "ferro_branches",
    "ferro_runs",
    "ferro_row_authors",
    "ferro_quarantine",
    "ferro_run_activity",
];

impl SystemView {
    pub const ALL: [SystemView; 5] = [
        SystemView::Branches,
        SystemView::Runs,
        SystemView::RowAuthors,
        SystemView::Quarantine,
        SystemView::RunActivity,
    ];

    /// The view with this name, or `None`.
    ///
    /// **Case-sensitive and exact**, matching how every other relation name in this engine is
    /// resolved (`Catalog::tables` is a `HashMap<String, _>` keyed by the name as written, and the
    /// scanner does not fold case). Matching loosely here would make `FERRO_RUNS` a view while
    /// `SELECT * FROM T` still fails for a table created as `t`, which is a worse surface than
    /// either rule applied consistently.
    pub fn by_name(name: &str) -> Option<SystemView> {
        SystemView::ALL.into_iter().find(|v| v.name() == name)
    }

    pub fn name(&self) -> &'static str {
        match self {
            SystemView::Branches => VIEW_NAMES[0],
            SystemView::Runs => VIEW_NAMES[1],
            SystemView::RowAuthors => VIEW_NAMES[2],
            SystemView::Quarantine => VIEW_NAMES[3],
            SystemView::RunActivity => VIEW_NAMES[4],
        }
    }

    /// This view's declared columns.
    ///
    /// # Two width decisions that are not stylistic
    ///
    /// `lease_deadline` and `row_id` are `DECIMAL`, not `BIGINT`, because both are `u64` values that
    /// genuinely exceed `i64::MAX` in this codebase and `BIGINT` is `i64`. The trunk's lease is
    /// `LeaseDeadline(u64::MAX)` (`branch::catalog::TRUNK_LEASE`), which as an `i64` is `-1`; a
    /// `RowId` for a non-integer primary key is `fnv64` of the key bytes, which is uniformly
    /// distributed over the whole `u64` range, so half of them are negative as `i64`. `Value::Decimal`
    /// holds the digits verbatim and renders as `numeric` on the wire, so no value is misreported
    /// and none is rounded. A wrong number with a right label is the failure mode this avoids.
    pub fn columns(&self) -> Vec<Column> {
        let text = |n: &str, w: u16| Column { name: n.into(), data_type: DataType::Varchar(w), nullable: false };
        let text_null =
            |n: &str, w: u16| Column { name: n.into(), data_type: DataType::Varchar(w), nullable: true };
        let int = |n: &str| Column { name: n.into(), data_type: DataType::Integer, nullable: false };
        let big = |n: &str| Column { name: n.into(), data_type: DataType::BigInt, nullable: false };
        let big_null = |n: &str| Column { name: n.into(), data_type: DataType::BigInt, nullable: true };
        let dec = |n: &str| Column { name: n.into(), data_type: DataType::Decimal, nullable: false };

        match self {
            SystemView::Branches => vec![
                big("branch_id"),
                int("generation"),
                text("branch", 32),
                // NULL only for the trunk, which has no parent. A sentinel here would be
                // indistinguishable from branch 0, which is the trunk itself.
                big_null("parent_id"),
                big("fork_epoch"),
                big("root_page_id"),
                text("state", 16),
                int("depth"),
                int("arenas"),
                int("live_children"),
                dec("lease_deadline"),
            ],
            SystemView::Runs => vec![
                big("branch_id"),
                int("generation"),
                text("branch_name", 32),
                int("prov_id"),
                text("agent_id", 64),
                text("run_id", 64),
                text("model", 64),
                text("model_version", 32),
                text("prompt_hash", 64),
                big("started_at"),
                text("parent_branch", 32),
            ],
            SystemView::RowAuthors => vec![
                text("table_name", 64),
                dec("row_id"),
                int("prov_id"),
                text("agent_id", 64),
                text("run_id", 64),
                text("model", 64),
                text("model_version", 32),
            ],
            SystemView::Quarantine => vec![
                big("branch_id"),
                int("generation"),
                text("branch", 32),
                // NULL is a real answer, not a missing one: branch state is durable and the reason
                // is not, so a branch held before a restart is still held and its reason is gone.
                // Reporting that as an empty string would claim it was quarantined for no reason.
                text_null("reason", 512),
            ],
            SystemView::RunActivity => vec![
                big("branch_id"),
                int("generation"),
                text("branch_name", 32),
                text_null("agent_id", 64),
                text_null("run_id", 64),
                big("ops_captured"),
                big("guards_captured"),
                big("staged_rows"),
                big("rows_read_exact"),
                big("scan_reads"),
                big("scan_rows_observed"),
                big("blind_writes"),
            ],
        }
    }

    pub fn schema(&self) -> Schema {
        Schema::new(self.columns())
    }

    /// The view's own columns as a bound schema, qualified by `qualifier` (the alias if the query
    /// gave one, otherwise the view name).
    fn bound_columns(&self, qualifier: &str) -> Vec<BoundColumn> {
        self.columns()
            .into_iter()
            .map(|c| BoundColumn {
                qualifier: qualifier.to_string(),
                name: c.name,
                data_type: c.data_type,
                nullable: c.nullable,
            })
            .collect()
    }

    /// Materialise every row of this view, every column, in a deterministic order.
    ///
    /// **The ordering is load-bearing, not cosmetic.** `LogBranchCatalog` keeps its records in a
    /// `HashMap` and `all_branches` returns `values().cloned()`, so the order it hands back varies
    /// between runs of the same program. A view that inherited that order would make every
    /// positional assertion in every test flaky, and would report a different answer to two
    /// identical queries.
    pub fn materialise(
        &self,
        catalog: &Catalog,
        runtime: &AgentRuntime,
    ) -> Result<Vec<Vec<Value>>, FerroError> {
        match self {
            SystemView::Branches => branches_rows(runtime),
            SystemView::Runs => runs_rows(runtime),
            SystemView::RowAuthors => row_author_rows(catalog, runtime),
            SystemView::Quarantine => quarantine_rows(runtime),
            SystemView::RunActivity => run_activity_rows(runtime),
        }
    }
}

fn u64_text(v: u64) -> Value {
    // Digits, verbatim. See `SystemView::columns` for why not `BigInt`.
    Value::Decimal(v.to_string())
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn state_name(s: BranchState) -> &'static str {
    match s {
        BranchState::Live => "Live",
        BranchState::Quarantined => "Quarantined",
        BranchState::Reaping => "Reaping",
        BranchState::Reaped => "Reaped",
    }
}

fn branches_rows(runtime: &AgentRuntime) -> Result<Vec<Vec<Value>>, FerroError> {
    // `all_branches`, not `live_branches`: a view whose job is to show what the branch engine holds
    // must show a quarantined or reaping branch, and `live_branches` filters to `Live` so it can
    // never return one. The `state` column is how a reader narrows it.
    let mut records = runtime.branches().all_branches()?;
    records.sort_by_key(|r| (r.branch_id.id, r.branch_id.generation));
    Ok(records
        .into_iter()
        .map(|r| {
            vec![
                Value::BigInt(r.branch_id.id as i64),
                Value::Integer(r.branch_id.generation as i32),
                Value::Varchar(r.branch_id.to_string()),
                match r.parent_id {
                    Some(p) => Value::BigInt(p.id as i64),
                    None => Value::Null,
                },
                Value::BigInt(r.fork_epoch.0 as i64),
                Value::BigInt(r.root_page_id as i64),
                Value::Varchar(state_name(r.state).into()),
                Value::Integer(r.depth as i32),
                Value::Integer(r.arenas.len() as i32),
                Value::Integer(r.live_children.len() as i32),
                u64_text(r.lease_deadline.0),
            ]
        })
        .collect())
}

fn runs_rows(runtime: &AgentRuntime) -> Result<Vec<Vec<Value>>, FerroError> {
    // One row per branch that has a live run behind it. `run_of` answers from the workspace, which
    // `seal` drops the moment a branch merges or is abandoned, so this view is about work in
    // flight. `ferro_row_authors` is the question that keeps answering afterwards, and saying so
    // here is the difference between an empty view and a lost one.
    let mut records = runtime.branches().all_branches()?;
    records.sort_by_key(|r| (r.branch_id.id, r.branch_id.generation));
    let mut out = Vec::new();
    for rec in records {
        let Some(run) = runtime.run_of(rec.branch_id) else { continue };
        out.push(vec![
            Value::BigInt(rec.branch_id.id as i64),
            Value::Integer(rec.branch_id.generation as i32),
            Value::Varchar(format!("b_{}", rec.branch_id.id)),
            Value::Integer(run.prov_id.0 as i32),
            Value::Varchar(run.agent_id.clone()),
            Value::Varchar(run.run_id.clone()),
            Value::Varchar(run.model.clone()),
            Value::Varchar(run.model_version.clone()),
            Value::Varchar(hex32(&run.prompt_hash)),
            Value::BigInt(run.started_at as i64),
            Value::Varchar(run.parent_branch.to_string()),
        ]);
    }
    Ok(out)
}

fn row_author_rows(catalog: &Catalog, runtime: &AgentRuntime) -> Result<Vec<Vec<Value>>, FerroError> {
    // `Catalog::tables` is a `HashMap`, so the table order it yields varies run to run. Sorted, and
    // sorted by name before the per-table lookup rather than after, so a table with no attributed
    // rows costs nothing and the result is grouped the way a reader expects.
    let mut names: Vec<&str> = catalog.tables.keys().map(|s| s.as_str()).collect();
    names.sort_unstable();
    let mut out = Vec::new();
    for table in names {
        let mut authored = runtime.authors_of(table);
        authored.sort_by_key(|(row, _)| row.0);
        for (row, run) in authored {
            out.push(vec![
                Value::Varchar(table.to_string()),
                u64_text(row.0),
                Value::Integer(run.prov_id.0 as i32),
                Value::Varchar(run.agent_id.clone()),
                Value::Varchar(run.run_id.clone()),
                Value::Varchar(run.model.clone()),
                Value::Varchar(run.model_version.clone()),
            ]);
        }
    }
    Ok(out)
}

fn quarantine_rows(runtime: &AgentRuntime) -> Result<Vec<Vec<Value>>, FerroError> {
    let mut held = runtime.quarantined_branches()?;
    held.sort_by_key(|b| (b.id, b.generation));
    Ok(held
        .into_iter()
        .map(|b| {
            vec![
                Value::BigInt(b.id as i64),
                Value::Integer(b.generation as i32),
                Value::Varchar(b.to_string()),
                // `None` reaches the client as SQL NULL rather than as the string "None" or as an
                // empty reason. The gate always records one, so a NULL here means the reason did
                // not survive a restart — which a reader has to be able to tell apart from a
                // branch held for nothing.
                match runtime.quarantine_reason(b) {
                    Some(r) => Value::Varchar(r),
                    None => Value::Null,
                },
            ]
        })
        .collect())
}

fn run_activity_rows(runtime: &AgentRuntime) -> Result<Vec<Vec<Value>>, FerroError> {
    Ok(runtime
        .run_activity()
        .into_iter()
        .map(|a| {
            let (agent, run) = match &a.run {
                Some(r) => (Value::Varchar(r.agent_id.clone()), Value::Varchar(r.run_id.clone())),
                None => (Value::Null, Value::Null),
            };
            vec![
                Value::BigInt(a.branch.id as i64),
                Value::Integer(a.branch.generation as i32),
                Value::Varchar(a.branch_name.clone()),
                agent,
                run,
                Value::BigInt(a.ops_captured as i64),
                Value::BigInt(a.guards_captured as i64),
                Value::BigInt(a.staged_rows as i64),
                Value::BigInt(a.rows_read_exact as i64),
                Value::BigInt(a.scan_reads as i64),
                Value::BigInt(a.scan_rows_observed as i64),
                Value::BigInt(a.blind_writes as i64),
            ]
        })
        .collect())
}

/// A relation that is already in memory, presented as an [`Executor`] so the ordinary operators can
/// read it.
///
/// Exists so a view's `WHERE` and projection run through [`Filter`] and [`Projection`] verbatim
/// rather than through a second evaluator written for views. The `RecordId` is synthetic — there is
/// no heap slot behind these rows — and nothing downstream of a projection consumes it.
struct MaterialisedRows {
    rows: std::vec::IntoIter<Vec<Value>>,
    at: u16,
}

impl MaterialisedRows {
    fn new(rows: Vec<Vec<Value>>) -> Self {
        MaterialisedRows { rows: rows.into_iter(), at: 0 }
    }
}

impl Executor for MaterialisedRows {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        let row = self.rows.next()?;
        let rid = RecordId { page_id: 0, slot_num: self.at };
        self.at = self.at.wrapping_add(1);
        Some(Ok((rid, row)))
    }
}

/// Run one `SELECT` against a system view.
///
/// Binds the query's projection and `WHERE` against the view's declared schema, builds the logical
/// plan so its [`output_schema`](LogicalPlan::output_schema) is what names the result's columns, and
/// evaluates through the engine's own [`Filter`] and [`Projection`].
///
/// # Refusals
///
/// Both are refusals rather than best-effort answers, because the alternative in each case is to
/// answer a different question than the one asked and not say so:
///
/// * `AS OF BRANCH b` — a system view is not branch-relative. Its rows describe the branch engine
///   itself, so there is no "as this branch saw it" state to read; answering from the current state
///   under an `AS OF` qualifier would report the present as the past.
/// * a join — a view has no `TableEntry`, so the optimizer cannot lower a scan of it, and the join
///   operators are reached only through `lower`. Refusing names the limit; ignoring the join clause
///   would silently return the left side alone.
pub fn run_select(
    view: SystemView,
    stmt: &Stmt,
    catalog: &Catalog,
    runtime: &AgentRuntime,
) -> Result<NamedRows, FerroError> {
    let Stmt::Select { from, columns, where_clause, joins } = stmt else {
        return Err(FerroError::Bind(format!(
            "{} is a system view and can only be read by SELECT",
            view.name()
        )));
    };
    if let Some(b) = &from.as_of {
        return Err(FerroError::Bind(format!(
            "{} is a system view describing the branch engine itself, so it has no state 'AS OF \
             BRANCH {}' to read; drop the qualifier",
            view.name(),
            b.name
        )));
    }
    if !joins.is_empty() {
        return Err(FerroError::Bind(format!(
            "joining {} is not supported: a system view has no catalog table entry for the \
             optimizer to lower a scan of",
            view.name()
        )));
    }

    let qualifier = from.alias.clone().unwrap_or_else(|| view.name().to_string());
    let mut scope = Scope::new();
    scope.add_table(&qualifier, &view.schema())?;
    let binder = Binder::new(catalog);

    let mut logical = LogicalPlan::Scan {
        table: view.name().to_string(),
        alias: from.alias.clone(),
        output: view.bound_columns(&qualifier),
    };
    let predicate = match where_clause {
        Some(w) => {
            let bound = binder.bind_expr(w.clone(), &scope)?;
            logical = LogicalPlan::Filter { input: Box::new(logical), predicate: bound.clone() };
            Some(bound)
        }
        None => None,
    };
    let (exprs, output) = binder.bind_projection(columns.clone(), &scope)?;
    logical = LogicalPlan::Projection { input: Box::new(logical), exprs: exprs.clone(), output };

    // The output columns come from the plan, not from the view definition, so `SELECT reason FROM
    // ferro_quarantine` advertises one column named `reason` and not all four.
    let schema = logical.output_schema();

    let mut root: Box<dyn Executor> = Box::new(MaterialisedRows::new(view.materialise(catalog, runtime)?));
    if let Some(pred) = predicate {
        root = Box::new(Filter { child: root, predicate: pred });
    }
    let mut root: Box<dyn Executor> = Box::new(Projection { child: root, exprs });

    let mut rows = Vec::new();
    while let Some(next) = root.next() {
        let (_, values) = next?;
        // A width that disagrees with the advertised schema would put every value under the wrong
        // column name for the rest of the result, which is exactly the mislabelling this module
        // exists to remove. It cannot happen — `bind_projection` returns `exprs` and `output` of
        // equal length and `Projection` emits one value per expr — so it is a bug if it does, and a
        // refusal beats shipping mislabelled rows.
        if values.len() != schema.len() {
            return Err(FerroError::Internal(format!(
                "{} produced a {}-column row against a {}-column schema",
                view.name(),
                values.len(),
                schema.len()
            )));
        }
        rows.push(values);
    }
    Ok(NamedRows::new(schema, rows))
}

/// The view this name refers to, unless a real table has already claimed it.
///
/// **An existing table wins, and that is a correctness rule rather than a courtesy.** A database
/// created before B9 can hold a table called `ferro_runs`: `Catalog::load` rebuilds `tables` straight
/// from the catalog pages and never passes through `create_table`, so
/// [`reject_view_name_collision`] never sees it. If the view answered for that name, every row of a
/// real table would be permanently unreachable — writes accepted, reads answering something else,
/// no error anywhere. Yielding to the table cannot lose anything, because a view's rows are derived
/// and can be asked for under any other spelling, while the table's rows exist nowhere else.
///
/// Going forward the ambiguity cannot be created at all, because `create_table` refuses it. This
/// handles only the databases that already exist.
pub fn view_for(catalog: &Catalog, name: &str) -> Option<SystemView> {
    if catalog.get_table(name).is_some() {
        return None;
    }
    SystemView::by_name(name)
}

/// The answer for a statement that names a system view, or `None` when none is involved.
///
/// One interception point, so the executor does not have to know which statement shapes name a
/// relation. Every non-`SELECT` shape is a refusal that says what a view is — the alternative is what
/// these statements did before, which was to fall through to `require_table` and answer `unknown
/// table 'ferro_runs'` about a name the very next `SELECT` resolves. A wrong reason for a right
/// refusal sends the reader looking for a typo.
pub fn intercept(
    stmt: &Stmt,
    catalog: &Catalog,
    runtime: &AgentRuntime,
) -> Option<Result<NamedRows, FerroError>> {
    let read_only = |view: SystemView, verb: &str| {
        Some(Err(FerroError::Constraint(format!(
            "{} is a read-only system view, so it cannot be the target of {verb}; it is materialised \
             from the agent layer on every read and has no rows of its own to change",
            view.name()
        ))))
    };
    match stmt {
        Stmt::Select { from, .. } => {
            let view = view_for(catalog, &from.name)?;
            Some(run_select(view, stmt, catalog, runtime))
        }
        Stmt::Insert { table, .. } => read_only(view_for(catalog, table)?, "INSERT"),
        Stmt::Update { table, .. } => read_only(view_for(catalog, table)?, "UPDATE"),
        Stmt::Delete { table, .. } => read_only(view_for(catalog, table)?, "DELETE"),
        Stmt::DropTable { table } => read_only(view_for(catalog, table)?, "DROP TABLE"),
        Stmt::CreateIndex { table, .. } => read_only(view_for(catalog, table)?, "CREATE INDEX"),
        Stmt::Analyze { table } => read_only(view_for(catalog, table)?, "ANALYZE"),
        // `EXPLAIN` of a view would have to describe a physical plan, and there is none: the rows are
        // materialised rather than scanned. Refused by name rather than left to answer `unknown
        // table`, which is what `planner::explain` says for a name it cannot find in the catalog.
        Stmt::Explain(inner) => match &**inner {
            Stmt::Select { from, .. } => {
                let view = view_for(catalog, &from.name)?;
                Some(Err(FerroError::Bind(format!(
                    "EXPLAIN cannot describe {}: a system view is materialised from the agent \
                     layer's APIs rather than scanned, so it has no physical plan. Its columns are \
                     fixed and listed in catalog::system_views",
                    view.name()
                ))))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Refuse a `CREATE TABLE` that would shadow a system view.
///
/// **A guard over the resulting state, not over the statement text.** A table named
/// `ferro_quarantine` would be permanently unreadable: the executor recognises a view name before
/// any other route, so every `SELECT` against it would answer from the view and the table's rows
/// would be invisible for as long as it existed — writes accepted, reads answering something else.
/// Refusing at creation makes that state unrepresentable rather than documented.
///
/// Checked here in `catalog` rather than in the parser so it holds for every path that creates a
/// table, not only for the one that spells it in SQL.
///
/// **Stated blind spot:** this cannot see a table that already exists. `Catalog::load` rebuilds
/// `tables` from the catalog pages directly and does not come through here, so a database written
/// before these views existed can hold a colliding name. That case is handled at the other end
/// instead — [`view_for`] yields to a real table — because refusing to open such a database would
/// take away the data rather than protect it.
pub fn reject_view_name_collision(name: &str) -> Result<(), FerroError> {
    if SystemView::by_name(name).is_some() {
        return Err(FerroError::Constraint(format!(
            "'{name}' is a read-only system view, so a table cannot take that name: every SELECT \
             against it would answer from the view and the table's rows would be unreachable. The \
             system views are: {}",
            VIEW_NAMES.join(", ")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_view_name_resolves_and_nothing_else_does() {
        for name in VIEW_NAMES {
            assert!(SystemView::by_name(name).is_some(), "{name} did not resolve");
        }
        assert_eq!(SystemView::ALL.len(), VIEW_NAMES.len());
        // The allowlist is exact. A prefix rule would make `ferro_anything` a view.
        for name in ["ferro", "ferro_", "ferro_branches2", "FERRO_BRANCHES", "branches", ""] {
            assert!(SystemView::by_name(name).is_none(), "{name} resolved and should not have");
        }
    }

    #[test]
    fn view_names_and_declared_columns_agree_with_the_enum() {
        for (i, v) in SystemView::ALL.into_iter().enumerate() {
            assert_eq!(v.name(), VIEW_NAMES[i]);
            let cols = v.columns();
            assert!(!cols.is_empty(), "{} declares no columns", v.name());
            let mut names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
            names.sort_unstable();
            let before = names.len();
            names.dedup();
            assert_eq!(before, names.len(), "{} declares a duplicate column name", v.name());
        }
    }

    /// The breaking shape: `LeaseDeadline(u64::MAX)` — the trunk's lease — and a `RowId` above
    /// `i64::MAX`, which is what `fnv64` of a varchar primary key produces about half the time.
    /// Declared as `BIGINT` and cast, both come back negative: `-1` for the lease. The digits are
    /// the only rendering that survives.
    #[test]
    fn u64_values_past_i64_max_are_not_reported_as_negative() {
        // Matched on the variant AND its digits, not with `==`: `Value`'s `PartialEq` goes through
        // its cross-type `Ord`, so an assertion written as equality would also pass for a numerically
        // equal value of a different type — which is exactly the confusion under test.
        match u64_text(u64::MAX) {
            Value::Decimal(d) => assert_eq!(d, "18446744073709551615"),
            other => panic!("a u64 past i64::MAX must render as exact digits, got {other:?}"),
        }
        assert_eq!(u64::MAX as i64, -1, "the cast this avoids");
        let hashish = 0xf000_0000_0000_0001u64;
        assert!((hashish as i64) < 0, "the breaking shape: a fnv64 row id above i64::MAX");
        match u64_text(hashish) {
            Value::Decimal(d) => assert_eq!(d, "17293822569102704641"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn a_table_may_not_take_a_view_name_but_an_ordinary_name_is_fine() {
        for name in VIEW_NAMES {
            let err = reject_view_name_collision(name).expect_err("must refuse");
            assert!(err.to_string().contains(name), "{err}");
        }
        // Anti-vacuity: the guard is not simply refusing everything.
        for name in ["inventory", "ferro", "ferro_branchesx", "t"] {
            assert!(reject_view_name_collision(name).is_ok(), "{name} was refused");
        }
    }

    #[test]
    fn hex_of_a_prompt_hash_is_64_characters() {
        assert_eq!(hex32(&[0u8; 32]).len(), 64);
        assert_eq!(&hex32(&[0xab; 32])[..4], "abab");
    }
}
