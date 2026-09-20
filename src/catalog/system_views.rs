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
//! # A view is a snapshot per source, not one snapshot across sources
//!
//! Stated because the alternative is for a reader to assume otherwise. Three of the five views read
//! more than one source through separate lock acquisitions — `ferro_row_authors` asks `authors_of`
//! per table, `ferro_quarantine` takes the held set and then a reason per branch, and `ferro_runs`
//! takes its live-run snapshot and then reads branch records — so a `MERGE` landing mid-scan can
//! leave a row out that was live when the scan began, or show one table's newly published rows while
//! another table's from the same merge are missing. `ferro_run_activity` is built from a single
//! acquisition.
//!
//! **`ferro_runs` used to be the worst of these and is now the mildest** (D28). It took an
//! `all_branches` snapshot and then asked `run_of` PER BRANCH, so at 10⁶ branches it acquired the
//! runtime's one `Mutex` 10⁶ times, each acquisition seeing a possibly different world. It now takes
//! [`AgentRuntime::live_runs`] once and reads records over the span those runs occupy — strictly
//! fewer acquisitions and a strictly more consistent answer, which is the unusual case of the fast
//! version also being the more honest one.
//!
//! Holding one lock across all of it is not the fix: the runtime's `Mutex` and the branch catalog's
//! `RwLock` are currently only ever nested in one order, and widening a view's critical section is how
//! that stops being true. The honest answer is that these are diagnostics, not a serialisable read,
//! and this paragraph is where that is written down.
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
//! # What a [`ViewHint`] changes about that, and what it deliberately does not
//!
//! Materialise-then-filter costs O(source) per statement whatever the predicate selects, so at 10⁶
//! branches `SELECT * FROM ferro_branches WHERE branch_id = 1` built a million rows to return one
//! and took 22.2 s doing it — 0.13 ms after (`bench/d28_view_pushdown.txt`). D28 lets the generator
//! be told, in a
//! vocabulary that cannot express a wrong answer, which rows are worth building — and nothing else.
//! `Filter` still runs, unchanged, over whatever the generator hands back, and remains the only
//! thing that decides what a row means. A hint can only ever ask for MORE rows than the query needs,
//! never fewer, so the paragraph above still holds: there is no second implementation of comparison
//! semantics here, because a hint is not a comparison. See [`ViewHint`].
//!
//! # Real columns on the wire
//!
//! The output columns come from [`LogicalPlan::output_schema`], which is why this module builds a
//! logical plan it never lowers. Before B9 the column *names and types* in `BoundColumn` were dead
//! weight — every call site used `output_schema()` only for `.len()`, so pgwire had nothing to
//! advertise and named columns `column1..N`. A view whose columns arrive as `column1..column10` is
//! not a typed result a client can consume, so the schema is threaded from the plan to the wire
//! and [`Outcome::Table`](crate::execution::executor::Outcome::Table) is the shape that carries it.

use std::collections::BTreeSet;

use crate::agent_sql::runtime::AgentRuntime;
use crate::binder::binder::{Binder, BoundColumn, BoundExpr, Scope};
use crate::branch::types::BranchState;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::{Column, DataType, Value};
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::execution::executor::Executor;
use crate::execution::filter::Filter;
use crate::execution::projection::Projection;
use crate::parser::parser::Stmt;
use crate::parser::scanner::TokenType;
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
    /// # Column names must be selectable, which rules two obvious names out
    ///
    /// `branch` and `model` are **reserved words** in this SQL surface (`scanner.rs` maps them to
    /// `TokenType::Branch` and `TokenType::Model` for `AS OF BRANCH` and `MODEL '...'`), so a column
    /// called either one can only ever be reached through `SELECT *` — `SELECT branch FROM
    /// ferro_quarantine` fails to scan. Both were originally named that way and it was not caught by
    /// any assertion that read columns out of a `SELECT *`, which is every obvious one. They are
    /// `branch_name` and `model_name`, and
    /// `integration_system_views::every_declared_column_is_selectable_by_name` is the guard that
    /// catches the next collision instead of a reader finding it.
    ///
    /// `branch_name` is the spelling `AS OF BRANCH` and `MERGE BRANCH` accept — `b_{id}` — rather
    /// than `BranchId`'s `Display` (`b1@g0`), because a name a reader can paste into a statement is
    /// worth more than a second rendering of the identity that `branch_id` and `generation` already
    /// carry. It resolves only while the branch has a live session: the trunk has none, so `b_0` is
    /// a spelling and not a resolvable name.
    ///
    /// # Two width decisions that are not stylistic
    ///
    /// `lease_deadline` and `row_id` are `DECIMAL`, not `BIGINT`, because both are `u64` values that
    /// genuinely exceed `i64::MAX` in this codebase and `BIGINT` is `i64`.
    ///
    /// The one other narrowing cast here is `prov_id`, a `u32` announced as `int4`. Listed rather than
    /// left out: it is allocated as `runs.len() + 1`, so it takes 2^31 interned runs in one process to
    /// go negative, and unlike the two above there is no sentinel that reaches the boundary
    /// deliberately. `branch_id.id`, `fork_epoch`, `started_at` and `txn_id` are monotonic counters
    /// with no `u64::MAX` sentinel anywhere, and `root_page_id` is a `u32` under `BIGINT`, which is
    /// lossless. The trunk's lease is
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
                // **The id slot's CURRENT generation, which is not always the one the branch was
                // minted with.** `BranchRecord` carries both: `branch_id.generation` is as minted
                // and never changes, while `generation` is bumped by `mark_reaped` so a stale
                // handle fails loudly. For a live branch they are equal; for a reaped one they are
                // not, and reporting the minted value showed a reaped slot sitting at generation 0
                // — the very number `check_readable` rejects, presented as the slot's identity. The
                // reaper's own convention for naming a slot is
                // `BranchId::new(r.branch_id.id, r.generation)` (`branch/reaper.rs`), and this
                // matches it.
                int("generation"),
                text("branch_name", 32),
                // NULL only for the trunk, which has no parent. A sentinel here would be
                // indistinguishable from branch 0, which is the trunk itself.
                big_null("parent_id"),
                big("fork_epoch"),
                big("root_page_id"),
                text("state", 16),
                // **BIGINT, not INT — D60.** The depth field widened to `u32` precisely so a deep
                // chain is reported honestly, and narrowing it here would have put the lie one
                // layer above the record instead of in it: `u32` does not fit `i32`. Found by a
                // fresh-context review of D60, which noticed the cast below.
                big("depth"),
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
                text("model_name", 64),
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
                text("model_name", 64),
                text("model_version", 32),
            ],
            SystemView::Quarantine => vec![
                big("branch_id"),
                // From `quarantined_branches`, which yields `branch_id`, so this is the generation
                // as minted. Equal to the slot's current generation here and not by luck: only
                // `mark_reaped` bumps it, and a record in state `Quarantined` has not been reaped.
                // `ferro_branches` is where the two can differ, and its column doc says so.
                int("generation"),
                text("branch_name", 32),
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
        self.materialise_hinted(catalog, runtime, &ViewHint::All)
    }

    /// The same rows, generated under a [`ViewHint`] the generator is free to ignore.
    ///
    /// The hint can only ever make this produce a **superset** of what the query needs. It is never
    /// asked, and is not able, to decide what a row means: [`Filter`] runs over whatever comes back
    /// and stays the sole authority. See [`ViewHint`] for why that asymmetry is the whole design.
    pub fn materialise_hinted(
        &self,
        catalog: &Catalog,
        runtime: &AgentRuntime,
        hint: &ViewHint,
    ) -> Result<Vec<Vec<Value>>, FerroError> {
        match self {
            SystemView::Branches => branches_rows(runtime, hint),
            SystemView::Runs => runs_rows(runtime, hint),
            SystemView::RowAuthors => row_author_rows(catalog, runtime, hint),
            // **These two decline the hint, and that is a decision rather than an omission.** Each
            // already generates rows in proportion to its own ANSWER rather than to the catalog:
            // `quarantined_branches` reads the `Quarantined` state span, and `run_activity` walks
            // the workspace map under one lock. Narrowing them would be a `retain` over a list that
            // is already as short as the result, which buys nothing and adds a second place for the
            // hint to be wrong. Measured at 10⁶ branches (`bench/d28_view_pushdown.txt`): 0.058 ms
            // and 0.029 ms, against 22.2 s for `ferro_branches`.
            SystemView::Quarantine => quarantine_rows(runtime),
            SystemView::RunActivity => run_activity_rows(runtime),
        }
    }

    /// The one column of this view a hint can narrow on: the column whose order the generator's
    /// source is already keyed by, so a range over it is a range over the source.
    ///
    /// Answered by NAME against [`Self::columns`] rather than by a hardcoded index, so reordering a
    /// view's columns cannot silently point the hint at a different column — which would narrow on
    /// the wrong values and lose rows, the one failure this design otherwise makes impossible.
    fn key_column(&self) -> Option<(usize, KeyKind)> {
        let (name, kind) = match self {
            SystemView::Branches | SystemView::Runs => ("branch_id", KeyKind::BranchId),
            SystemView::RowAuthors => ("table_name", KeyKind::TableName),
            // Declining the hint, as `materialise_hinted` explains.
            SystemView::Quarantine | SystemView::RunActivity => return None,
        };
        self.columns().iter().position(|c| c.name == name).map(|i| (i, kind))
    }
}

/// Which lattice a view's key column narrows in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    /// A `u64` branch id, presented as `BIGINT`. Narrows to an inclusive interval.
    BranchId,
    /// A table name. Narrows to a set of names.
    TableName,
}

/// A conservative narrowing of the rows a view has to generate — **a hint, never the authority.**
///
/// # The asymmetry this type exists to enforce
///
/// This module's header says the reuse of [`Filter`] and [`Projection`] "is the point": a view gets
/// the engine's comparison semantics rather than a second implementation of them that can drift.
/// That is correct and this does not undo it. What it does is let the *generator* be told, in a
/// vocabulary too small to express a wrong answer, that it need not build rows nobody asked for.
///
/// Every variant means **"generate at least these"**, never "these are the answer". A generator may
/// return more rows than a hint permits — including all of them, which is what
/// [`ViewHint::All`] and every unrecognised predicate produce — and the result is identical, only
/// slower. A generator may never return fewer. `Filter` then runs over whatever arrived and decides
/// what a row means, exactly as before.
///
/// So the failure mode of a bug in here is a slow query, not a wrong one. The failure mode of
/// pushing the predicate itself into the generator — the option this replaces — is a wrong one.
///
/// # Prior art, named rather than reinvented
///
/// This is the SARGable / recheck split: Postgres separates an index *condition*, which chooses
/// which heap tuples to fetch, from a *recheck* qualifier that is evaluated on every tuple fetched.
/// The index condition is allowed to be lossy in exactly one direction. Nothing here is novel and
/// it should not be presented as if it were.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewHint {
    /// Generate everything. The hint that obliges nothing, and therefore the only one that is
    /// always safe — which is why it is what every predicate this cannot read falls back to.
    All,
    /// Generate **at least** every row whose `branch_id` lies in `[lo, hi]`, inclusive.
    ///
    /// `lo > hi` is an empty range and is reachable: `branch_id >= 9 AND branch_id <= 3`.
    BranchIds { lo: u64, hi: u64 },
    /// Generate **at least** every row whose `table_name` appears in this set.
    Tables(BTreeSet<String>),
}

impl ViewHint {
    /// What this statement lets a generator skip, read off the **bound** predicate.
    ///
    /// Bound rather than parsed, so a column is a resolved offset and a literal has already been
    /// through the binder — this never has to guess what a name refers to.
    ///
    /// Returns [`ViewHint::All`] for anything it does not fully understand, which is most things.
    /// That is the intended behaviour and not a gap to be closed later: the set of predicates worth
    /// recognising is small (an equality or a range on the view's own key), and every predicate
    /// outside it must cost one full generation rather than a guess.
    pub fn for_select(view: SystemView, stmt: &Stmt, catalog: &Catalog) -> Result<Self, FerroError> {
        let Stmt::Select { from, where_clause, .. } = stmt else { return Ok(ViewHint::All) };
        let Some(w) = where_clause else { return Ok(ViewHint::All) };
        let qualifier = from.alias.clone().unwrap_or_else(|| view.name().to_string());
        let mut scope = Scope::new();
        scope.add_table(&qualifier, &view.schema())?;
        let bound = Binder::new(catalog).bind_expr(w.clone(), &scope)?;
        Ok(ViewHint::from_bound(view, &bound))
    }

    /// The same, off a predicate that is **already bound** against the view's own schema.
    ///
    /// The one place a hint is derived. `run_select` reaches it with the predicate it bound for the
    /// `Filter`; [`Self::for_select`] reaches it by binding one itself. Two entry points, one
    /// extraction, so there is no second reading of a predicate to drift from the first.
    pub fn from_bound(view: SystemView, predicate: &BoundExpr) -> Self {
        let Some((key, kind)) = view.key_column() else { return ViewHint::All };
        match kind {
            KeyKind::BranchId => id_hint(predicate, key),
            KeyKind::TableName => match name_constraint(predicate, key) {
                Some(names) => ViewHint::Tables(names),
                None => ViewHint::All,
            },
        }
    }

    /// Is `id` inside this hint's branch-id range? `true` for every hint that does not constrain
    /// branch ids, because a hint that says nothing excludes nothing.
    fn admits_id(&self, id: u64) -> bool {
        match self {
            ViewHint::BranchIds { lo, hi } => id >= *lo && id <= *hi,
            _ => true,
        }
    }
}

/// The inclusive branch-id range every row satisfying `expr` must lie in.
///
/// # Why the arithmetic happens in `i64` and not in `u64`
///
/// The rows carry `Value::BigInt(id as i64)`, so the engine compares a branch id **as a signed
/// 64-bit number**. An id above `i64::MAX` therefore compares as negative, and `branch_id <= 20`
/// matches it. Extracting `[0, 20]` in the `u64` domain would drop that row: a SUBSET, which is the
/// one thing this type is not allowed to produce.
///
/// So the bounds are gathered in the domain the comparison actually happens in, and converted once,
/// at the end, only when the result is genuinely one contiguous span of ids — which is exactly when
/// the lower bound is non-negative. A predicate whose lower bound is negative wraps around the
/// `u64` range in two pieces and is declined (`All`). `branch_id = 3`, `branch_id >= 10 AND
/// branch_id <= 20` and every other shape a reader would type narrow normally.
fn id_hint(expr: &BoundExpr, key: usize) -> ViewHint {
    let (lo, hi) = signed_bounds(expr, key);
    if lo < 0 {
        return ViewHint::All;
    }
    if hi < lo {
        // Empty, and honestly so: no id satisfies it.
        return ViewHint::BranchIds { lo: 1, hi: 0 };
    }
    ViewHint::BranchIds { lo: lo as u64, hi: hi as u64 }
}

/// Bounds on the key column in the signed domain. `(i64::MIN, i64::MAX)` constrains nothing, and is
/// what every expression this cannot read returns.
fn signed_bounds(expr: &BoundExpr, key: usize) -> (i64, i64) {
    const ANY: (i64, i64) = (i64::MIN, i64::MAX);
    let BoundExpr::BinaryOp { left, operator, right } = expr else { return ANY };
    match operator {
        // A row satisfying both sides satisfies each, so it lies in the intersection.
        TokenType::And => {
            let (a, b) = signed_bounds(left, key);
            let (c, d) = signed_bounds(right, key);
            (a.max(c), b.min(d))
        }
        // The union of two intervals is not an interval, and this vocabulary has only intervals.
        // Widening to everything is the answer that cannot be wrong.
        TokenType::Or => ANY,
        _ => comparison_bounds(left, *operator, right, key).unwrap_or(ANY),
    }
}

/// `col OP literal` or `literal OP col`, as bounds — or `None` for anything else.
fn comparison_bounds(
    left: &BoundExpr,
    op: TokenType,
    right: &BoundExpr,
    key: usize,
) -> Option<(i64, i64)> {
    let (v, op) = match (left, right) {
        (BoundExpr::Column(i), BoundExpr::Literal(v)) if *i == key => (v, op),
        // `3 < branch_id` means the same as `branch_id > 3`. Reading only one order would make the
        // hint depend on how the query was typed.
        (BoundExpr::Literal(v), BoundExpr::Column(i)) if *i == key => (v, mirror(op)),
        _ => return None,
    };
    let n = match v {
        Value::Integer(i) => *i as i64,
        Value::BigInt(i) => *i,
        // Every other literal type is declined rather than converted. A `Decimal("3.0")` or a
        // `Varchar("3")` may well compare equal to a branch id under the engine's own coercion
        // rules, and it is not this function's job to hold a second copy of those rules — that is
        // the drift the whole module refuses. Declining costs one full generation and cannot be
        // wrong.
        _ => return None,
    };
    Some(match op {
        TokenType::Equal => (n, n),
        // `n == i64::MAX` has no representable successor, so there is nothing to narrow to; `ANY`
        // via `None` is correct and a wrapped bound would not be.
        TokenType::Greater => (n.checked_add(1)?, i64::MAX),
        TokenType::GreaterEqual => (n, i64::MAX),
        TokenType::Less => (i64::MIN, n.checked_sub(1)?),
        TokenType::LessEqual => (i64::MIN, n),
        // `!=` excludes one value out of a range, which is not an interval. Declined.
        _ => return None,
    })
}

/// The same comparison with its operands swapped.
fn mirror(op: TokenType) -> TokenType {
    match op {
        TokenType::Greater => TokenType::Less,
        TokenType::GreaterEqual => TokenType::LessEqual,
        TokenType::Less => TokenType::Greater,
        TokenType::LessEqual => TokenType::GreaterEqual,
        // `=` and `!=` are symmetric; anything else is declined upstream anyway.
        other => other,
    }
}

/// The set of table names every row satisfying `expr` must have, or `None` for "any name".
///
/// Unlike the id lattice this one is a SET, so `OR` is expressible here and is taken: a union of
/// two name sets is a name set, where a union of two id intervals is not an interval.
fn name_constraint(expr: &BoundExpr, key: usize) -> Option<BTreeSet<String>> {
    let BoundExpr::BinaryOp { left, operator, right } = expr else { return None };
    match operator {
        TokenType::And => match (name_constraint(left, key), name_constraint(right, key)) {
            (Some(a), Some(b)) => Some(a.intersection(&b).cloned().collect()),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        },
        TokenType::Or => {
            // An unconstrained side means "any name", and a union with "any name" is "any name".
            let (a, b) = (name_constraint(left, key)?, name_constraint(right, key)?);
            Some(a.union(&b).cloned().collect())
        }
        TokenType::Equal => {
            let name = match (&**left, &**right) {
                (BoundExpr::Column(i), BoundExpr::Literal(Value::Varchar(s))) if *i == key => s,
                (BoundExpr::Literal(Value::Varchar(s)), BoundExpr::Column(i)) if *i == key => s,
                _ => return None,
            };
            Some([name.clone()].into_iter().collect())
        }
        // No ordering hint on names: the generator's source is a name-sorted list, so a range would
        // be expressible, but the engine's `Varchar` collation is the authority on what `<` means
        // between two names and this would be a second copy of it.
        _ => None,
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

fn branches_rows(runtime: &AgentRuntime, hint: &ViewHint) -> Result<Vec<Vec<Value>>, FerroError> {
    // `scan`, which is every record whatever its state: a view whose job is to show what the
    // branch engine holds must show a quarantined or reaping branch too, and the `state` column is
    // how a reader narrows it. It arrives in branch-id order, which is what this used to sort into
    // afterwards, so the sort is gone with the dump it replaced.
    //
    // `scan_ids` is the same scan with the hint's bounds on it. The two arms produce the same rows
    // for the same database — the narrow one simply does not build the records `Filter` is about to
    // discard. Measured at 10⁶ branches (`bench/d28_view_pushdown.txt`): `WHERE branch_id = 1` was
    // 22170.73 ms of record hydration to return one row, and is 0.13 ms. `SELECT *` with no `WHERE`
    // is unchanged at ~22 s, which is the control — a query that asks for every row must build
    // every row.
    let scan = match hint {
        ViewHint::BranchIds { lo, hi } => runtime.branches().scan_ids(*lo, *hi)?,
        _ => runtime.branches().scan()?,
    };
    let records = scan.collect::<Result<Vec<_>, FerroError>>()?;
    Ok(records
        .into_iter()
        .map(|r| {
            vec![
                Value::BigInt(r.branch_id.id as i64),
                // `r.generation`, NOT `r.branch_id.generation`. See the column's doc: the record
                // carries both, and only this one is the id slot's current generation.
                Value::Integer(r.generation as i32),
                Value::Varchar(format!("b_{}", r.branch_id.id)),
                match r.parent_id {
                    Some(p) => Value::BigInt(p.id as i64),
                    None => Value::Null,
                },
                Value::BigInt(r.fork_epoch.0 as i64),
                Value::BigInt(r.root_page_id as i64),
                Value::Varchar(state_name(r.state).into()),
                Value::BigInt(r.depth as i64),
                Value::Integer(r.arenas.len() as i32),
                Value::Integer(r.live_children.len() as i32),
                u64_text(r.lease_deadline.0),
            ]
        })
        .collect())
}

fn runs_rows(runtime: &AgentRuntime, hint: &ViewHint) -> Result<Vec<Vec<Value>>, FerroError> {
    // One row per branch that has a live run behind it. `run_of` answers from the workspace, which
    // `seal` drops the moment a branch merges or is abandoned, so this view is about work in
    // flight. `ferro_row_authors` is the question that keeps answering afterwards, and saying so
    // here is the difference between an empty view and a lost one.
    //
    // **The relation is the workspace map, and this now reads it that way round.** It used to scan
    // every branch record and ask `run_of` per record — 10⁶ acquisitions of the runtime's single
    // `Mutex`, the one every `INSERT` also takes, to return one row (W2: 22654.23 ms at 10⁶ here,
    // 44.1 s as S15 measured it; `bench/d28_view_pushdown.txt`). That is not a cost predicate
    // pushdown removes: an unselective query brings every acquisition back. It is 0.05 ms now.
    // `live_runs` takes the lock **once** and hands back the rows' actual source, so the count is
    // bounded by open sessions rather than by branches ever forked.
    //
    // The records are still the authority for `generation`, so they are still read — but only over
    // the span the live runs occupy, in one scan, merged by id. Both sides are in ascending id
    // order, `live_runs` by `BTreeMap` and the scan by key. **Stated rather than hidden:** live
    // sessions scattered to both ends of the id space widen that span back to the whole catalog,
    // which is exactly the scan this replaces and never more than it.
    let mut live = runtime.live_runs();
    live.retain(|(id, _)| hint.admits_id(*id));
    let (Some((first, _)), Some((last, _))) = (live.first(), live.last()) else {
        // No live run, so no row — and no reason to touch the catalog at all.
        return Ok(Vec::new());
    };
    let records = runtime
        .branches()
        .scan_ids(*first, *last)?
        .collect::<Result<Vec<_>, FerroError>>()?;

    let mut out = Vec::with_capacity(live.len());
    let mut at = 0usize;
    for rec in records {
        // Both sides ascend, so the cursor only ever moves forward.
        while at < live.len() && live[at].0 < rec.branch_id.id {
            at += 1;
        }
        if at >= live.len() {
            break;
        }
        if live[at].0 != rec.branch_id.id {
            continue;
        }
        let run = &live[at].1;
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
            // `b_{id}`, not `BranchId`'s `Display` (`b0@g0`). This view already carries `branch_name`
            // in the pasteable spelling and justifies it as "a name a reader can paste into a
            // statement"; putting the other rendering in the next column over made the view argue
            // with itself, and `AS OF BRANCH b0@g0` does not resolve — `state.names` is keyed by this
            // spelling.
            Value::Varchar(format!("b_{}", run.parent_branch.id)),
        ]);
    }
    Ok(out)
}

fn row_author_rows(
    catalog: &Catalog,
    runtime: &AgentRuntime,
    hint: &ViewHint,
) -> Result<Vec<Vec<Value>>, FerroError> {
    // `Catalog::tables` is a `HashMap`, so the table order it yields varies run to run. Sorted, and
    // sorted by name before the per-table lookup rather than after, so a table with no attributed
    // rows costs nothing and the result is grouped the way a reader expects.
    let mut names: Vec<&str> = catalog.tables.keys().map(|s| s.as_str()).collect();
    names.sort_unstable();
    // The narrowing that matters here is not the row count but the number of `authors_of` calls:
    // each one walks a whole table's provenance span, so `WHERE table_name = 'orders'` used to read
    // every other table's provenance in order to throw it away. A name the hint carries that is not
    // a table simply matches nothing, which is the same answer the filter would reach.
    if let ViewHint::Tables(want) = hint {
        names.retain(|n| want.contains(*n));
    }
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
                Value::Varchar(format!("b_{}", b.id)),
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
/// rather than through a second evaluator written for views.
///
/// The `RecordId` is synthetic — there is no heap slot behind these rows — but it is **unique per
/// row**, and that is a guard rather than a nicety. `RecordId.slot_num` is a `u16`, so a counter kept
/// in that field alone repeats after 65,536 rows, and `ferro_row_authors` returns one row per
/// attributed row per table, which passes 65,536 on any real database. Nothing downstream of a
/// projection reads the rid today, so a repeat would be invisible — right up until something does,
/// at which point two different rows would claim one identity and the bug would look like a
/// deduplication fault a long way from here. Spreading one `u64` counter across `page_id` and
/// `slot_num` gives 2^48 distinct ids and costs a shift, so the state is unrepresentable instead of
/// documented.
struct MaterialisedRows {
    rows: std::vec::IntoIter<Vec<Value>>,
    at: u64,
}

impl MaterialisedRows {
    fn new(rows: Vec<Vec<Value>>) -> Self {
        MaterialisedRows { rows: rows.into_iter(), at: 0 }
    }

    /// The `n`th synthetic id. Split so `slot_num`'s 16 bits are the low end and `page_id`'s 32 the
    /// high, which makes ids distinct for the first 2^48 rows.
    fn rid_of(n: u64) -> RecordId {
        RecordId { page_id: (n >> 16) as u32, slot_num: (n & 0xffff) as u16 }
    }
}

impl Executor for MaterialisedRows {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        let row = self.rows.next()?;
        let rid = MaterialisedRows::rid_of(self.at);
        self.at += 1;
        Some(Ok((rid, row)))
    }
}

/// The one refusal for joining a system view, so the two places that can detect it — a view on the
/// left, reached through `run_select`, and a view on the right, reached through `intercept` — cannot
/// drift into two different messages for one condition.
fn join_refusal(view: SystemView) -> FerroError {
    FerroError::Bind(format!(
        "joining {} is not supported: a system view has no catalog table entry for the optimizer to \
         lower a scan of",
        view.name()
    ))
}

/// The columns a `SELECT` on this view will produce, **without running it**.
///
/// pgwire's extended protocol lets a client `Describe` a statement before it `Execute`s it, so the
/// field list has to be answerable at PARSE time — and before this existed, `describe_stmt` bound a
/// view through `scope_for_table`, which looks in `Catalog::tables`, where a view is deliberately
/// absent. The whole statement was rejected as `unknown table 'ferro_branches'` before the executor
/// ever saw it, so the views were readable from the CLI and invisible over the wire.
///
/// It builds the scope exactly as `run_select` does and binds the same projection, so what
/// `Describe` announces and what `Execute` sends agree **by construction** rather than by two
/// copies of the same logic staying in step. That matters here more than usual: this server refuses
/// outright if it described a statement one way and then produced rows another
/// (`"described the statement as returning no rows and then produced some"`), so a drift between
/// the two is not a cosmetic mismatch — it is an error the client sees.
///
/// A projection narrows it: `SELECT reason FROM ferro_quarantine` describes one column, not four.
pub fn describe_select(
    view: SystemView,
    stmt: &Stmt,
    catalog: &Catalog,
) -> Result<Vec<BoundColumn>, FerroError> {
    let Stmt::Select { from, columns, joins, .. } = stmt else {
        return Err(FerroError::Bind(format!(
            "{} is a system view and can only be read by SELECT",
            view.name()
        )));
    };
    if !joins.is_empty() {
        return Err(join_refusal(view));
    }
    let qualifier = from.alias.clone().unwrap_or_else(|| view.name().to_string());
    let mut scope = Scope::new();
    scope.add_table(&qualifier, &view.schema())?;
    let (_, output) = Binder::new(catalog).bind_projection(columns.clone(), &scope)?;
    Ok(output)
}

/// Run one `SELECT` against a system view.
///
/// Binds the query's projection and `WHERE` against the view's declared schema, builds the logical
/// plan so its [`output_schema`](LogicalPlan::output_schema) is what names the result's columns, and
/// evaluates through the engine's own [`Filter`] and [`Projection`].
///
/// **Doc block restored here by D28, from above `describe_select` where it had come to sit.** It
/// describes this function — `describe_select` announces columns and refuses nothing — and while it
/// was attached to the wrong item `run_select` had no documentation at all and the `# Refusals`
/// section below read as a claim about `Describe`.
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
    select_with(view, stmt, catalog, runtime, Pushdown::On)
}

/// The same query with the generator hint **withheld**, so every view is materialised whole.
///
/// This exists for one reason and it is worth stating rather than leaving to be inferred: it is the
/// other half of the drift test. A hint that can only widen what the generator produces is only
/// *provably* harmless if some test runs the same statement both ways and compares the answers, and
/// there is no way to run it the other way without a way to ask for it.
///
/// It is not a fallback, a toggle or a recovery path. Nothing in the engine calls it, and a
/// disagreement between this and [`run_select`] is a bug in the hint rather than a reason to
/// prefer this.
pub fn run_select_unhinted(
    view: SystemView,
    stmt: &Stmt,
    catalog: &Catalog,
    runtime: &AgentRuntime,
) -> Result<NamedRows, FerroError> {
    select_with(view, stmt, catalog, runtime, Pushdown::Off)
}

/// Whether [`select_with`] is allowed to read a hint off the predicate it just bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pushdown {
    On,
    Off,
}

fn select_with(
    view: SystemView,
    stmt: &Stmt,
    catalog: &Catalog,
    runtime: &AgentRuntime,
    pushdown: Pushdown,
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
        return Err(join_refusal(view));
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
    // Read off the predicate **this function already bound**, rather than binding the `WHERE`
    // clause a second time for the hint's benefit. `ViewHint::for_select` binds and then calls the
    // same `from_bound`, so the two entry points cannot extract different hints from one statement.
    let hint = match (pushdown, &predicate) {
        (Pushdown::On, Some(p)) => ViewHint::from_bound(view, p),
        _ => ViewHint::All,
    };

    let (exprs, output) = binder.bind_projection(columns.clone(), &scope)?;
    logical = LogicalPlan::Projection { input: Box::new(logical), exprs: exprs.clone(), output };

    // The output columns come from the plan, not from the view definition, so `SELECT reason FROM
    // ferro_quarantine` advertises one column named `reason` and not all four.
    let schema = logical.output_schema();

    let mut root: Box<dyn Executor> =
        Box::new(MaterialisedRows::new(view.materialise_hinted(catalog, runtime, &hint)?));
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
             from the agent layer on every read, so there is nothing behind it to write to, index or \
             analyse",
            view.name()
        ))))
    };
    match stmt {
        Stmt::Select { from, joins, .. } => {
            if let Some(view) = view_for(catalog, &from.name) {
                return Some(run_select(view, stmt, catalog, runtime));
            }
            // **A view named on the RIGHT of a join, which `from.name` alone does not see.**
            // Without this the statement falls through to `bind_scan` and answers
            // `unknown table 'ferro_runs'` — about a name the very next `SELECT` resolves, and the
            // exact "right refusal, wrong reason" this module exists to remove. `run_select`'s own
            // doc already claims a join is refused by name, so leaving this arm out made that doc
            // false for half the join shapes.
            joins
                .iter()
                .find_map(|j| view_for(catalog, &j.table.name))
                .map(|view| Err(join_refusal(view)))
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

    /// The breaking shape: more than 65,536 rows. A counter living in `RecordId.slot_num` alone is a
    /// `u16`, so row 0 and row 65,536 would carry the same synthetic id — invisible today because
    /// nothing downstream of a projection reads it, and a deduplication fault far from here the moment
    /// something does. `ferro_row_authors` is one row per attributed row per table, so this is
    /// reachable rather than theoretical.
    #[test]
    fn synthetic_row_ids_stay_distinct_past_the_slot_number_width() {
        let boundary = [0u64, 1, 65_535, 65_536, 65_537, 131_071, 131_072, u32::MAX as u64 + 1];
        let mut seen: Vec<RecordId> = Vec::new();
        for n in boundary {
            let rid = MaterialisedRows::rid_of(n);
            assert!(!seen.contains(&rid), "row {n} reused the id of an earlier row: {rid:?}");
            seen.push(rid);
        }
        // The specific collision a u16 counter produces, named so the test cannot pass vacuously.
        assert_ne!(
            MaterialisedRows::rid_of(0),
            MaterialisedRows::rid_of(65_536),
            "the 65,536th row collided with the first"
        );
        assert_eq!(MaterialisedRows::rid_of(65_536).page_id, 1);
        assert_eq!(MaterialisedRows::rid_of(65_536).slot_num, 0);
    }

    #[test]
    fn hex_of_a_prompt_hash_is_64_characters() {
        assert_eq!(hex32(&[0u8; 32]).len(), 64);
        assert_eq!(&hex32(&[0xab; 32])[..4], "abab");
    }
}
