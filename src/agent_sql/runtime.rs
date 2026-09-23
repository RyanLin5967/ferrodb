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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::agent_sql::persistent_map::PersistentMap;

use crate::catalog::alter::{
    conform_row, refuse_if_the_row_cannot_land, resulting_schema, AlterPlan, NARROW_THE_ROW_FIRST,
};
use crate::execution::executor::alteration_of;
use crate::parser::parser::AlterAction;
use crate::agent_sql::changeset::{
    ChangeOutcome, ChangeSet, MergeReport, RowChange, RowChangeKind, RowMergeOutcome,
};
use crate::branch::catalog::LogBranchCatalog;
use crate::tel::MemEffectLog;
use crate::tel::schema_merge::{merge_schema, SchemaEdit};
use crate::agent_sql::changeset::SchemaMergeReport;
use crate::agent_sql::merge_engine::{
    apply_op, check_guards, compose_ops, invert, resolve_cell, CellMerge, CellResolution,
    CellState, PolicyTable,
};
use crate::agent_sql::escrow::EscrowLedger;
use crate::agent_sql::gate::{AssertionResult, GateOutcome};
use crate::agent_sql::paged_rows::{decode_row, encode_row, split_row_key, PageRowChange, PagedRows};
use crate::agent_sql::simulate::Assertion;
use crate::agent_sql::session::{AgentSession, ForkDurability};
use crate::binder::binder::{Binder, BoundExpr, Scope};
use crate::branch::record::{CapabilityEnvelope, RowImage};
use crate::branch::types::{BranchId, BranchState, CommitHash, Epoch, LeaseDeadline, PageId};
use crate::branch::attest::{
    AttestedHistory, Attestation, BranchOp, ContentId, HistoryEntry, InclusionProof, TreeHead,
};
use crate::branch::cherry::{
    cherry_pick as cherry_pick_ops, CherryLog, CherryResult, CherryTarget, CherryWrite, OpSelector,
    RecordedOp,
};
use crate::branch::version_graph::{AncestryError, VersionGraph};
use crate::cow::diff::{diff as cow_diff, Change as CowChange, PageIdentity};
use crate::cow::PageStore;
use crate::branch::{BranchCatalog, Reaper};

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
use crate::planner::plan::{plan, predicate_to_bounds, Plan};
use crate::optimizer::optimizer::split_and;
use crate::catalog::column::DataType;
use crate::provenance::capture::{ProvenanceLog, TxnCapture, WriteRecord};
use crate::provenance::readset::{AccessShape, Bound, PredicateSummary, VersionRef};
use crate::provenance::revert::{DependencyGraph, RevertMode, RevertPlan};
use crate::provenance::store::MemProvenanceStore;
use crate::provenance::sha256::prompt_digest;
use crate::provenance::{ProvId, ProvenanceStore, RunEntity};
use crate::storage::heap_file_manager::RecordId;
use crate::tel::frame::TxnFrame;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use crate::tel::guard::{ArithOp, CmpOp, Guard, GuardExpr};
use crate::tel::ids::{ColId, RowId, TableId, TxnId};
use crate::tel::merge::{ConflictKind, ConflictReport, MergeOutcome, MergePolicy};
use crate::tel::op::{Delta, Op, OpKind};
use crate::tel::EffectLog;
use crate::wal::txn::{ReadView, TxnManager};

/// Default lease on an agent branch. Leases are non-cooperative: expiry does not require the
/// client to call anything (DESIGN.md exit criterion 8).
pub const DEFAULT_LEASE_MILLIS: u64 = 15 * 60 * 1000;

/// **D114 instrument — how many ops the `ours`-side cell scan TOUCHED, and how many it KEPT.**
///
/// A wall-clock number on this box is an upper bound that moves with the build fleet; these are
/// integers, and an integer does not move when the machine is loaded. That is the whole reason
/// they exist, and it is the same reason `wal::log::FSYNC_CALLS` exists.
///
/// The pair is what separates the two mechanisms that both look linear from the outside:
///
/// * `EXAMINED` counts every op the filter looked at — `snapshot.ops.len()` per changed cell,
///   because `filter().map().collect()` has no early exit. This is the cost an INDEX removes.
/// * `MATCHED` counts the ops that survived, which `compose_ops` must then fold. This is the
///   cost an index does NOT remove, because the fold still has to see them.
///
/// If `EXAMINED` grows and `MATCHED` does not, the scan is the defect. If both grow together,
/// indexing divides by the cell count and changes no complexity class — which is exactly the
/// trap D86's attempt 1 fell into, and it reported it rather than shipping it.
///
/// ⚠ Counted per CELL, not per op: one relaxed add per changed cell (four per merge in these
/// harnesses) against a scan of thousands, so the instrument cannot create the slope it measures.
/// A per-op `fetch_add` would have been ~10x the cost of the comparison it wraps.
pub static OURS_SCAN_EXAMINED: AtomicU64 = AtomicU64::new(0);
pub static OURS_SCAN_MATCHED: AtomicU64 = AtomicU64::new(0);

/// `(examined, matched)` since process start. Read twice and subtract to scope it to a phase.
pub fn ours_scan_counters() -> (u64, u64) {
    (
        OURS_SCAN_EXAMINED.load(AtomicOrdering::Relaxed),
        OURS_SCAN_MATCHED.load(AtomicOrdering::Relaxed),
    )
}

/// **How many table rows a merge actually reads — D176.** Same rationale as `OURS_SCAN_*` above,
/// aimed at a different question: whether merge cost is O(table) or O(delta).
///
/// # Why this is a counter and not a stopwatch
///
/// The duration form of this experiment (`examples/d68_merge_is_o_table.rs`) has been VOIDED
/// TWICE — once for timing an in-memory stub, once by a disk emergency mid-sweep. An integer is
/// not a wall-clock number and does not move when the box is loaded, which is the same reason
/// `wal::log::FSYNC_CALLS` and the pair above exist.
///
/// # Why `scan_rows_observed` could not answer it
///
/// `RunActivity::scan_rows_observed` (`:1042`) looks like this counter and is not. It is a
/// per-workspace field derived in `run_activity` by summing `rows_observed` over the
/// `ReadSet::Predicate` entries a SESSION recorded in `state.captures`. A merge's internal scans
/// call `scan_table_where` directly and record nothing into `captures`, so that field is
/// structurally blind to them — it reads zero for a merge that scanned a million rows.
///
/// # ⚠ Counted PER SCAN, not per row
///
/// One relaxed add of `out.len()` per `scan_table_where` call, against a scan of thousands — so
/// the instrument cannot create the slope it measures. `CALLS` is kept beside `ROWS` because the
/// two separate a merge doing one big scan from one doing many small lookups, and those are
/// different complexity classes that produce the same total.
///
/// ⛔ **`ROWS` is rows RETURNED, which is NOT rows read by the engine.** A point lookup with the
/// predicate pushed down returns one row; if the planner declines the index and falls back to a
/// sequential scan, the engine reads the whole table while this counter reports 1. Read it
/// together with `execution::seq_scan::SEQ_SCAN_TUPLES`, which counts what the heap actually
/// yielded. Flat here plus growing there is an O(table) merge behind a blind instrument.
pub static SCAN_TABLE_CALLS: AtomicU64 = AtomicU64::new(0);
pub static SCAN_TABLE_ROWS: AtomicU64 = AtomicU64::new(0);

/// Of the tuples `SEQ_SCAN_TUPLES` counts, the ones pulled by a tree THIS function built.
///
/// The attribution counter. A merge runs two loops that each issue one statement per changed row —
/// the point lookups here, and the publish path's `PendingWrite::into_stmt` UPDATEs — so a global
/// count of `delta` sequential scans does not say which loop produced them. This one does, because
/// it is incremented only from inside `scan_table_where`'s own drain. `SEQ_SCAN_TUPLES` minus this
/// is the work done by every OTHER scan in the window.
pub static SCAN_TABLE_SEQ_TUPLES: AtomicU64 = AtomicU64::new(0);

/// `(calls, rows_returned)` since process start. Read twice and subtract to scope it to a phase.
pub fn scan_table_counters() -> (u64, u64) {
    (
        SCAN_TABLE_CALLS.load(AtomicOrdering::Relaxed),
        SCAN_TABLE_ROWS.load(AtomicOrdering::Relaxed),
    )
}

/// Tuples sequentially scanned by trees `scan_table_where` itself built. See the static above.
pub fn scan_table_seq_tuples() -> u64 {
    SCAN_TABLE_SEQ_TUPLES.load(AtomicOrdering::Relaxed)
}

/// **The `ours` side of one cell's three-way comparison:** this branch's own recorded ops on that
/// cell, in the order it wrote them, for `compose_ops` to fold.
///
/// # ⛔ D114 — yes, this is a full scan per changed cell. MEASURED, and it is NOT the cost.
///
/// If you are here because you noticed that the caller computes `theirs` on the very next line via
/// `concurrent_op` — which **D86 indexed** down to a `partition_point` — and that this side was
/// left linear: that reading is correct, and it has already been measured. Do not index it.
///
/// The scan is exactly `delta x ops`, confirmed to a ratio of **1.000** by a counter. It is also
/// invisible: driving `examined` up **128.5x** moved merge latency 0.93x-1.20x, and a
/// configuration walking **32x more ops** merges FASTER than one walking fewer with a bigger
/// delta. **Merge cost tracks the DELTA, not this op log.** The curve, both directions, 15/15
/// merges applied, is `bench/d114_ours_side_scan.md` with raw runs in `bench/d114_before.txt`.
///
/// Two reasons indexing it would be a mistake rather than merely useless. First, `evaluate_merge`
/// already clones this whole Vec once per merge (`ops: ws.frame.ops.clone()`), so the merge is
/// O(ops) regardless and the scan only adds a factor of the delta — never a complexity class.
/// Second, it would be the THIRD wrong mechanism proposed for a slope on this exact code (D68,
/// D69-REOPEN, D114); the first two were also read correctly off the source and also wrong.
///
/// ⚠ If you are looking for a real quadratic on this path, it is in `stage_all`, which clones the
/// whole frame once **per statement** — measured at exponent 1.95, and 200x larger than this.
///
/// One function rather than the three identical iterator chains that were here before
/// (`evaluate_merge`, `merge_into`, `sibling_op`) — they differed only in which `Vec<Op>` they
/// read, and a defect in a scan repeated three times is a defect that gets fixed twice. The
/// counting is why it is worth a call: `examined` has to be taken from the iterator that actually
/// walks the ops, not computed as `ops.len()` from outside, or the instrument is an assertion
/// about the code rather than a reading of it.
fn ours_ops_on_cell(ops: &[Op], tbl: TableId, row: RowId, col: ColId) -> Vec<OpKind> {
    let mut examined = 0u64;
    let kinds: Vec<OpKind> = ops
        .iter()
        .filter(|o| {
            examined += 1;
            o.tbl == tbl && o.row == row && o.col == Some(col)
        })
        .map(|o| o.kind.clone())
        .collect();
    OURS_SCAN_EXAMINED.fetch_add(examined, AtomicOrdering::Relaxed);
    OURS_SCAN_MATCHED.fetch_add(kinds.len() as u64, AtomicOrdering::Relaxed);
    kinds
}

/// Everything the runtime needs to reach the shared tables, with the catalog EXCLUSIVELY.
///
/// A statement holding one of these is the only statement that may run, because the server hands
/// it the `&mut Catalog` it gets from `pgwire::ServerContext::catalog()` — one process-wide mutex,
/// taken outermost for the whole statement (`src/pgwire/mod.rs:100`). That is what D50 measured as
/// a flat throughput curve: 52,187 -> 46,897 statements/s over 1 -> 16 threads.
pub struct ExecCtx<'a> {
    pub catalog: &'a mut Catalog,
    pub bp: Arc<BufferPoolManager>,
    pub txn: Arc<TxnManager>,
}

/// The same thing with the catalog SHARED, for statements that only read it.
///
/// # Why this type exists rather than a bool or a convention
///
/// D51 asked the compiler whether a read needs a mutable catalog, by changing `ExecCtx.catalog`
/// to `&Catalog` and reading the errors: **two sites in the entire crate, both writes**
/// (`bench/d51_type_probe.txt`). So the serialization was never a property of what a read does —
/// it was one type signature propagated from two write-side call sites to every statement.
///
/// Splitting the type rather than relaxing it means a read path **cannot** mutate the catalog by
/// accident: there is no `&mut` to reach for, so the mistake is not expressible rather than
/// forbidden by a comment. `ExecCtx::read` reborrows, so the exclusive path can call every
/// read-only helper without duplicating one of them.
pub struct ReadCtx<'a> {
    pub catalog: &'a Catalog,
    pub bp: Arc<BufferPoolManager>,
    pub txn: Arc<TxnManager>,
}

impl<'a> ExecCtx<'a> {
    /// Reborrow as a read context. Free, and it is what lets the exclusive path reuse the shared
    /// helpers instead of growing a second copy of each.
    pub fn read(&self) -> ReadCtx<'_> {
        ReadCtx { catalog: self.catalog, bp: self.bp.clone(), txn: self.txn.clone() }
    }
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
    fnv64_update(0xcbf2_9ce4_8422_2325, bytes)
}

/// FNV-1a, resumable, so a fingerprint can be folded over many pieces without concatenating them.
fn fnv64_update(mut h: u64, bytes: &[u8]) -> u64 {
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
        Some(Value::BigInt(i)) => RowId(*i as u64),
        // A decimal's identity is its digits, not a float rendering of them: two amounts that
        // differ only past 2^53 must not collapse onto the same row.
        Some(Value::Decimal(d)) => RowId(fnv64(d.as_bytes())),
        Some(Value::Timestamp(ms)) => RowId(*ms as u64),
        Some(Value::Null) | None => RowId(0),
    }
}

/// **D57.** The one overlay key a predicate can touch, if it has a `pk = literal` conjunct.
///
/// Uses the planner's own `split_and` + `predicate_to_bounds` — the extraction `build_index_scan`
/// uses to pick the base access — so the overlay is probed by the same key the base scan probed.
///
/// The literal's variant must match the primary key column's declared type. `row_id_of` maps an
/// `Integer` to its value but a `Float` to a hash of its bits, so `id = 5.0` against an INTEGER
/// key would probe a key no staged row was ever stored under and MISS a staged version — the one
/// wrong answer. On any mismatch this returns `None` and the caller walks the table prefix, which
/// is always correct.
fn overlay_probe_key(bound: &BoundExpr, pk_type: Option<&DataType>) -> Option<u64> {
    let pk_type = pk_type?;
    let mut conjuncts = Vec::new();
    split_and(bound.clone(), &mut conjuncts);
    conjuncts.iter().find_map(|c| match predicate_to_bounds(c) {
        Some((0, std::ops::Bound::Included(lo), std::ops::Bound::Included(hi))) if lo == hi && literal_matches(&lo, pk_type) => {
            Some(row_id_of(std::slice::from_ref(&lo)).0)
        }
        _ => None,
    })
}

/// A literal may be probed only when `row_id_of` is exactly as fine as `=` for its variant --
/// equal values MUST land on one key, or the probe misses a staged row the walk would have found.
///
/// `Integer`/`BigInt`/`Timestamp` map to their value; `Varchar` and `Boolean` hash the same bytes
/// `Value::cmp` compares; `Float` hashes its bits and `cmp` is `total_cmp`, which is also
/// bit-exact. **`Decimal` is deliberately absent:** `row_id_of` hashes the digit TEXT (`"1.50"`)
/// while `decimal_cmp` is numeric (`Decimal("1.50") == Decimal("1.5")`), so a re-spelled literal
/// passes the variant check and probes a key no staged row was stored under. Found by a
/// fresh-context review after the probe first shipped; `hash_join.rs` canonicalises a decimal
/// before hashing for the same reason. A DECIMAL key walks the table prefix instead.
fn literal_matches(v: &Value, t: &DataType) -> bool {
    matches!(
        (v, t),
        (Value::Integer(_), DataType::Integer)
            | (Value::BigInt(_), DataType::BigInt)
            | (Value::Varchar(_), DataType::Varchar(_))
            | (Value::Timestamp(_), DataType::Timestamp)
            | (Value::Float(_), DataType::Float)
            | (Value::Boolean(_), DataType::Boolean)
    )
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
    /// The branch's root page at fork time.
    ///
    /// `set_root` moves the branch's live root on every copy-on-write write, so the fork point is
    /// gone the moment the branch writes anything. It has to be captured here or the changeset
    /// has nothing to diff against.
    fork_root: PageId,
    rows: PersistentMap<(u32, u64), RowState>,
    /// **D57.** How many `Present` rows were staged in a state the read path's probe cannot see
    /// through. The probe -- "only the overlay entry at `row_id_of(k)` can affect `pk = k`" -- is
    /// sound only while every staged row (a) sits under the key its own column 0 derives, and
    /// (b) holds column 0 in the column's DECLARED variant, so that `=` and `row_id_of` agree.
    /// Both can be false on a branch, and neither is refused there:
    ///
    /// * an UPDATE may assign the primary key (trunk refuses it in `execution::update`;
    ///   `integration_escrow` relies on the staged row KEEPING its original key so a PK move
    ///   cannot leave an escrow pool), breaking (a);
    /// * an INSERT/UPDATE stages a literal as the variant it was written in -- `21.0` into an
    ///   INTEGER column stays `Float`, keyed by the hash of its bits -- because a page-backed
    ///   branch does not run the tuple encoder's width check (`agent_sql_surface` documents this),
    ///   breaking (b).
    ///
    /// Monotone and conservative: counted at the single site that writes `rows`, inherited by a
    /// forked child (which shares the entries), never decremented; `visible_rows_where` walks
    /// instead of probing whenever it is non-zero. A guard on the resulting state, not on the
    /// statement that caused it -- all three of these were found by a fresh-context review after
    /// the probe first shipped, and the first fix (a counter for (a) alone) missed (b).
    unprobeable_rows: u64,
    /// Image at first touch = the fork-point value. `None` means the row did not exist.
    base_rows: PersistentMap<(u32, u64), Option<Vec<Value>>>,
    /// Ancestor txns whose STAGED writes this workspace copied at fork time, oldest first.
    ///
    /// A fork takes a snapshot of the parent's `rows`/`schema_edits` rather than a link, so a
    /// child's `MERGE` publishes writes its ANCESTORS staged. Those ancestors' captures are the
    /// read premises for rows that are now in the shared tables, and F6's discriminator
    /// (`published`, meaning "did *this* branch's own seal come from a merge") cannot see that:
    /// the ancestor's own seal is an ABANDON or a reap. Without this list, dropping its capture
    /// silently deletes the premise of a published row -- measured as
    /// `REVERT MERGE m1` reporting `blocked_by = []` and then removing the row the published row
    /// was derived from.
    inherited: Vec<TxnId>,
    tables: PersistentMap<u32, String>,
    frame: TxnFrame,
    // B11's fields, WITHOUT its `reads`: B4 deleted `Workspace::reads` as a second copy of a
    // read-set the runtime already keeps in `State::captures`, and re-adding it here would restore
    // the duplication B4 removed. The one consumer below reads it from `captures` instead.
    /// Column-level schema changes this branch has made but not published — B11.
    ///
    /// Pending rather than applied, because a schema is the most visible write there is: applying
    /// one immediately would make every other connection see the column the moment one agent
    /// typed the statement, and abandoning the branch would not take it away. Published at
    /// `MERGE`, after [`crate::tel::schema_merge::merge_schema`] has decided they compose with
    /// whatever the target's shape has become.
    schema_edits: Arc<Vec<(String, SchemaEdit)>>,
    /// Each touched table's shape **at first touch** — the fork point, in exactly the sense
    /// `base_rows` is the fork-point row image.
    ///
    /// Needed by two things at merge: the three-way schema merge, and conforming this branch's
    /// rows to a target whose shape a sibling agent has widened since. Without it a branch that
    /// forked before a concurrent `ADD COLUMN` publishes rows one value short and the failure
    /// surfaces from inside `Tuple::serialize`.
    base_shapes: PersistentMap<String, Schema>,
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

/// One row's worth of a staged change, so a whole statement can be checked before any of it lands.
///
/// Exists so `stage_all` can take a statement rather than a row: the refusal has to be decided for every
/// row before any row is recorded, and that is not expressible while the arguments are loose parameters.
struct Staged {
    row: RowId,
    before: Option<Vec<Value>>,
    after: RowState,
    ops: Vec<Op>,
    guard: Option<Guard>,
}

/// One published op, as an agent choosing a cherry-pick sees it.
///
/// `seq` is what [`AgentRuntime::cherry_pick`] takes, and it names ONE write: a txn may write a
/// cell more than once, and the point of the operation is to be able to pick one of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PickableOp {
    pub seq: u64,
    pub txn: TxnId,
    /// The branch that published it, recovered through the merge record keyed by txn.
    pub branch: BranchId,
    pub table: String,
    pub row: RowId,
    /// `None` for whole-row ops.
    pub col: Option<ColId>,
}

/// The runtime's applied-op log, **projected to one cherry-pick**.
///
/// # Why a projection and not a borrow of `State`
///
/// [`CherryLog::ops_on_cell`] returns `&[u64]` — sequence numbers — while D86's index holds
/// **positions** into `State::applied`. The two are different numbers over the same order (seqs
/// are handed out monotonically from `apply_seq` and `applied` is append-only, so position *i*
/// carries `applied[i].seq`), so the seq list has to exist somewhere as a slice.
///
/// It is built **only for the cells the selection touches**, by reading D86's index for each of
/// them — not by scanning the log, and not by building a second index over it, which is what
/// `ops_on_cell`'s contract rules out. The cost is O(picked + ops on the picked cells), which is
/// the same bound `concurrent_op` pays per cell on the merge path.
struct RuntimeCherryLog {
    /// Every op the pick can reach: the selected ones, plus every op on the cells they name.
    ops: BTreeMap<u64, RecordedOp>,
    /// Seqs per touched cell, increasing — the order `plan_cherry_pick`'s `partition_point`
    /// reasoning depends on.
    cells: BTreeMap<(u32, u64, u32), Vec<u64>>,
    empty: Vec<u64>,
}

impl RuntimeCherryLog {
    fn project(state: &State, picked: &[u64]) -> RuntimeCherryLog {
        // Which branch published each txn. `AppliedOp` does not carry a branch; the runtime
        // recovers it through `MergeRecord { branch, txns }`, which is the join `cherry.rs` says
        // an impl should do once rather than making every caller repeat it.
        let mut branch_of: BTreeMap<u64, BranchId> = BTreeMap::new();
        for rec in state.merges.values() {
            for t in &rec.txns {
                branch_of.insert(t.0, rec.branch);
            }
        }
        // Position of each seq, so a cell's position list becomes a seq list.
        let mut at_seq: BTreeMap<u64, usize> = BTreeMap::new();
        for (i, op) in state.applied.iter().enumerate() {
            at_seq.insert(op.seq, i);
        }

        let recorded = |op: &AppliedOp| RecordedOp {
            seq: op.seq,
            txn: op.txn,
            branch: branch_of.get(&op.txn.0).copied().unwrap_or(BranchId::TRUNK),
            table: op.table.clone(),
            tbl: op.tbl,
            row: op.row,
            col: op.col,
            kind: op.kind.clone(),
            before: op.before.clone(),
            before_row: op.before_row.clone(),
        };

        let mut ops: BTreeMap<u64, RecordedOp> = BTreeMap::new();
        let mut cells: BTreeMap<(u32, u64, u32), Vec<u64>> = BTreeMap::new();
        for seq in picked {
            let Some(&pos) = at_seq.get(seq) else { continue };
            let Some(op) = state.applied.get(pos) else { continue };
            ops.insert(op.seq, recorded(op));
            let Some(col) = op.col else { continue };
            let key = (op.tbl.0, op.row.0, col.0);
            if cells.contains_key(&key) {
                continue;
            }
            // D86's index, read exactly as `concurrent_op` reads it.
            let mut seqs = Vec::new();
            for &at in state.applied_at_cell(op.tbl, op.row, col) {
                if let Some(o) = state.applied.get(at as usize) {
                    seqs.push(o.seq);
                    ops.entry(o.seq).or_insert_with(|| recorded(o));
                }
            }
            cells.insert(key, seqs);
        }
        RuntimeCherryLog { ops, cells, empty: Vec::new() }
    }
}

impl CherryLog for RuntimeCherryLog {
    fn op_at(&self, seq: u64) -> Option<&RecordedOp> {
        self.ops.get(&seq)
    }

    fn ops_on_cell(&self, tbl: TableId, row: RowId, col: ColId) -> &[u64] {
        self.cells.get(&(tbl.0, row.0, col.0)).map(|v| v.as_slice()).unwrap_or(&self.empty)
    }
}

/// A live agent branch as the thing a pick lands on.
///
/// **`commit_all` is one [`AgentRuntime::stage_all`] call**, which is the all-or-nothing door
/// `cherry.rs` says the runtime owes: it decides every refusal — the capability envelope, then the
/// escrow check over the whole batch — before it applies anything. That is why
/// [`AgentRuntime::cherry_pick`] refuses a selection spanning more than one table: the guarantee
/// is per-table, so across tables there would be nothing to hold it.
struct BranchCherryTarget<'a> {
    runtime: &'a AgentRuntime,
    branch: BranchId,
    /// The target's image of every row the plan can touch, resolved before planning began.
    images: BTreeMap<(u32, u64), Vec<Value>>,
    pk_types: BTreeMap<u32, (String, DataType)>,
    committed: bool,
}

impl CherryTarget for BranchCherryTarget<'_> {
    fn row_image(&self, tbl: TableId, row: RowId) -> Option<Vec<Value>> {
        self.images.get(&(tbl.0, row.0)).cloned()
    }

    fn commit_all(&mut self, writes: &[CherryWrite]) -> Result<(), FerroError> {
        // One pick, one commit. `cherry_pick` reaches this once and only after every row has been
        // decided; a second call would mean the engine had split a plan, which is the thing the
        // single-door design exists to prevent.
        if self.committed {
            return Err(FerroError::Internal(
                "a cherry-pick committed twice: the all-or-nothing contract has been broken \
                 upstream of the target"
                    .into(),
            ));
        }
        if writes.is_empty() {
            return Ok(());
        }

        // Fold the writes into one staged item per row. Several `Cell` writes can land on one
        // row, and staging them separately would append the row to the frame more than once.
        let mut staged: BTreeMap<(u32, u64), Staged> = BTreeMap::new();
        let mut tbl_id: Option<TableId> = None;
        for w in writes {
            tbl_id = Some(w.tbl());
            let key = (w.tbl().0, w.row().0);
            match w {
                CherryWrite::Cell { tbl, row, col, value, before, .. } => {
                    let entry = staged.entry(key).or_insert_with(|| Staged {
                        row: *row,
                        before: self.images.get(&key).cloned(),
                        after: RowState::Present(
                            self.images.get(&key).cloned().unwrap_or_default(),
                        ),
                        ops: Vec::new(),
                        guard: None,
                    });
                    if let RowState::Present(img) = &mut entry.after {
                        let idx = col.0 as usize;
                        if idx >= img.len() {
                            return Err(FerroError::Internal(format!(
                                "cherry-pick would write column {idx} of a {}-column image on \
                                 row {row}",
                                img.len()
                            )));
                        }
                        img[idx] = value.clone();
                    }
                    entry.ops.push(Op::new(
                        *tbl,
                        *row,
                        Some(*col),
                        OpKind::Assign(value.clone()),
                    ));
                    let _ = before;
                }
                CherryWrite::InsertRow { tbl, row, image, replaced, .. } => {
                    staged.insert(
                        key,
                        Staged {
                            row: *row,
                            before: replaced.clone(),
                            after: RowState::Present(image.clone()),
                            ops: vec![Op::new(
                                *tbl,
                                *row,
                                None,
                                OpKind::RowCreate(image.clone()),
                            )],
                            guard: None,
                        },
                    );
                }
                CherryWrite::DeleteRow { tbl, row, before_row, .. } => {
                    staged.insert(
                        key,
                        Staged {
                            row: *row,
                            before: Some(before_row.clone()),
                            after: RowState::Deleted,
                            ops: vec![Op::new(*tbl, *row, None, OpKind::RowDelete)],
                            guard: None,
                        },
                    );
                }
            }
        }

        let tbl = tbl_id.expect("a non-empty write list names a table");
        let (name, pk_type) = self.pk_types.get(&tbl.0).cloned().ok_or_else(|| {
            FerroError::Internal(format!("no schema resolved for table id {}", tbl.0))
        })?;
        let items: Vec<Staged> = staged.into_values().collect();
        self.runtime.stage_all(self.branch, tbl, &name, &pk_type, items)?;
        self.committed = true;
        Ok(())
    }
}

/// One side of a sibling merge, lifted out of its `Workspace` so the state lock is not held
/// across the composition.
///
/// Every field is a `PersistentMap` clone or a `Vec` clone of what the workspace already holds —
/// D27 made the maps structurally shared, so a clone is an `Arc` bump rather than a copy of the
/// branch's whole staged set.
struct SiblingSide {
    rows: PersistentMap<(u32, u64), RowState>,
    base_rows: PersistentMap<(u32, u64), Option<Vec<Value>>>,
    tables: PersistentMap<u32, String>,
    ops: Vec<Op>,
    guards: Vec<Guard>,
    base_shapes: PersistentMap<String, Schema>,
}

/// The fork point of two branches, and what finding it cost.
///
/// **`hops` and `walk_hops` are the instrument the complexity claim is stated in**, and they are
/// integers for the reason `VersionGraph::is_ancestor_hops` gives: a hop count does not move when
/// the machine is loaded, and a wall clock on this box measures the build fleet as much as the
/// algorithm. `walk_hops` is the comparable number for the parent-pointer walk to the same
/// answer — one dereference per level on each side — so the two are directly commensurable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkPoint {
    /// The lowest common ancestor. Never fabricated: two branches in different trees are an
    /// error, not an assumed trunk.
    pub branch: BranchId,
    /// The fork point's ancestry depth.
    pub depth: u32,
    /// Jump-pointer dereferences the O(log depth) query performed.
    pub hops: u64,
    /// Dereferences a parent-pointer walk would have performed for the same answer.
    pub walk_hops: u64,
}

/// What a sibling merge decided, and where its fork point came from.
#[derive(Debug, Clone)]
pub struct SiblingMergeReport {
    pub merge_id: String,
    pub from: BranchId,
    pub into: BranchId,
    /// The **computed** fork point the three-way merge was scored against.
    pub fork: ForkPoint,
    pub outcome: MergeOutcome,
    pub rows: Vec<RowMergeOutcome>,
    /// Whether the composed rows were staged onto `into`. False for a conflict, which stages
    /// nothing and leaves both branches alive.
    pub applied: bool,
}

/// Carry an ancestry-index fault out as a branch error.
///
/// Kept distinct in wording: an [`AncestryError`] is a fault of the index, not of the branch, and
/// a caller that sees one is looking at a bookkeeping bug rather than a reaped handle.
fn ancestry_error(e: AncestryError) -> FerroError {
    FerroError::Branch(format!("ancestry index: {e}"))
}

/// What one page-derived changeset ([`AgentRuntime::page_changeset_with_cost`]) cost, in integers.
///
/// ⚠ D193: this used to read "what one page-derived `DIFF` cost". A `DIFF <branch>` statement
/// never produces one of these: it runs [`AgentRuntime::diff`], which reads the workspace's
/// touched-rows map and descends no page tree, and `page_changeset_with_cost` has no caller in
/// `src/` outside `page_changeset`, which has none either. Numbers in this type are the cost of the page-derived changeset only.
///
/// **Integers and not a duration, deliberately.** This box runs a build fleet and a 46x
/// quiet-vs-loaded spread has been measured on it, so a wall clock here would report the load
/// rather than the algorithm. A node count does not move when the machine is busy, which is the
/// same reasoning `version_graph::is_ancestor_hops` gives for counting jump-pointer dereferences.
///
/// `visited` is every node this diff READ — there is no second, uncounted enumeration behind it.
/// That is precisely what `cow::btree::TreeDiff::pages_examined` could not say on its own, which
/// is why `page_changeset_with_cost` reports a `DiffCost` rather than a `TreeDiff`. See
/// [`AgentRuntime::page_changeset_with_cost`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DiffCost {
    /// Nodes whose payload was decoded by the synchronised descent.
    pub visited: usize,
    /// Subtree pairs found equal by page identity and abandoned without either page being read.
    pub skipped_subtrees: usize,
}


#[derive(Default)]
struct State {
    /// **Keyed by the whole [`BranchId`], not by the id slot — D158 item 1.**
    ///
    /// This was `BTreeMap<u64, Workspace>` and eleven call sites asked it `get(&branch.id)`. The
    /// catalog recycles a reaped branch's id SLOT (`reaper.rs` `release_id`, `catalog.rs` `fork`
    /// pops it) and bumps the generation, so a caller holding a stale `BranchId` — and a
    /// connection holds one for the life of its `AgentSession`, which nothing re-reads from the
    /// catalog — was answered about the slot's NEW occupant: **another agent's workspace**.
    ///
    /// Measured over the real statement path in
    /// `tests/w4_stale_branch_crosses_agents.rs`, against this file one commit earlier: a
    /// reaped session's `SELECT` returned `qty = 999`, the live agent's staged and unmerged row,
    /// and its `ABANDON;` unbound the live agent's name. **There is no automatic keepalive**: the
    /// only production `renew_lease` is `simulate.rs:431`, which sets the lease on a candidate
    /// branch `simulate` forked two lines above it, so it cannot extend a session that is idle.
    /// An agent that pauses longer than [`DEFAULT_LEASE_MILLIS`] therefore loses its branch with
    /// its connection still open, and the interleaving needs no race to win.
    ///
    /// ⚠ This paragraph first said `renew_lease` "has no caller in `src` outside the catalogs'
    /// own tests", which is false — `simulate.rs:431` is production. Corrected before landing;
    /// the conclusion is unchanged because the hazard is the absent keepalive, not the absent
    /// symbol. `bench/w4/DECISION.md` addendum 6 carries the full reversal.
    ///
    /// `forget_one_branch` had already taken this argument and validated the whole `BranchId`
    /// against `names`; only that one write path took it. Keying here retires the argument
    /// instead of repeating it: a stale generation is a different key, so it MISSES, and a site
    /// added later cannot forget a check it does not have to write. That is the one reason to
    /// prefer this over a generation field on `Workspace` plus eleven comparisons.
    ///
    /// **Invariant, maintained by [`State::insert_workspace`] and relied on below: at most ONE
    /// workspace per id slot.** A fork into a recycled slot evicts any entry still sitting on it,
    /// exactly as `BTreeMap::insert` used to by colliding. Two consequences that are load-bearing
    /// rather than incidental: `live_runs` can still report a bare slot without ambiguity, and
    /// `forget_one_branch` can treat "the key is still here" as proof that the slot was not
    /// recycled, so it needs no second authority to decide whether the name is still its own.
    ///
    /// Ordering is by `(id, generation)` — `BranchId` derives `Ord` in field order — so a slot's
    /// entries are contiguous and the map still ranges in id-slot order for the chunked sweeps.
    workspaces: BTreeMap<BranchId, Workspace>,
    names: BTreeMap<String, BranchId>,
    next_txn: u64,
    next_merge: u64,
    apply_seq: u64,
    applied: Vec<AppliedOp>,
    /// **D86.** `(tbl, row, col)` -> positions in `applied`, so a merge stops rescanning the whole
    /// log for every cell it changes.
    ///
    /// `concurrent_op` asks a KEYED question — `a.tbl == tbl && a.row == row && a.col == Some(col)`
    /// — and answered it with a linear scan of a Vec that is never pruned. Measured
    /// (`bench/d86_merge_degrades_with_merge_count.txt`): across 400 merges a merge got **1.60x
    /// slower** while the UPDATEs in the same cycles got 18% FASTER, which is a within-run
    /// comparison a shared box cannot fake. Merge k cost O(k x delta); merging N branches was
    /// O(N^2).
    ///
    /// ⚠ The Vec STAYS. Two other readers need it and neither is served by this key: the `txn`
    /// filter in `REVERT` (`:4159`) and `highest_applied_seq` (`:3538`). This is an index beside
    /// the log, not a replacement for it — which also means the two must be pushed together, and
    /// `push_applied` is the only place that does either.
    applied_by_cell: std::collections::HashMap<(u32, u64, u32), Vec<u32>>,
    merges: BTreeMap<String, MergeRecord>,
    /// Why each quarantined branch is being held, keyed by branch id SLOT.
    ///
    /// ⚠ **Still keyed by the slot after D158 item 1 moved `workspaces` off it, and that is a
    /// decision rather than an oversight.** Every path that writes or reads it — `quarantine`,
    /// `release_from_quarantine`, and `merge`'s refusal message — passes `self.branches.get()`
    /// first, which refuses a stale generation, so a recycled slot is not reachable through them.
    /// What IS reachable: `seal` does not clear this map, so a reaped quarantined branch leaves
    /// its reason behind and a later fork into that slot would read the DEAD branch's string.
    /// That is a wrong sentence in a diagnostic, not a cross-agent answer about data — which is
    /// why it is recorded here instead of being bundled into a correctness fix that has a failing
    /// test behind it. `forget_one_branch` does clear it, and may, because a surviving workspace
    /// key proves the slot was not recycled (see [`State::workspaces`]).
    quarantine_reasons: BTreeMap<u64, String>,
    /// Reservations over bounded cells, so an overdraw fails when it is written.
    escrow: EscrowLedger,
    versions: BTreeMap<(u32, u64), VersionRef>,
    /// What each agent task retained: the reads its access shapes demanded — every scan carrying
    /// the snapshot it read at — and every version it published, with the values it published.
    ///
    /// **The runtime's only retention point, and that is the fix.** This used to be a bare
    /// `DependencyGraphBuilder` here plus a second copy of the read-sets on each `Workspace`, fed
    /// with exact versions only. `record_predicate_read` and `record_write_value` — the two calls
    /// that turn a SCAN into a causal edge — were reachable from `provenance::capture` and from
    /// nowhere else, and the runtime never referenced that module. So `REVERT ... CASCADE`
    /// under-reported precisely where agents read: the query surface has no `LIMIT` and no
    /// `ORDER BY`, which makes an agent's natural read a full scan, and a full scan retained a
    /// region that nothing downstream ever looked at.
    ///
    /// Keyed by txn, and never dropped by `seal`: the dependency graph has to outlive the workspace
    /// for the same reason `row_author` does — a merge retires the branch at the moment its rows
    /// become visible to everyone else, which is the moment they can start being read.
    captures: BTreeMap<u64, TxnCapture>,
    /// Txns whose staged writes reached the shared tables -- by their own `MERGE`, or by a
    /// DESCENDANT's merge publishing writes the descendant inherited at fork time.
    ///
    /// A capture may only be dropped for a task that published nothing (F6). "Published" is a
    /// property of the WRITES, not of which branch happened to call `MERGE`, so it is recorded
    /// here when the publish happens rather than re-derived at seal time from a flag that cannot
    /// answer for an ancestor.
    published_txns: BTreeSet<u64>,
    /// How many LIVE workspaces still reference each txn — as their own `txn`, or in their
    /// `inherited` list. A reverse index over exactly the question `capture_is_protected` asks.
    ///
    /// **Why an index and not the scan it replaces.** That predicate ran
    /// `workspaces.values().any(..)` while holding the one Mutex every statement takes, once per
    /// branch being forgotten — O(forgotten × open sessions) under the per-statement lock. With
    /// 10⁵ open sessions and 64 branches reaped, an unrelated statement waited **134 ms at the
    /// median** for the sweep to finish (`bench/w4/statement-lock-BEFORE.txt`, table 2).
    /// A count keyed by txn answers the same question in O(log n) and is maintained at the two
    /// places a workspace enters and leaves the map.
    ///
    /// Maintained ONLY by [`State::insert_workspace`] and [`State::remove_workspace`], which are
    /// the only doors into `workspaces` for exactly that reason: an insert or a remove that went
    /// straight to the map would leave this under- or over-counted, and an under-count makes
    /// `capture_is_protected` answer "nothing needs this" about a capture a live task is standing
    /// on — which is the F6 data loss, reached by a new door. [`State::audit_txn_refs`] re-derives
    /// the whole index by brute force at both doors in debug builds, so every fork and every seal
    /// in the test suite is a differential test of this index against the scan it replaced.
    txn_refs: BTreeMap<u64, u32>,
    policy: PolicyTable,
}

impl State {
    /// **D86.** The one place that appends to `applied`, so the log and its index cannot drift.
    ///
    /// A second source of truth rots when someone adds a write and does not know about the index.
    /// Making the append the only operation removes the ordering to get wrong — the same reason
    /// `CatalogState::install` exists for the live-branch counter.
    fn push_applied(&mut self, op: AppliedOp) {
        if let Some(col) = op.col {
            let at = self.applied.len() as u32;
            self.applied_by_cell
                .entry((op.tbl.0, op.row.0, col.0))
                .or_default()
                .push(at);
        }
        self.applied.push(op);
    }

    /// Positions in `applied` for one cell, newest last. Empty when the cell has never been
    /// published, which is the common case and is what makes this worth indexing.
    fn applied_at_cell(&self, tbl: TableId, row: RowId, col: ColId) -> &[u32] {
        self.applied_by_cell
            .get(&(tbl.0, row.0, col.0))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Insert a workspace, taking the txn references it holds. One of the two doors into
    /// `workspaces`; see [`State::txn_refs`] for why there are only two.
    fn insert_workspace(&mut self, branch: BranchId, ws: Workspace) {
        for t in txn_refs_of(&ws) {
            *self.txn_refs.entry(t).or_insert(0) += 1;
        }
        // An id slot is recycled after a reap, so an insert CAN land on an occupied slot, and the
        // workspace sitting there has to be released. That workspace's branch is dead by
        // construction — nothing else could be occupying its slot — so everything the reap path
        // releases has to be released here too. Dropping only the `txn_refs` and letting the
        // capture go was exactly that leak: one permanently unreferenced `captures` entry per
        // recycled slot, because `forget_captures_unless_published` is the only site that calls
        // `captures.remove` and this path did not reach it. That is the unbounded growth
        // `forget_reaped_branches` exists to prevent, arriving through a second door.
        //
        // **The eviction is now explicit, and it has to be.** While the map was keyed by the slot
        // this was `BTreeMap::insert`'s return value: a recycled slot collided and handed the old
        // workspace back. Keyed by the whole `BranchId` (D158 item 1) a recycled slot arrives at a
        // NEW generation and therefore a DIFFERENT key, so it collides with nothing and the dead
        // workspace would sit there for ever — the same leak, re-opened by the fix to a different
        // defect. Scanning the slot's own contiguous range is what replaces the collision, and it
        // is what maintains the one-workspace-per-slot invariant stated on `State::workspaces`
        // that `live_runs` and `forget_one_branch` both rest on.
        // **Order: take the displaced ones out, put the new one IN, and only then release them.**
        //
        // Not cosmetic. `forget_captures_unless_published` reaches `capture_is_protected`, whose
        // `debug_assert` compares `txn_refs` against a live scan of `workspaces` — and the new
        // workspace's references were counted into `txn_refs` at the top of this function. Release
        // before inserting and any txn held only by the incoming workspace reads as indexed but
        // unscannable, which fires that assertion for a state that is in fact consistent. The
        // slot-keyed map got this ordering free, because `BTreeMap::insert` put the new value in
        // and handed the old one back in the same call; keying by `BranchId` split that into two
        // steps, so the order has to be written down rather than inherited.
        let displaced: Vec<BranchId> = self
            .workspaces
            .range(BranchId::new(branch.id, 0)..=BranchId::new(branch.id, u32::MAX))
            .map(|(b, _)| *b)
            .collect();
        let olds: Vec<Workspace> =
            displaced.into_iter().filter_map(|dead| self.workspaces.remove(&dead)).collect();
        self.workspaces.insert(branch, ws);
        for old in olds {
            self.drop_txn_refs(&old);
            forget_captures_unless_published(self, &old);
        }
        self.audit_txn_refs();
    }

    /// Re-derive `txn_refs` by brute force and compare. Debug builds only.
    ///
    /// **This is what makes "the suite checks the index" true rather than nearly true.** The
    /// `debug_assert` inside `capture_is_protected` was carrying that claim alone, and it is
    /// reached from exactly one non-test caller — `forget_captures_unless_published`, which runs on
    /// the abandon and reap arms and nowhere else. A fork or a published merge never reaches it. So
    /// a desync introduced when a workspace was INSERTED stayed invisible until some later abandon
    /// happened to ask about that particular txn, and a suite full of forks and merges proved
    /// nothing about it.
    ///
    /// That matters beyond this file: the one hard constraint on anything that rewrites the fork
    /// path is that `workspaces` has exactly two doors, because they maintain this index, and a
    /// rewrite going straight to `BTreeMap::insert` desyncs it until `capture_is_protected` starts
    /// answering "nothing needs this" about a capture a live task is standing on — the F6 data
    /// loss. Auditing at both doors is what turns that constraint from advice into a failing test.
    #[cfg(debug_assertions)]
    fn audit_txn_refs(&self) {
        // **D98 — bounded, and the bound gives up nothing against the bug this catches.**
        //
        // The full re-derive is O(open sessions) and runs at both doors, so a debug fixture that
        // opens N sessions is O(N²) and a debug build stops being a way to reproduce anything at
        // scale. Measured on this tree with `examples/outer_runtime_lock.rs`, building the same
        // fixture in both profiles (machine under fleet load, so read the trend, not the cells):
        //
        // | N      | debug   | release | per branch, debug MINUS release |
        // |--------|---------|---------|---------------------------------|
        // |  4 000 |  20.2 s |  14.7 s | 1 373 µs                        |
        // |  8 000 |  46.4 s |  25.1 s | 2 660 µs                        |
        // | 16 000 | 107.8 s |  53.5 s | 3 396 µs                        |
        //
        // Release's per-branch cost is flat (3 685 → 3 143 → 3 344 µs); debug's excess over it
        // RISES with N, which is this audit appearing. At 16 000 a debug build already costs 2×,
        // and the factor keeps growing — at the 10⁶ branches `SCALE-DESIGN.md` targets it is the
        // difference between an hour and days. That is a reproduction trap, not merely slow: the
        // person it stops is the next one trying to reproduce a scale row in a debuggable build.
        //
        // **So it is capped — and the cap is not a weakening.** The bug this exists to catch is
        // structural: a rewrite that reaches `workspaces` without going through
        // `insert_workspace`/`remove_workspace` desyncs the index, and it does so on its FIRST
        // crossing. To have more than `AUDIT_FULL_MAX` workspaces you must have crossed a door
        // that many times, starting from none — so every such bypass is exercised, and fully
        // audited, long before the map outgrows the cap. Sampling or a cadence WOULD have been a
        // weakening; a prefix of the crossings is not.
        //
        // ⚠ The blind spot, stated here rather than discovered later: a door that is correct for
        // small maps and wrong only above `AUDIT_FULL_MAX` is not caught. Nothing in this file
        // branches on the map's size, so no such door exists today, and a change that added one
        // would have to raise this cap with it.
        if self.workspaces.len() > AUDIT_FULL_MAX {
            audit_downgraded_once(self.workspaces.len());
            return;
        }
        let mut want: BTreeMap<u64, u32> = BTreeMap::new();
        for ws in self.workspaces.values() {
            for t in txn_refs_of(ws) {
                *want.entry(t).or_insert(0) += 1;
            }
        }
        assert_eq!(self.txn_refs, want, "txn_refs disagrees with a scan of workspaces");
    }

    /// Release builds pay nothing, which is the only reason this can sit on two hot doors.
    ///
    /// The note that used to be here said a future debug test forking tens of thousands of
    /// sessions "would feel it, and should use a release build or a fixture helper rather than
    /// weakening this". D98 measured how much it would feel it and took the third option instead:
    /// see [`AUDIT_FULL_MAX`] and the comment on the debug arm for why capping the map size gives
    /// up nothing against the bug the audit exists to catch.
    #[cfg(not(debug_assertions))]
    fn audit_txn_refs(&self) {}

    /// Remove a workspace, releasing the txn references it held.
    ///
    /// The references are released **before** the caller inspects the result, which is what makes
    /// `capture_is_protected` a question about the workspaces that REMAIN — the same thing the
    /// scan meant when it ran after `BTreeMap::remove` had already taken this one out.
    fn remove_workspace(&mut self, branch: &BranchId) -> Option<Workspace> {
        let ws = self.workspaces.remove(branch)?;
        self.drop_txn_refs(&ws);
        self.audit_txn_refs();
        Some(ws)
    }

    fn drop_txn_refs(&mut self, ws: &Workspace) {
        for t in txn_refs_of(ws) {
            match self.txn_refs.get_mut(&t) {
                Some(n) if *n > 1 => *n -= 1,
                Some(_) => {
                    self.txn_refs.remove(&t);
                }
                // Loud in debug, and a no-op rather than a wrapping subtraction in release: an
                // over-count strands a capture (a leak), an under-count deletes a live premise.
                // Of the two, refusing to go below zero is the direction that loses no data.
                None => debug_assert!(false, "txn_refs underflow for txn {t}"),
            }
        }
    }
}

/// Every txn one workspace keeps alive: the ancestors' staged writes it copied at fork time, and
/// its own. Exactly the disjunction `capture_is_protected` used to scan for.
fn txn_refs_of(ws: &Workspace) -> impl Iterator<Item = u64> + '_ {
    ws.inherited.iter().map(|t| t.0).chain(std::iter::once(ws.txn.0))
}

/// What one live agent task has written and read, as counts. See [`AgentRuntime::run_activity`]
/// for what each field does and does not include.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunActivity {
    pub branch: BranchId,
    /// The name this branch answers to in SQL (`b_3`).
    pub branch_name: String,
    /// `None` only if the interned run went missing, which would be a bug rather than a state.
    pub run: Option<RunEntity>,
    /// Typed ops in this task's own frame. Not inherited at fork.
    pub ops_captured: u64,
    /// Guards in this task's own frame. Not inherited at fork.
    pub guards_captured: u64,
    /// Rows in the workspace map. **Inherited at fork** from an open parent session.
    pub staged_rows: u64,
    /// Distinct `(table, row)` pairs read as exact versions.
    pub rows_read_exact: u64,
    /// Range / full-scan reads, which retain a predicate summary rather than versions.
    pub scan_reads: u64,
    /// Rows those scans reported observing. Diagnostic; never feeds a decision.
    pub scan_rows_observed: u64,
    /// Rows staged without ever being read — DESIGN.md section 4's cheap metric.
    pub blind_writes: u64,
}

/// How many workspaces one acquisition of the state lock will examine in
/// [`AgentRuntime::forget_reaped_branches`].
///
/// This is the knob that bounds that sweep's blocking cost. The walk is the same length either
/// way — the sweep is still O(open sessions) of work in total — but it is spread over
/// ⌈sessions / chunk⌉ separate acquisitions, so what any ONE statement waits for is a chunk
/// rather than the whole map.
///
/// **1024 chosen on the TAIL, not the median, and the cost is not monotone in either direction.**
/// A smaller chunk holds the lock for less time per acquisition but takes it far more often, and
/// each acquisition is another chance to be descheduled while still holding it — so shrinking the
/// chunk makes the worst case WORSE, which is the opposite of the obvious guess. Measured at 10⁵
/// open sessions, round-robin across chunk sizes over 9 rounds, worst stall observed for an
/// unrelated statement (`bench/w4/forget-chunk-selection.txt`):
///
/// ```text
///   chunk     median        worst
///      64    757 us     17644 us
///     256    280 us      2908 us
///    1024    462 us       574 us
///    4096   1080 us      1281 us
/// ```
///
/// 256 wins the median and loses the tail by 5x; 1024 stayed inside 419-574 us on every single
/// round. A latency bound is a claim about the worst case, so the tail is what decides it.
const FORGET_CHUNK: usize = 1024;

/// Largest `workspaces` map [`State::audit_txn_refs`] re-derives in full, in debug builds.
///
/// **D98.** Above this the audit is skipped and says so once. The argument for why that is a bound
/// and not a weakening is on the audit itself; the short form is that reaching a map of size M
/// requires M crossings of a door starting from zero, so every door is fully audited while the map
/// is small, and a bypass cannot hide above a threshold it had to walk past.
///
/// 1024 rather than the ~200 the old note claimed the suite needed, because that figure was
/// asserted rather than measured. It is checked the way that claim should have been: the skip
/// prints a distinctive line, and `cargo test --lib` is run and grepped for it. A suite that does
/// exceed this is not broken — it has downgraded one detector, and the line is there to say so.
#[cfg(debug_assertions)]
const AUDIT_FULL_MAX: usize = 1024;

/// Say once, per process, that the debug index audit has stopped re-deriving in full.
///
/// Once, because a hot door would otherwise turn a notice into the cost it is announcing. On
/// stderr with a tolerated `EPIPE`, for the reason `branch::lease_thread::report` spells out: this
/// can run on a thread whose panic nobody would see.
#[cfg(debug_assertions)]
fn audit_downgraded_once(len: usize) {
    use std::io::Write;
    use std::sync::atomic::{AtomicBool, Ordering};
    static SAID: AtomicBool = AtomicBool::new(false);
    if SAID.swap(true, Ordering::Relaxed) {
        return;
    }
    let _ = writeln!(
        std::io::stderr(),
        "ferrodb: AUDIT_TXN_REFS_DOWNGRADED — {len} open workspaces exceeds AUDIT_FULL_MAX \
         ({AUDIT_FULL_MAX}), so the debug re-derive of `txn_refs` is no longer running at every \
         door. It ran for the first {AUDIT_FULL_MAX} crossings, which is where a door that \
         bypasses insert_workspace/remove_workspace would have shown itself. A door that is \
         correct below {AUDIT_FULL_MAX} and wrong above it is NOT covered."
    );
}

/// Resolves a branch name written in SQL (`b_3`) to a live `BranchId`.
pub trait BranchResolver {
    fn resolve_branch(&self, name: &str) -> Result<BranchId, FerroError>;
}

/// Everything a caller declares about the run behind a session — what provenance interns.
///
/// A struct rather than four more positional parameters on `begin_session_with_model`. Three of
/// the four fields are optional and two of those are `Option<&str>`: as parameters, `run_id` and
/// `prompt` would sit in one argument list with nothing in the type system between them, and a
/// swapped pair would attribute every row of the run to a prompt while hashing the run id — a
/// mistake with no symptom, because both values are strings and both are accepted.
///
/// [`Default`] gives the shape a caller who declares only an agent: no run, no model, no prompt.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunIdentity<'a> {
    /// Stable identity of the agent across runs. The one field that is required.
    pub agent_id: &'a str,
    /// This particular invocation. `None` is recorded as the literal `<unnamed>`.
    pub run_id: Option<&'a str>,
    /// `(name, version)`. `None` is recorded as `unspecified` in both halves, which reads as
    /// "never declared" rather than attributing the writes to a model nobody named.
    pub model: Option<(&'a str, &'a str)>,
    /// The prompt behind the run, as text. It is hashed by `begin_session_as` and never stored:
    /// `RunEntity::prompt_hash` is the digest, so a prompt carrying customer data does not become
    /// a durable copy of it.
    ///
    /// `None` — no prompt declared — is the all-zero hash, deliberately **not**
    /// `prompt_digest("")`. An empty prompt is a prompt.
    pub prompt: Option<&'a str>,
}

pub struct AgentRuntime {
    branches: Arc<dyn BranchCatalog>,
    /// Interned runs, and which run wrote each version the executor writes.
    ///
    /// This is the id authority for attribution. Attribution is run-level, so two sessions for
    /// the same `(agent_id, run_id)` must share one `ProvId`; a local counter cannot honour that
    /// and would split one run across two entities.
    prov_store: Arc<dyn ProvenanceStore>,
    /// Rows on copy-on-write pages, when this runtime was built with a page store.
    ///
    /// `None` is the historical shape: rows live only in each `Workspace`'s map, which is enough
    /// for the merge semantics but means exit criteria 1 and 8 — both claims about *data pages* —
    /// have no pages to be about. `Some` puts a branch's rows behind its own root page, so
    /// "forking copies zero pages" becomes a measurement rather than a component-level assertion.
    storage: Option<PagedRows>,
    /// Where captured frames go. One frame per agent task, re-appended as the task grows, so a
    /// merge engine on the other side of this trait sees exactly what the SQL layer captured.
    log: Arc<dyn EffectLog>,
    /// The reaper that reclaims a branch's pages the moment the branch is retired, if one is
    /// attached. See [`AgentRuntime::with_reaper`].
    reaper: Option<Arc<dyn Reaper>>,
    state: Mutex<State>,
    /// **The ancestry index (D103).** Jump-pointer forest over the branch graph, so the fork point
    /// of two branches is an O(log depth) query instead of two walks to the root.
    ///
    /// # Why it is its own lock and not a field of `State`
    ///
    /// Hydrating it reads `self.branches`, which takes the catalog lock. `stage_all` documents the
    /// one lock order this runtime keeps — `put_row` takes the catalog lock and must not be
    /// reached while `state` is held — so an ancestry index living inside `State` would have to be
    /// filled from under the state lock, which is exactly the inverted order that deadlocks. This
    /// is a leaf lock: it is taken alone, never while `state` is held, and nothing under it does
    /// I/O.
    ///
    /// # Why it is hydrated on demand rather than written at fork
    ///
    /// One door instead of two. A fork hook would be a second place that has to stay in step with
    /// the branch catalog, and a catalog that was reopened from a record log — or a branch minted
    /// by anything other than `begin_session_as` — would be missing from the index with nothing to
    /// say so. [`AgentRuntime::ensure_ancestry`] derives the chain from the catalog itself, so the
    /// index cannot disagree with the records it answers about; a branch already present costs one
    /// hash lookup, and only a branch that is absent pays the walk, once.
    ancestry: Mutex<VersionGraph>,
    /// **The branch-history chain (D103).** Tamper-evident record of the branch lifecycle: one
    /// entry per fork, per published merge and per reap, hash-linked per branch and accumulated
    /// into an RFC 6962 Merkle log whose head an operator can publish.
    ///
    /// Also a leaf lock, taken alone and never while `state` or the catalog is held, for the same
    /// reason [`AgentRuntime::ancestry`] is.
    ///
    /// # ⚠ What is attested, and what is deliberately NOT
    ///
    /// **Attested:** that a branch was forked from a particular parent at a particular epoch under
    /// a particular run identity; that a merge published a particular set of row images; that a
    /// branch was reaped, sealing its history. Altering any of those after the fact breaks every
    /// later link, and `verify_consistency` against a previously published head catches a wholesale
    /// rewrite that a chain walk cannot.
    ///
    /// **Not attested: the branch's tree contents at fork or commit time.** That would need a
    /// digest over the whole tree, and the only one this engine has is `cow::cid::subtree_cid`,
    /// whose own doc says "Cost is the whole subtree, every time — there is no memo table". Paying
    /// it on the fork path would make forking O(N) — destroying exit criterion 1, the measured
    /// claim that a fork copies zero pages, to gain a commitment the merge entries already make
    /// for the data that actually reaches the shared tables. So there is **no `BranchOp::Commit`
    /// entry**, and that is a decision rather than an omission.
    ///
    /// **Not durable.** `AttestedHistory` is in-memory and says so (item 4 of its own "What this
    /// does NOT prove"). Across a restart this answers nothing; within one process an operator who
    /// records [`AgentRuntime::attestation_head`] can later prove the log was only appended to.
    /// Persisting it is a separate decision about where roots are published, not a wiring detail.
    attested: Mutex<AttestedHistory>,
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
        AgentRuntime {
            branches,
            log,
            prov_store: Arc::new(MemProvenanceStore::new()),
            storage: None,
            reaper: None,
            state: Mutex::new(State::default()),
            ancestry: Mutex::new(VersionGraph::new()),
            attested: Mutex::new(AttestedHistory::new()),
        }
    }

    /// Does `root` hold a page this branch engine actually wrote?
    ///
    /// **The strength of this check is `read_page`'s, not this function's, and saying so matters.**
    /// `ArenaPageStore::read_page` verifies the page's crc32 before returning it
    /// (`src/branch/arena.rs:684`), so a page written by something else fails there. An earlier
    /// version of this function re-verified the checksum itself, which read as though that call
    /// were what made the guard safe; it was dead weight, and a fire-check proved it — removing it
    /// changed no test outcome, because the store had already refused the page.
    ///
    /// This matters because a page *type* byte can collide by coincidence: `PageHeader::read_from`
    /// will parse `2` out of arbitrary bytes and report a BTreeLeaf. In the CLI, page 1 is the
    /// first catalog page and `TRUNK_ROOT_PAGE` is 1, so that collision is reachable rather than
    /// theoretical. A crc32 over the whole page does not collide by accident.
    ///
    /// **Stated limit:** the runtime takes an `Arc<dyn PageStore>`, and this inherits whatever
    /// validation that store performs. A `PageStore` implementation that does not checksum its
    /// pages makes this probe as weak as a type check.
    fn trunk_tree_exists(store: &Arc<dyn PageStore>, root: PageId) -> bool {
        // An unreadable page — absent, or failing its checksum — is not a tree. A fresh database
        // has nothing at the placeholder.
        store.read_page(root).is_ok()
    }

    /// Build over a real page store **for a database that has no trunk tree yet**, creating one.
    ///
    /// The trunk's root is created here rather than assumed: `TRUNK_ROOT_PAGE` is a placeholder
    /// id for the map-backed runtime, not a B+tree page that exists on disk. Descending from it
    /// would read whatever happens to occupy page 1.
    ///
    /// **Refuses if a trunk tree already exists**, because creating a second one would leave the
    /// first unreachable — every row in it silently gone, with the database looking empty and
    /// perfectly healthy. Use [`AgentRuntime::reopen_with_storage`] to attach to it instead.
    pub fn with_storage(
        branches: Arc<dyn BranchCatalog>,
        log: Arc<dyn EffectLog>,
        store: Arc<dyn PageStore>,
    ) -> Result<Self, FerroError> {
        let existing = branches.get(BranchId::TRUNK)?.root_page_id;
        if Self::trunk_tree_exists(&store, existing) {
            return Err(FerroError::Branch(format!(
                "this database already has a trunk tree at page {existing}; `with_storage` would                  create a second one and orphan every row in the first. Use                  `AgentRuntime::reopen_with_storage` to attach to the existing tree."
            )));
        }
        let rows = PagedRows::new(store);
        let epoch = branches.next_epoch();
        let root = rows.create_root(BranchId::TRUNK, epoch)?;
        branches.set_root(BranchId::TRUNK, root)?;
        Ok(AgentRuntime {
            branches,
            log,
            prov_store: Arc::new(MemProvenanceStore::new()),
            storage: Some(rows),
            reaper: None,
            state: Mutex::new(State::default()),
            ancestry: Mutex::new(VersionGraph::new()),
            attested: Mutex::new(AttestedHistory::new()),
        })
    }

    /// Attach to the trunk tree an earlier process already created.
    ///
    /// This is the other half of [`AgentRuntime::with_storage`], and the two exist as separate
    /// constructors on purpose rather than as one function that guesses. A single "create it if
    /// it is missing" call has to decide, from an ambiguous page, whether it is looking at a fresh
    /// database or an existing one — and the failure mode of guessing wrong is silent: it mints a
    /// new empty trunk root, the old tree becomes unreachable, and the database reports itself
    /// healthy and empty. Two constructors that each refuse the other's case cannot do that.
    ///
    /// Refuses when trunk's recorded root does not hold a page this engine wrote, because the only
    /// alternatives are to invent a tree (losing whatever is really there) or to descend into
    /// unrelated bytes.
    pub fn reopen_with_storage(
        branches: Arc<dyn BranchCatalog>,
        log: Arc<dyn EffectLog>,
        store: Arc<dyn PageStore>,
    ) -> Result<Self, FerroError> {
        let root = branches.get(BranchId::TRUNK)?.root_page_id;
        if !Self::trunk_tree_exists(&store, root) {
            return Err(FerroError::Branch(format!(
                "the catalog records trunk's rows at page {root}, but that page could not be read as one \
                 this branch engine wrote: it is absent, or it fails the checksum the page \
                 store verifies on read. This is a database with no trunk tree, so there is \
                 nothing to reopen - use `AgentRuntime::with_storage`, which creates one. \
                 Refusing rather than inventing a root, because inventing one would hide \
                 whatever is actually on that page."
            )));
        }
        Ok(AgentRuntime {
            branches,
            log,
            prov_store: Arc::new(MemProvenanceStore::new()),
            storage: Some(PagedRows::new(store)),
            reaper: None,
            state: Mutex::new(State::default()),
            ancestry: Mutex::new(VersionGraph::new()),
            attested: Mutex::new(AttestedHistory::new()),
        })
    }

    /// Attach the reaper that reclaims a branch's pages when the branch is retired.
    ///
    /// **Why a retired branch needs a reaper at all.** `seal` marks a merged or abandoned branch
    /// reaped in the catalog, which makes its id a hard error and unpins its parent's pages — but
    /// nothing frees the extents the branch itself allocated. Its shadow pages are garbage the
    /// instant its work is published (the rows are in the shared tables now) and they stay charged
    /// to the store until something takes them back. Without a reaper attached that is a lease
    /// scan away; with one it is immediate, which is what lets a simulation's page count return to
    /// its baseline rather than to its baseline plus the winners.
    ///
    /// **The reaper must be built over the same catalog and page store as this runtime.** A reaper
    /// over a different catalog would reap a record this runtime never wrote. That is the caller's
    /// contract because the `Reaper` trait carries no way to check it.
    pub fn with_reaper(mut self, reaper: Arc<dyn Reaper>) -> Self {
        self.reaper = Some(reaper);
        self
    }

    /// The provenance store: interned runs, and the author of each version the executor wrote.
    /// Swap in a provenance store that **outlives the process**.
    ///
    /// Every constructor builds a `MemProvenanceStore`, which is right for a test and wrong for a
    /// database: the run behind each row lives only in this process, so `who_wrote_row` and
    /// `ferro_row_authors` answer correctly all session and then answer nothing at all after a
    /// restart. B5 built `DurableProvenanceStore` for exactly this and could not wire it, because
    /// it opens a PATH and the constructors take page stores — a database file's name is not
    /// something `with_storage` is given.
    ///
    /// So it is a builder rather than a constructor parameter: the caller that owns the database
    /// path applies it, and the three constructors keep their signatures and their many call sites.
    /// A runtime built without it still works — it is simply in-memory, which is what a test wants.
    pub fn with_durable_provenance(
        mut self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, FerroError> {
        self.prov_store = Arc::new(crate::provenance::DurableProvenanceStore::open(path)?);
        Ok(self)
    }

    pub fn provenance(&self) -> &Arc<dyn ProvenanceStore> {
        &self.prov_store
    }

    /// The page-backed row store, if this runtime has one.
    pub fn storage(&self) -> Option<&PagedRows> {
        self.storage.as_ref()
    }

    fn rows(&self) -> Result<&PagedRows, FerroError> {
        self.storage.as_ref().ok_or_else(|| {
            FerroError::Bind(
                "this runtime has no page store; build it with AgentRuntime::with_storage".into(),
            )
        })
    }

    /// Pages the store currently holds, when this runtime is page-backed.
    ///
    /// `None` — no page store — is deliberately not `Some(0)`. Exit criteria 1 and 8 are both
    /// stated as page counts, and a measurement of a store that does not exist reading as zero is
    /// how "the fork copied nothing" becomes a fact about the instrument instead of the database.
    pub fn live_page_count(&self) -> Result<Option<u32>, FerroError> {
        match &self.storage {
            Some(rows) => Ok(Some(rows.tree().store().live_page_count()?)),
            None => Ok(None),
        }
    }

    /// The page a branch's rows currently hang off.
    pub fn root_of(&self, branch: BranchId) -> Result<PageId, FerroError> {
        Ok(self.branches.get(branch)?.root_page_id)
    }

    /// Write one row on `branch`.
    ///
    /// A copy-on-write update does not write in place, so the new root has to be recorded against
    /// the branch or the write is unreachable and silently lost.
    pub fn put_row(
        &self,
        branch: BranchId,
        table: &str,
        row_id: u64,
        vals: &[Value],
    ) -> Result<(), FerroError> {
        let rows = self.rows()?;
        let root = self.root_of(branch)?;
        let epoch = self.branches.next_epoch();
        let new_root = rows.put(root, branch, epoch, table_id(table).0, row_id, vals)?;
        if new_root != root {
            self.branches.set_root(branch, new_root)?;
        }
        Ok(())
    }

    /// Read one row as `branch` sees it. Never consults another branch.
    pub fn get_row(
        &self,
        branch: BranchId,
        table: &str,
        row_id: u64,
    ) -> Result<Option<Vec<Value>>, FerroError> {
        let rows = self.rows()?;
        rows.get(self.root_of(branch)?, table_id(table).0, row_id)
    }

    /// Delete one row on `branch`.
    pub fn delete_row(
        &self,
        branch: BranchId,
        table: &str,
        row_id: u64,
    ) -> Result<(), FerroError> {
        let rows = self.rows()?;
        let root = self.root_of(branch)?;
        let epoch = self.branches.next_epoch();
        let new_root = rows.delete(root, branch, epoch, table_id(table).0, row_id)?;
        if new_root != root {
            self.branches.set_root(branch, new_root)?;
        }
        Ok(())
    }

    /// Every row of `table` as `branch` sees it, in row order, **lazily**.
    ///
    /// The iterator is the public edge of the streaming scan: collecting it costs exactly what
    /// this call used to cost unconditionally, and not collecting it costs one page. A caller
    /// that wants the whole table writes `.collect::<Result<Vec<_>, _>>()?` and has said so.
    pub fn scan_rows(
        &self,
        branch: BranchId,
        table: &str,
    ) -> Result<impl Iterator<Item = Result<(u64, Vec<Value>), FerroError>>, FerroError> {
        let rows = self.rows()?;
        rows.scan_table(self.root_of(branch)?, table_id(table).0)
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
        self.begin_session_as(RunIdentity { agent_id, run_id, ..RunIdentity::default() }, parent)
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
        self.begin_session_as(RunIdentity { agent_id, run_id, model, prompt: None }, parent)
    }

    /// The full form: fork a branch and intern the run under everything the caller declared
    /// about it, the prompt included.
    ///
    /// This is the only one of the three with a body; the other two are the shapes that predate
    /// [`RunIdentity`] and delegate here. One body is the point — a second copy of the interning
    /// sequence is how the `[0u8; 32]` this row exists to remove survived being fixed once.
    pub fn begin_session_as(
        &self,
        id: RunIdentity<'_>,
        parent: BranchId,
    ) -> Result<AgentSession, FerroError> {
        let (session, durability) = self.begin_session_as_staged(id, parent)?;
        durability.complete()?;
        Ok(session)
    }

    /// `begin_session_as`, **stopping one step short of durable.**
    ///
    /// Returns the session and a [`ForkDurability`] the caller must `complete()` before telling
    /// anyone the fork happened. Use this **only** when holding a lock wider than the runtime's
    /// own — over pgwire that is `ServerContext::catalog()`, held for the whole statement — so the
    /// sync can be awaited after releasing it. Everything this method does before returning is
    /// still inside the caller's exclusion; only the disk round-trip is deferred.
    ///
    /// ⛔ **THE ATOMICITY THIS PRESERVES IS THE REASON IT IS SHAPED THIS WAY, AND IT IS NOT
    /// OBVIOUS.** This function's body is two disjoint critical sections — the branch-catalog
    /// mutations under `logical`, and the `state` block below that snapshots the PARENT's
    /// workspace — and the child's `fork_root` comes from the first while its staged rows come
    /// from the second. Only the caller's wider lock makes those one instant. A `MERGE`,
    /// `CHERRY PICK ... ONTO` or `seal` of the parent landing between them yields a child whose
    /// page-level view and whose staged-buffer view come from different moments: empty rows beside
    /// a `fork_root` that still addresses the parent's arena tree. ⇒ **Do not "fix" the
    /// serialisation by calling this without a wider lock held.** What is safe to move out is the
    /// fsync, and only the fsync — which is exactly what this returns rather than performs.
    pub fn begin_session_as_staged(
        &self,
        id: RunIdentity<'_>,
        parent: BranchId,
    ) -> Result<(AgentSession, ForkDurability), FerroError> {
        let RunIdentity { agent_id, run_id, model, prompt } = id;
        if agent_id.trim().is_empty() {
            return Err(FerroError::Bind("agent id must not be empty".into()));
        }
        // STAGED, not durable. The ticket travels out with the session; see the type's docs.
        let (record, fork_seq_ticket) = self
            .branches
            .fork_staged(parent, LeaseDeadline::from_now(DEFAULT_LEASE_MILLIS))?;
        // ⛔ CONSTRUCTED HERE, on the line after staging, and that placement is the guarantee —
        // not a tidiness choice. From this point every early exit, including any `?` added to this
        // function in future, drops `durability`, and `Drop` discharges the sync. Holding the
        // ticket as a bare `Option<u64>` across the body instead (as the first version did) means
        // the next `?` anyone adds silently recreates a staged fork that nothing ever syncs, with
        // no compiler signal and no test signal.
        let durability =
            ForkDurability { branches: Arc::clone(&self.branches), seq: fork_seq_ticket };
        let branch = record.branch_id;

        let run = run_id.unwrap_or("<unnamed>").to_string();
        let (model_name, model_version) = model.unwrap_or(("unspecified", "unspecified"));
        // **D103 — the fork is attested here, before the state lock is taken.**
        //
        // Hoisted above the lock rather than folded in below it for one reason: the attestation
        // log is a LEAF lock, taken alone and never while `state` is held. Both halves of that
        // discipline have to be kept at every call site or it is not a discipline; see the field
        // docs on `AgentRuntime::attested`. The three values it needs are all derived from the
        // caller's arguments, so nothing forces this below the lock.
        //
        // Cost: one SHA-256 over ~100 bytes plus the Merkle extend, which is `ceil(log2(n))`
        // compressions of 64 bytes and no I/O at all. The fork path immediately above this line
        // appends a `BranchRecord` to the catalog log and fsyncs it, so this is not the same order
        // of expense — and it is deliberately NOT a digest of the branch's tree, which would make
        // a fork O(N) and destroy exit criterion 1.
        //
        // **This is where the prompt stops being text.** Hashed once, here, and the `&str` is
        // dropped at the end of the call: what the store, the WAL identity record, the change feed
        // and `ferro_runs` all receive is 32 bytes. `None` — no `PROMPT` clause — is the all-zero
        // hash, which is not `prompt_digest("")` and must never become it: "no prompt was declared"
        // and "the prompt was empty" are different facts about a run.
        let prompt_hash = prompt.map(prompt_digest).unwrap_or([0u8; 32]);
        self.attest_fork(
            branch,
            parent,
            record.fork_epoch,
            agent_id,
            &run,
            (model_name, model_version),
            &prompt_hash,
        );

        let mut state = self.state.lock().unwrap();
        state.next_txn += 1;
        let txn = TxnId(state.next_txn);

        // Intern first so the store assigns the id, then rebuild the entity carrying it. The
        // store returns the SAME id for a repeated (agent, run), which is what makes attribution
        // run-level; it also refuses a re-intern whose actor tuple disagrees. `same_actor` counts
        // `prompt_hash`, so re-beginning one run under a different prompt is refused here rather
        // than quietly reusing the first prompt's slot.
        let started = LeaseDeadline::now_millis();
        // A plain `?` again, deliberately. `intern` refusing a re-intern whose actor tuple
        // disagrees is the one reachable failure after the fork has been staged, and it used to
        // need a hand-written recovery arm here. It does not any more: `durability` was built on
        // the line after `fork_staged`, so this `?` drops it and `Drop` discharges the sync. That
        // is the difference between an invariant maintained at every call site and one maintained
        // by the type — and the reason to prefer the second is that this `?` is exactly the shape
        // of the next edit someone makes to this function.
        let prov = self.prov_store.intern(&RunEntity::new(
            ProvId::NONE,
            agent_id,
            run.clone(),
            model_name,
            model_version,
            prompt_hash,
            started,
            parent,
        ))?;
        // No second copy of the entity is kept here, because `intern` is ITSELF first-wins: it
        // returns the existing `ProvId` when `same_actor` holds and leaves the stored entity
        // untouched, so the store already holds the record this used to mirror into `State::runs`
        // — including the first `started_at`. That property is load-bearing and survives the move:
        // `same_actor` deliberately excludes `started_at`, so a mirror maintained with `insert`
        // stamped every branch of one run with the LAST session's start and `ferro_runs` reported a
        // run starting after a branch it had already forked. One record cannot disagree with itself
        // the way two that had to be kept in step could.

        let name = format!("b_{}", branch.id);
        state.names.insert(name.clone(), branch);
        let fork_seq = state.apply_seq;
        // Forking from a branch that is itself an open agent task: the child's visible state *is*
        // the parent's state at fork time, uncommitted rows included, exactly as the child's root
        // page is the parent's root page. Taking a snapshot rather than a link is what keeps the
        // parent's *later* writes invisible to the child, and keeps the read path from walking
        // the parent chain — the one pattern DESIGN.md rules out outright.
        //
        // **These clones are O(1), and the snapshot is still a snapshot.** The maps are
        // [`PersistentMap`]s: a clone is an `Arc` bump, the child shares the parent's nodes, and a
        // write copies only its own root-to-leaf path. That is what makes this affordable at
        // fanout. It used to be a deep copy of the parent's whole staged working set, so N
        // children of a parent holding W staged rows cost O(N·W) — measured at 4.1M row copies and
        // 1651 MB for N=1024, W=4000 (`bench/d27_fork_workspace_cost_before.txt`).
        //
        // Sharing does not weaken either property above, and the reason is worth keeping: the
        // nodes are immutable, so a later parent write allocates new nodes and rebinds the
        // PARENT's root while the child still addresses the fork-point tree, and a read is still a
        // descent of the child's own tree with no parent pointer anywhere to walk. Both are tested
        // directly in `tests/d27_fork_shares_without_leaking.rs`, the second against an ancestor
        // chain abandoned out from under the child.
        let (rows, base_rows, tables, unprobeable_rows) = match state.workspaces.get(&parent) {
            Some(p) => (p.rows.clone(), p.base_rows.clone(), p.tables.clone(), p.unprobeable_rows),
            None => (PersistentMap::new(), PersistentMap::new(), PersistentMap::new(), 0),
        };
        let (parent_schema_edits, parent_base_shapes) = match state.workspaces.get(&parent) {
            Some(p) => (p.schema_edits.clone(), p.base_shapes.clone()),
            None => (Arc::new(Vec::new()), PersistentMap::new()),
        };
        // Which ancestors' staged writes did that snapshot just hand us? Only recorded when the
        // parent actually had staged state to hand over: a parent that staged nothing has no
        // published write for a descendant to protect, and naming it here would re-open exactly
        // the leak F6 closed -- a ghost task's scan blocking every revert forever.
        let inherited: Vec<TxnId> = match state.workspaces.get(&parent) {
            Some(p) if !p.rows.is_empty() || !p.schema_edits.is_empty() => {
                let mut v = p.inherited.clone();
                v.push(p.txn);
                v
            }
            _ => Vec::new(),
        };
        state.insert_workspace(
            branch,
            Workspace {
                name: name.clone(),
                prov,
                txn,
                fork_seq,
                fork_root: record.root_page_id,
                rows,
                unprobeable_rows,
                base_rows,
                tables,
                inherited,
                frame: TxnFrame::new(txn, branch, CommitHash::ZERO, 0, 1),
                // A child forked from a live agent task inherits its parent's pending schema
                // edits for the same reason it inherits its rows: the child's visible state IS
                // the parent's state at fork time.
                schema_edits: parent_schema_edits,
                base_shapes: parent_base_shapes,
            },
        );
        state.captures.insert(txn.0, TxnCapture::new(txn, prov, branch));
        Ok((
            AgentSession {
                branch,
                branch_name: name,
                agent_id: agent_id.to_string(),
                run_id: run,
                prov,
                txn,
            },
            durability,
        ))
    }

    /// The interned run behind a branch: which agent + run + model wrote here.
    ///
    /// Answers only for a *live* branch — the workspace is dropped when the branch merges or is
    /// abandoned. For a row that has already been published, ask [`AgentRuntime::who_wrote_row`].
    pub fn run_of(&self, branch: BranchId) -> Option<RunEntity> {
        // The `state` guard is dropped before the store is touched. Every other path in this file
        // takes state -> store and never the reverse, and keeping that one direction is what makes
        // the pair deadlock-free; this is the one place the store answer does not need the guard.
        let prov = {
            let state = self.state.lock().unwrap();
            state.workspaces.get(&branch)?.prov
        };
        self.prov_store.lookup(prov).ok()
    }

    /// Every live branch's interned run, as `(id slot, run)`, in id-slot order — from **one** lock
    /// acquisition.
    ///
    /// [`Self::run_of`] answered for one branch, so `ferro_runs` asked it per branch: it took an
    /// `all_branches` snapshot and then probed the workspace map once for each record. At 10⁶
    /// branches that is 10⁶ acquisitions of the runtime's single `Mutex` — the same one every
    /// `INSERT`, `visible_rows` and `resolve_branch` takes — to return, typically, one row. Reading
    /// a diagnostic view is not permitted to stall every other connection 10⁶ times, and no
    /// predicate pushdown fixes that on its own: a genuinely unselective query brings every
    /// acquisition straight back.
    ///
    /// **It is also the right row source and not merely the cheaper one.** `ferro_runs` has a row
    /// exactly where a workspace exists, so the workspace map *is* the relation; branch records
    /// were being enumerated to discover a subset of this map. The count is now bounded by open
    /// sessions rather than by branches ever forked, which is the number the view is actually about.
    ///
    /// `BTreeMap` order is id-slot order, so the result needs no sort — the same order the
    /// branch-id-ordered scan it replaces produced. That is still true now the map is keyed by
    /// the whole `BranchId`: `Ord` is derived in field order, so it sorts by slot first.
    ///
    /// The slot alone remains an unambiguous row key here because `insert_workspace` holds at
    /// most one workspace per slot (see [`State::workspaces`]); without that invariant a
    /// recycled slot could contribute two rows naming the same branch.
    pub fn live_runs(&self) -> Vec<(u64, RunEntity)> {
        // The guard is dropped before the store is touched, exactly as `run_of` does and for the
        // same reason: every other path takes state -> store and never the reverse.
        let slots: Vec<(u64, ProvId)> = {
            let state = self.state.lock().unwrap();
            state.workspaces.iter().map(|(b, ws)| (b.id, ws.prov)).collect()
        };
        slots
            .into_iter()
            .filter_map(|(id, p)| self.prov_store.lookup(p).ok().map(|e| (id, e)))
            .collect()
    }

    /// Exit criterion 9: which agent + run + model wrote a given row.
    ///
    /// Answers for a row in the shared tables — that is, one some merge published — and keeps
    /// answering after the writing branch is gone. A row nobody attributed (seeded before any
    /// agent ran) returns `None`, never a guess.
    ///
    /// **And it keeps answering after the PROCESS is gone, when the runtime was built with
    /// [`AgentRuntime::with_durable_provenance`]** (row E79c). Both halves of the answer — which
    /// run published the row, and who that run was — now come from the provenance store rather
    /// than from two maps on `State`, so a reopened database answers instead of going quiet. A
    /// runtime on the default `MemProvenanceStore` still forgets at exit, which is the honest
    /// behaviour for an in-memory store and not a regression.
    pub fn who_wrote_row(&self, table: &str, row: RowId) -> Option<RunEntity> {
        let prov = self.prov_store.row_author(table_id(table).0, row.0).ok()?;
        // `ProvId::NONE` means nobody is on record. Returned as `None` explicitly rather than left
        // to `lookup` to reject, so an unattributed row and a damaged store stay distinguishable.
        if prov.is_none() {
            return None;
        }
        self.prov_store.lookup(prov).ok()
    }

    /// Every attributed row of `table`, as `(row, run)`, ordered by row id.
    pub fn authors_of(&self, table: &str) -> Vec<(RowId, RunEntity)> {
        // Ordered by row id because `attributed_rows` ranges a `BTreeMap` over one table's key
        // space; `ferro_row_authors` presents this directly and the order is part of that contract.
        self.prov_store
            .attributed_rows(table_id(table).0)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(r, p)| self.prov_store.lookup(p).ok().map(|e| (RowId(r), e)))
            .collect()
    }

    /// Per-run counters for the observability views, one row per **live** agent task.
    ///
    /// **Counts only; this adds no bookkeeping.** Every number here is the length of something the
    /// workspace already holds — the captured frame, the staged row map, the retained read-set — so
    /// this is a read-only projection of state the runtime maintains for merge, not a new ledger
    /// kept for reporting. Nothing here is durable: a workspace exists only between
    /// `BEGIN AGENT SESSION` and `MERGE` / `ABANDON` (`seal` drops it), so this answers about work
    /// in flight and says nothing about work already published. `authors_of` is the question to ask
    /// about published rows.
    ///
    /// The counters are deliberately separate rather than summed into one "writes" and one "reads",
    /// because they are not interchangeable and adding them would invent a number:
    ///
    /// * `ops_captured` / `guards_captured` come from this task's own `TxnFrame`, which is **not**
    ///   copied at fork, so they count only what this run did.
    /// * `staged_rows` is the workspace's row map, which **is** copied at fork from an open parent
    ///   session (`begin_session_with_model`). For a branch forked from another live agent task it
    ///   therefore includes rows inherited at fork time, not only rows this run wrote. Named
    ///   `staged_rows` and not `rows_written` for exactly that reason. **`blind_writes` is derived
    ///   from the same map and inherits the same caveat**: for a branch forked from an open session
    ///   it counts inherited rows the child never read, which is true of the map and is not a
    ///   statement about what the child did.
    /// * `rows_read_exact` counts DISTINCT `(table, row)` pairs across the exact-version read-sets;
    ///   a point read repeated is one premise, not two.
    /// * `scan_reads` / `scan_rows_observed` are the range and full-scan reads, kept apart from the
    ///   exact ones because a scan retains a predicate summary rather than versions — DESIGN.md
    ///   section 2's "chosen by ACCESS SHAPE, never by size". `rows_observed` is that summary's own
    ///   diagnostic count.
    pub fn run_activity(&self) -> Vec<RunActivity> {
        use crate::provenance::readset::ReadSet;
        let state = self.state.lock().unwrap();
        let mut out = Vec::with_capacity(state.workspaces.len());
        for (branch, ws) in state.workspaces.iter() {
            // **The key IS the branch, generation included** (D158 item 1). This used to read the
            // generation back out of `names`, because the map was keyed by the id SLOT and a
            // fallback of generation 0 would name a *different* branch after a slot was recycled.
            // The map now carries it, so the detour and its fallback are both gone — there is no
            // longer a second place this answer could come from, and so no second place it could
            // disagree.
            let branch = *branch;
            // A `BTreeSet`, not a sorted `Vec`. `Vec::insert` memmoves the tail, so building the
            // distinct set was O(n^2) in the size of a branch's read-set — and it ran while holding
            // the one Mutex every write path also takes, so reading `ferro_run_activity` against a
            // branch with a large read-set stalled every other connection. This is documented as
            // "counts only"; it should not be able to block a writer.
            let mut exact: BTreeSet<(u32, u64)> = BTreeSet::new();
            let mut scan_reads = 0u64;
            let mut scan_rows_observed = 0u64;
            // B4 deleted `Workspace::reads` as a second copy of the read-set the runtime already
            // keeps in `State::captures`, and B9 was written before that deletion. Derived here the
            // way every other consumer derives it (see the same expression at `:905` and `:2079`),
            // so this view still reads the one read-set rather than reviving the duplicate.
            let reads = state.captures.get(&ws.txn.0).map(|c| c.read_sets()).unwrap_or_default();
            for rs in &reads {
                match rs {
                    ReadSet::ExactVersions(versions) => {
                        for v in versions {
                            exact.insert((v.tbl.0, v.row.0));
                        }
                    }
                    ReadSet::Predicate(p) => {
                        scan_reads += 1;
                        scan_rows_observed += p.rows_observed;
                    }
                }
            }
            out.push(RunActivity {
                branch,
                branch_name: ws.name.clone(),
                run: self.prov_store.lookup(ws.prov).ok(),
                ops_captured: ws.frame.ops.len() as u64,
                guards_captured: ws.frame.guards.len() as u64,
                staged_rows: ws.rows.len() as u64,
                rows_read_exact: exact.len() as u64,
                scan_reads,
                scan_rows_observed,
                blind_writes: blind_writes_of(&ws.rows, &reads).len() as u64,
            });
        }
        out.sort_by_key(|a| (a.branch.id, a.branch.generation));
        out
    }

    /// Forget every per-row record this runtime holds for `table`. Called when a table is dropped.
    ///
    /// # Why a drop has to reach in here at all
    ///
    /// Row authorship (now in the provenance store) and `versions` are both keyed by
    /// `table_id(name)` — an FNV hash of the table's **name**, chosen because the catalog mints no
    /// table ids and a name hash is stable across processes where an assignment counter would not
    /// be. The cost of that choice is that a table dropped and recreated under the same name is, to
    /// both of them, the same table: the new one inherits the old one's authorship and version
    /// stamps. Since E79c the authorship half is durable, which makes this sharper rather than
    /// softer — an unrecorded drop is now inherited across a restart as well as within a session,
    /// which is why the store persists a `ForgetTable` record instead of only dropping keys.
    ///
    /// Before B9 that was invisible, because `authors_of` and `who_wrote_row` had no SQL surface.
    /// `ferro_row_authors` gives them one, and it then reported rows of the *previous* table —
    /// attributed to an agent that never touched the new one, for row ids the new table does not
    /// contain. `Catalog::drop_table` already purges `stats` for the same reason (E69 fixed exactly
    /// that omission); this is the same omission one layer over.
    ///
    /// # `versions` is deliberately NOT purged, and the first version of this function purged it
    ///
    /// It looked symmetrical: `versions` is keyed the same way, so a stale stamp under a recycled name
    /// could report a moved premise for a row the branch never read. Purging it **disarmed B1's
    /// read-premise gate**, and the test
    /// `integration_system_views::dropping_a_table_does_not_silence_the_read_premise_gate` exists
    /// because of it.
    ///
    /// `versions` is what the gate compares against at merge admission, and the comparison reads an
    /// **absent** entry as "the premise holds" (`merge`, the `_ => {}` arm). Erasing the entries
    /// therefore erases the evidence: a branch whose premise had already been replaced merged `Clean`
    /// instead of being held. Measured, with a control proving the gate fires in the same fixture
    /// without the drop. It is the same absence-reads-as-unchanged confusion the comment beside that
    /// arm records having already been fixed once, arriving from the opposite direction.
    ///
    /// The two directions are not symmetrical in cost, which is what decides this. A stale stamp
    /// over-approximates staleness and routes to **quarantine** — a hold that stays queryable and can
    /// be released. A missing stamp under-approximates it and **publishes** a merge computed from
    /// state that no longer exists. One is recoverable and the other is not, so absence is the error
    /// worth avoiding. And nothing in this module needed `versions` purged in the first place: no
    /// view exposes it, so purging it bought nothing and cost a safety check.
    ///
    /// **What this deliberately does NOT purge**, stated rather than left to be discovered:
    /// `versions` as above, plus the escrow ledger, the dependency graph and the applied-op log. None
    /// of the latter three is exposed by the observability views, and each is keyed by something other
    /// than the table alone — undoing them on a drop is a separate decision about `REVERT`'s reach,
    /// not a presentation fix.
    ///
    /// Note that authorship deliberately survives an ordinary `DELETE` of the row. That is an audit
    /// record answering "which agent wrote this", which is criterion 9, and it outliving the row is
    /// the point; a dropped *table* is different, because the name can come back attached to
    /// different data.
    pub fn forget_table(&self, table: &str) {
        let tbl = table_id(table).0;
        // The signature returns `()` and a durable store's append can fail, but the failure is not
        // swallowed: `DurableProvenanceStore::forget_table` poisons ITSELF when the record does not
        // reach the file, so every later call through that store refuses rather than reporting a
        // forget a reopen would silently undo. Discarding the value here loses nothing that is not
        // already recorded somewhere louder.
        let _ = self.prov_store.forget_table(tbl);
    }

    // ---- reads -----------------------------------------------------------------------------

    /// Rows this branch wrote without ever reading. See [`blind_writes_of`].
    pub fn blind_writes(&self, branch: BranchId) -> Result<Vec<(TableId, RowId)>, FerroError> {
        let state = self.state.lock().unwrap();
        let ws = state.workspaces.get(&branch).ok_or_else(|| {
            FerroError::Branch(format!("no agent session on branch {branch}"))
        })?;
        let reads = state.captures.get(&ws.txn.0).map(|c| c.read_sets()).unwrap_or_default();
        Ok(blind_writes_of(&ws.rows, &reads))
    }

    /// The branch's changeset, derived from the PAGES rather than from the workspace map.
    ///
    /// This is what `DIFF` looks like when shadow paging provides it: compare the branch's current
    /// root against the root it forked from, and let shared subtrees answer "nothing changed here"
    /// by page identity instead of by comparison.
    ///
    /// It agrees with the map-derived changeset today for a specific reason worth stating, because
    /// it will stop being true: the branch's tree currently holds exactly the rows the branch
    /// staged, so the fork-point tree is empty of them and the diff is precisely the delta. Once
    /// base tables live in the tree as well, the fork root will hold real rows and this same call
    /// will return the same answer for a better reason — the diff will then be doing the work the
    /// map is doing now.
    ///
    /// # D103 — this is `cow::diff`'s synchronised descent, not `CowTree::diff`
    ///
    /// See [`AgentRuntime::page_changeset_with_cost`] for what changed and what it costs.
    pub fn page_changeset(&self, branch: BranchId) -> Result<Vec<PageRowChange>, FerroError> {
        Ok(self.page_changeset_with_cost(branch)?.0)
    }

    /// [`AgentRuntime::page_changeset`], with what the descent cost as integers.
    ///
    /// # Why `page_changeset` stopped calling `CowTree::diff`
    ///
    /// ⚠ D193: this heading used to read "Why the production `DIFF` path stopped calling
    /// `CowTree::diff`". This function is not the production `DIFF` path and never was: `DIFF
    /// <branch>` runs [`AgentRuntime::diff`] (the workspace's touched-rows map, no page tree), and
    /// this function's callers are integration tests and `examples/d103_production_diff_curve.rs`.
    /// Everything below is true of THIS function; none of it is a statement about `DIFF`'s cost.
    ///
    /// `CowTree::diff` prunes by page identity — sound here, and the right test — but it finds the
    /// shared pages by calling `walk_pages` on **both roots** into two `HashSet<PageId>` before it
    /// can prune either against the other. Its own doc conceded it: "page *identity* traversal is
    /// proportional to the tree". So a branch that changed four rows of a million-row table paid a
    /// two-million-page enumeration to discover four changes, and its `pages_examined` counter
    /// reported only the decode half — four — so the O(N) operation read as cheap. Both halves are
    /// now on [`crate::cow::btree::TreeDiff`], and this path pays neither.
    ///
    /// [`crate::cow::diff::diff`] descends the two roots **together**: at each level the children
    /// are merge-joined on their key spans and a pair with equal identity is abandoned in O(1)
    /// without either page being read. Neither side is ever enumerated alone, so the traversal is
    /// O(delta · log_m N) rather than O(N).
    ///
    /// # Which identity, and why not the content digest
    ///
    /// [`crate::cow::diff::PageIdentity`] — the page id **is** the identity. That is sound in this
    /// store and nowhere else, for exactly the reason `CowTree::diff` already relied on: DESIGN.md
    /// rules out content addressing and refcounts, so a subtree that did not change is not merely
    /// equal to its old self, it *is* the same page. The premise is unchanged from the path this
    /// replaces; only the traversal is new.
    ///
    /// ⚠ **`cow::cid::subtree_cid` is deliberately NOT used here**, and passing it in directly
    /// would be a performance regression wearing a skip counter: its own docs say "Cost is the
    /// whole subtree, every time — there is no memo table", so every O(1) skip test would become a
    /// full subtree walk and the diff would cost strictly more than the O(N) path it replaced.
    /// `cow::diff::MemoIdentity` exists for callers that need that digest and must be `warm()`ed
    /// first, with `misses()` checked afterwards so an unwarmed provider is visible rather than
    /// silently slow — `integration_production_diff_wiring::the_memoised_content_identity_agrees`
    /// exercises that contract. `PageIdentity` needs none of it: zero precompute, zero collisions.
    ///
    /// # The counters
    ///
    /// [`DiffCost::visited`] counts nodes whose payload was **decoded**, which on this path is
    /// also every node that was read at all — there is no second, uncounted enumeration hiding
    /// behind it. That is the difference from `pages_examined` and it is why this is the number
    /// the claim is stated in. Integers, not durations: this box runs a build fleet, and a wall
    /// clock here measures the fleet as much as the algorithm.
    pub fn page_changeset_with_cost(
        &self,
        branch: BranchId,
    ) -> Result<(Vec<PageRowChange>, DiffCost), FerroError> {
        let rows = self.rows()?;
        let fork_root = {
            let state = self.state.lock().unwrap();
            state.workspaces.get(&branch).map(|w| w.fork_root)
        }
        .ok_or_else(|| FerroError::Branch(format!("no agent session on branch {branch}")))?;
        let current = self.root_of(branch)?;

        let report = cow_diff(rows.tree(), fork_root, current, &PageIdentity)?;
        let mut out = Vec::with_capacity(report.changes.len());
        for change in &report.changes {
            let (key, before, after) = match change {
                CowChange::Added { key, value } => (key, None, Some(value)),
                CowChange::Removed { key, value } => (key, Some(value), None),
                CowChange::Modified { key, before, after } => (key, Some(before), Some(after)),
            };
            let (table, row) = split_row_key(key)?;
            out.push(PageRowChange {
                table,
                row,
                before: before.map(|v| decode_row(v)).transpose()?,
                after: after.map(|v| decode_row(v)).transpose()?,
            });
        }
        let cost = DiffCost {
            visited: report.visited,
            skipped_subtrees: report.skipped_subtrees,
        };
        Ok((out, cost))
    }

    /// The rows of `table` satisfying a predicate, as `branch` sees them.
    ///
    /// `raw` goes to the planner (so the base scan can use an index); `bound` is applied to the
    /// staged overlay. They must be the SAME predicate in two forms, and the caller binds it once.
    ///
    /// # Why applying the predicate to both sides is correct
    ///
    /// The unfiltered form computes `filter(pred, base ⊕ staged)` where `⊕` is a per-key overlay:
    /// a staged `Present` replaces the base row, a staged `Deleted` removes it. This computes
    /// `filter(pred, base) ⊕' filter(pred, staged)`, and the two are equal because the overlay is
    /// per-KEY and the predicate is per-ROW:
    ///
    /// * key in both — staged wins, so only `pred(staged_version)` matters → the staged version
    ///   is inserted if it passes and the key is REMOVED if it fails (a base row that passed must
    ///   not survive its own staged replacement failing)
    /// * key in base only — `pred(base_version)`, which the planner already applied
    /// * key staged only (a branch INSERT) — `pred(staged_version)`
    /// * staged `Deleted` — removed on both sides
    ///
    /// ⚠ That equality holds for ROW predicates only. A predicate that reads across rows does not
    /// commute with the overlay and must never be pushed here. `select` refuses joins before it
    /// gets this far, and there is no aggregate on this path, so the boundary holds by
    /// construction today — it is stated so the next reader does not widen it silently.
    fn visible_rows_where(
        &self,
        ctx: &ReadCtx,
        branch: Option<BranchId>,
        table: &str,
        alias: Option<&str>,
        raw: Option<&Expr>,
        bound: Option<&BoundExpr>,
    ) -> Result<Vec<(RowId, Vec<Value>)>, FerroError> {
        debug_assert_eq!(
            raw.is_some(),
            bound.is_some(),
            "raw and bound must be the same predicate in two forms, or the two sides of the \
             overlay are filtered differently"
        );
        let base = scan_table_where(table, alias, raw, ctx)?;
        let tbl = table_id(table);
        let mut rows: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
        for r in base {
            rows.insert(row_id_of(&r).0, r);
        }
        if let Some(b) = branch {
            // **D57.** This used to hold the State mutex and iterate EVERY staged row on the
            // branch — all tables — per statement: ~11.5 ns per staged row per read, ×8 at D27's
            // measured 4,000 staged rows (`bench/d57_staged_curve_before.txt`), and the whole
            // walk inside a process-wide critical section. Three things change, none of which
            // alters a result (the D55 commutation argument is untouched; `tests/d57_overlay_probe`
            // pins every case against a hand-derived truth AND against the walk):
            //
            // 1. The map is SNAPSHOTTED under the lock — `PersistentMap::clone` is one `Arc`
            //    bump, which is the whole reason that structure exists — and the lock is released
            //    before any work. What makes that safe is NOT that nobody else writes this
            //    workspace: another connection can hold the same branch (`AS OF BRANCH` reads a
            //    live workspace by name, and pgwire is a thread per connection). It is that the
            //    clone is taken under the lock and the nodes are immutable — `ins` never mutates
            //    one and `Arc::make_mut` is never used on `rows` — so the snapshot is exactly the
            //    map at one instant, which is what the walk under the lock also observed. Do not
            //    hoist the clone out of the lock: that would be a torn read of the root.
            // 2. A predicate with a `pk = literal` conjunct PROBES the one entry that can affect
            //    the result, PROVIDED every staged row sits under its own column 0's key in the
            //    declared variant — `Workspace::unprobeable_rows` counts the rows for which that
            //    is not so, and the walk is taken while it is non-zero.
            // 3. Every other predicate walks only THIS table's prefix of the key space, which is
            //    the honest floor for a non-key predicate — the same reason the base table needs
            //    an index for one.
            let staged = {
                let state = self.state.lock().unwrap();
                state.workspaces.get(&b).map(|ws| (ws.rows.clone(), ws.unprobeable_rows))
            };
            if let Some((staged, unprobeable_rows)) = staged {
                let mut apply = |row: u64, st: &RowState| -> Result<(), FerroError> {
                    match st {
                        RowState::Present(v) => {
                            let keep = match bound {
                                Some(p) => matches!(evaluate(p, v)?, Value::Boolean(true)),
                                None => true,
                            };
                            if keep {
                                rows.insert(row, v.clone());
                            } else {
                                // The staged version is what this branch sees, and it fails
                                // the predicate -- so the base version that passed must go.
                                rows.remove(&row);
                            }
                        }
                        RowState::Deleted => {
                            rows.remove(&row);
                        }
                    }
                    Ok(())
                };
                let pk_type = ctx.catalog.get_table(table).map(|e| e.schema.columns[0].data_type.clone());
                // The probe is sound only while every staged row sits under its own column 0's
                // key AND holds column 0 in the declared variant; `unprobeable_rows` says whether
                // either has ever stopped being true here (see `Workspace`).
                let probe = if unprobeable_rows == 0 { bound.and_then(|p| overlay_probe_key(p, pk_type.as_ref())) } else { None };
                match probe {
                    Some(row) => {
                        if let Some(st) = staged.get(&(tbl.0, row)) {
                            apply(row, st)?;
                        }
                    }
                    None => {
                        for ((_, row), st) in staged.range(&(tbl.0, 0), &(tbl.0, u64::MAX)) {
                            apply(*row, st)?;
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
        ctx: &ReadCtx,
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

        let rows = self.visible_rows_where(
            ctx,
            Some(branch),
            &from.name,
            from.alias.as_deref(),
            where_clause.as_ref(),
            bound_where.as_ref(),
        )?;
        // `visible_rows_where` is the SINGLE authority for the predicate: it pushed it into the
        // base scan and applied it to the staged overlay. This used to re-evaluate every returned
        // row, and that second filter masked a broken first one — a mutant that forgot to filter
        // the overlay survived `tests/d55_pushdown_commutes.rs` because this loop caught what it
        // let through. Two filters is one you cannot test.
        let matched: Vec<(RowId, Vec<Value>)> = rows;

        // Record the read-set against the *reading* session, if there is one.
        if let Some(reader_branch) = reader {
            let shape = access_shape(where_clause.as_ref(), &schema);
            self.record_read(
                reader_branch,
                table_id(&from.name),
                shape,
                &matched,
                where_clause.as_ref(),
                bound_where.as_ref(),
                ReadPurpose::Inspection,
            )?;
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

    /// Retain one access against the reading task's capture.
    ///
    /// `shape` alone decides the form (exact versions for a point or index lookup, a predicate for a
    /// range or scan); `TxnCapture::on_read` performs that routing, so there is no second copy of it
    /// here. What this function owns is the three things only the runtime knows: which versions the
    /// rows it returned are at, **what snapshot the read saw**, and whether the read was an
    /// inspection or a write statement addressing its own rows ([`ReadPurpose`]).
    ///
    /// Refuses when the reading session is gone, rather than retaining nothing and reporting success.
    fn record_read(
        &self,
        reader: BranchId,
        tbl: TableId,
        shape: AccessShape,
        matched: &[(RowId, Vec<Value>)],
        where_clause: Option<&Expr>,
        bound_where: Option<&BoundExpr>,
        purpose: ReadPurpose,
    ) -> Result<(), FerroError> {
        let mut state = self.state.lock().unwrap();
        // **A read whose session is gone REFUSES; it does not report success while retaining
        // nothing.**
        //
        // This was the reachable half of a pair of sequential guards, and the one that got hardened
        // was the unreachable half: `entry().or_insert_with` below covers a missing *capture*, which
        // there is no site to produce, while this arm covers a missing *workspace*, which any
        // connection can produce on demand. Workspace-absent is exactly branch-sealed — `seal`
        // removes it — and `ABANDON BRANCH b_2` from a second connection seals a live session's
        // branch by name.
        //
        // Measured before this: connection 1 opened a session, connection 2 abandoned its branch,
        // and connection 1's `SELECT ... WHERE qty >= 20 AND qty < 50` returned `Ok` with two rows
        // while its retention went on the floor; `REVERT MERGE m_1` then came back unblocked. The
        // rest of that session's surface already refuses — its next write fails with `no agent
        // session on branch` and its `MERGE` fails with `has been reaped` — so the read reporting
        // success was the one operation still lying about it.
        let (txn, prov) = match state.workspaces.get(&reader) {
            Some(ws) => (ws.txn, ws.prov),
            None => {
                return Err(FerroError::Branch(format!(
                    "no agent session on branch {reader}: this read cannot be retained, and a read \
                     that reports success while retaining nothing is indistinguishable from one \
                     that had nothing to retain. The branch was sealed — merged, abandoned or \
                     reaped — while this session still held it."
                )))
            }
        };
        // `begin_ts: 0` means "no published version existed when this row was read", and that is a
        // real observation rather than a null: `state.versions` is written only when a merge
        // PUBLISHES, so a row nobody has merged yet genuinely has no version to name.
        //
        // A baseline sentinel was tried here first and was wrong. Publishing records `begin_ts: seq`
        // and the first `seq` is 1, so a sentinel of 1 collided with the very first publish and the
        // premise check silently compared equal. Absence is already the signal; it does not need a
        // number, and any number picked here is one a real stamp can eventually reach.
        let versions: Vec<VersionRef> = matched
            .iter()
            .map(|(rid, _)| {
                state.versions.get(&(tbl.0, rid.0)).copied().unwrap_or(VersionRef {
                    tbl,
                    row: *rid,
                    rid: RecordId { page_id: 0, slot_num: 0 },
                    begin_ts: 0,
                })
            })
            .collect();
        // **The snapshot this read saw, on the runtime's own version clock.** `record_applied`
        // stamps every published version with `apply_seq`, so everything published so far is
        // `<= apply_seq` and the next version that can exist is `apply_seq + 1`. That is a HIGH
        // WATER MARK, and `DependencyGraphBuilder::build` compares strictly against it
        // (`begin_ts < observed_at`), the same rule `ReadView::is_commited_for_me` applies.
        //
        // The `+ 1` is load-bearing and is the anti-vacuity half of criterion 10. With `apply_seq`
        // itself, a scan would come out depending on the write that landed AT that seq — a write it
        // could not have seen — and every scan would then be a dependent of the merge immediately
        // preceding it, which is the same under-reporting failure with the sign flipped.
        let observed_at = state.apply_seq + 1;
        let summary = predicate_summary(tbl, where_clause, bound_where, matched.len() as u64);
        // `or_insert_with`, never `if let Some`. A read that finds no capture and retains nothing
        // silently is indistinguishable from a read that had nothing to retain, and that is the
        // precise failure this lane exists to remove — the previous shape lost a scan's region with
        // no error anywhere. There is one workspace-creation site and it opens a capture, so this
        // arm should be unreachable; it is written this way so that a second site cannot make
        // retention optional by forgetting.
        let capture = state
            .captures
            .entry(txn.0)
            .or_insert_with(|| TxnCapture::new(txn, prov, reader));
        match purpose {
            ReadPurpose::Inspection => capture.on_read(shape, versions, Some(summary), observed_at),
            ReadPurpose::RowTargeting => capture.on_write_targeting_read(summary, observed_at),
        }
        Ok(())
    }

    /// Retain the scan a WRITE statement's own `WHERE` clause performed.
    ///
    /// **`UPDATE ... WHERE` and `DELETE ... WHERE` are scans, by this module's own definition.** Both
    /// call `visible_rows_where`, which since D71 takes the statement's own predicate and     /// pushes it down — the scan is NOT unpredicated, and the comments at the three write sites     /// below record that, and then evaluate the bound
    /// clause against each one. None of that used to be retained anywhere, and `record_read` had
    /// exactly one caller — `select` — so the read-modify-write shape this lane exists to protect
    /// found ZERO dependents unless the agent happened to spell the scan as a `SELECT` first.
    ///
    /// Measured before this: `INSERT (7, 30); MERGE` then
    /// `UPDATE inventory SET qty = qty + 1 WHERE qty >= 20 AND qty < 50; MERGE` then
    /// `REVERT MERGE m_1` gave `blocked_by = []`, the halt-mode revert proceeded, and row 7 —
    /// carrying the second task's qty 31 — was deleted with no name in any tree. The identical
    /// workload with one extra `SELECT` over the same range gave `blocked_by = [TxnId(2)]`, so the
    /// difference was purely whether the scan had been spelled as a `SELECT`.
    ///
    /// The shape passed is [`AccessShape::FullScan`] because that is the physical access. Exact
    /// versions are deliberately NOT retained here even for a key-shaped clause: the merge engine
    /// already validates the cells a branch wrote against the target's current image with a witness
    /// per cell, and adding a second staleness mechanism on top of it would promote a resolvable
    /// cell merge into a hard `Retry`. What varies is the [`ReadPurpose`].
    fn record_write_scan(
        &self,
        branch: BranchId,
        tbl: TableId,
        schema: &Schema,
        matched: &[(RowId, Vec<Value>)],
        where_clause: Option<&Expr>,
        bound_where: Option<&BoundExpr>,
    ) -> Result<(), FerroError> {
        let purpose = match access_shape(where_clause, schema) {
            // `WHERE <pk> = <literal>`: the statement named the row, it did not look at anything.
            AccessShape::Point | AccessShape::IndexLookup => ReadPurpose::RowTargeting,
            // Anything else compared a value to decide which rows matched. That is looking.
            AccessShape::Range | AccessShape::FullScan => ReadPurpose::Inspection,
        };
        self.record_read(
            branch,
            tbl,
            AccessShape::FullScan,
            matched,
            where_clause,
            bound_where,
            purpose,
        )
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
        // The shape at first touch, for the same reason `base_rows` records the image at first
        // touch: it is the fork point, and a merge that has to reconcile a shape needs one.
        if let Some(name) = match &stmt {
            Stmt::Update { table, .. } | Stmt::Insert { table, .. } | Stmt::Delete { table, .. } => {
                Some(table.clone())
            }
            _ => None,
        } {
            if let Some(entry) = ctx.catalog.get_table(&name) {
                let shape = entry.schema.clone();
                self.note_base_shape(branch, &name, shape);
            }
        }
        match stmt {
            Stmt::Update { table, assignments, where_clause } => {
                self.branch_update(ctx, branch, &table, assignments, where_clause)
            }
            Stmt::Insert { table, values } => self.branch_insert(ctx, branch, &table, values),
            Stmt::Delete { table, where_clause } => {
                self.branch_delete(ctx, branch, &table, where_clause)
            }
            // `ALTER TABLE` does NOT come through here: it is staged by `stage_schema_edit`
            // before this point, because a schema change is not a row write and must not be
            // mixed into the effect log's cell algebra, whose `ColId` is the very ordinal an
            // alteration moves.
            _ => Err(FerroError::Bind(
                "only INSERT / UPDATE / DELETE run inside an agent session".into(),
            )),
        }
    }

    /// Record a table's shape at this branch's first touch of it. First-touch-wins, exactly as
    /// `base_rows` is populated: a later call cannot move the fork point.
    fn note_base_shape(&self, branch: BranchId, table: &str, shape: Schema) {
        let mut state = self.state.lock().unwrap();
        if let Some(ws) = state.workspaces.get_mut(&branch) {
            ws.base_shapes.insert_if_absent(table.to_string(), shape);
        }
    }

    /// The shape this branch is working against: its fork point plus its own pending edits.
    ///
    /// This is what a *second* `ALTER` in one session is validated against, so `ADD COLUMN note`
    /// twice in a row is refused when the agent types it rather than at merge.
    fn branch_shape(&self, branch: BranchId, table: &str, shared: &Schema) -> Schema {
        let state = self.state.lock().unwrap();
        let Some(ws) = state.workspaces.get(&branch) else { return shared.clone() };
        let mut shape = ws.base_shapes.get(table).cloned().unwrap_or_else(|| shared.clone());
        for (t, edit) in ws.schema_edits.iter() {
            if t == table {
                let _ = edit.apply(&mut shape);
            }
        }
        shape
    }

    /// Record an `ALTER TABLE` on a branch — B11.
    ///
    /// Validated **now**, against the branch's own projected shape, with the same rules
    /// [`Catalog::alter_table`] applies (`catalog::alter::resulting_schema`). An agent that types
    /// an `ADD COLUMN ... NOT NULL` is told so immediately rather than at merge, after everything
    /// that depended on it.
    pub fn stage_schema_edit(
        &self,
        catalog: &Catalog,
        branch: BranchId,
        table: &str,
        action: &AlterAction,
    ) -> Result<(), FerroError> {
        let entry = catalog.require_table(table)?;
        let shared = entry.schema.clone();
        let row_count = catalog.stats.get(table).map(|s| s.row_count).unwrap_or(0);
        self.note_base_shape(branch, table, shared.clone());

        let before = self.branch_shape(branch, table, &shared);
        // Refuses here or nowhere: this is the same function the shared-catalog path calls.
        resulting_schema(table, &before, action, row_count)?;

        let edit = match action {
            AlterAction::AddColumn(col) => SchemaEdit::AddColumn(col.clone()),
            AlterAction::RenameColumn { from, to } => {
                SchemaEdit::RenameColumn { from: from.clone(), to: to.clone() }
            }
            AlterAction::RetypeColumn { column, to } => {
                // The type the branch OBSERVED. It is the whole of the precondition the merge
                // re-checks — DESIGN's rule that a guard must name what was seen, not the
                // invariant — and the parser has no reason to know it.
                let from = before
                    .columns
                    .iter()
                    .find(|c| &c.name == column)
                    .map(|c| c.data_type.clone())
                    .ok_or_else(|| {
                        FerroError::Bind(format!("no column '{column}' to retype in '{table}'"))
                    })?;
                SchemaEdit::RetypeColumn { column: column.clone(), from, to: to.clone() }
            }
        };

        let mut state = self.state.lock().unwrap();
        let ws = state
            .workspaces
            .get_mut(&branch)
            .ok_or_else(|| FerroError::Branch(format!("no agent session on branch {branch}")))?;
        Arc::make_mut(&mut ws.schema_edits).push((table.to_string(), edit));
        Ok(())
    }

    /// The schema changes a branch is carrying, for `DIFF` and for tests.
    pub fn pending_schema_edits(&self, branch: BranchId) -> Vec<(String, SchemaEdit)> {
        self.state
            .lock()
            .unwrap()
            .workspaces
            .get(&branch)
            .map(|ws| ws.schema_edits.as_ref().clone())
            .unwrap_or_default()
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
                // Type-directed, as on trunk — and it must be the WRITE form.
                //
                // This is the third of the three sites that bind a literal into a typed column,
                // alongside INSERT's `bind_row_against` and trunk UPDATE in the planner. Calling
                // the comparison form here dropped the `FLOAT` widening, so `SET amount = 5`
                // against a FLOAT column stored `Integer(5)`.
                //
                // On trunk that combination is refused by the tuple encoder's width check. A
                // page-backed branch does not run it — `encode_row` writes whatever variant it is
                // handed — so the branch took the wrong variant silently and carried it until the
                // merge. See `Binder::literal_for_written_column` for why comparisons must NOT
                // share this widening.
                let bound = match Binder::literal_for_written_column(
                    &expr,
                    &schema.columns[idx].data_type,
                ) {
                    Some(res) => crate::binder::binder::BoundExpr::Literal(res?),
                    None => binder.bind_expr(expr.clone(), &scope)?,
                };
                resolved.push((idx, expr, bound));
            }
            (bw, resolved)
        };

        // **D71: PUSH THE PREDICATE, exactly as the READ path already does.**
        //
        // This was `visible_rows(..)` — the UNPREDICATED form — which is
        // `visible_rows_where(.., None, None, None)` and therefore `scan_table_where(table, alias,
        // None, ctx)`: it materialises EVERY row of the table into a `Vec<Vec<Value>>` and the
        // loop below then throws away all but the matches. O(table) per statement.
        //
        // `scan_table_where`'s own doc comment describes this defect and credits D55 with fixing
        // it — *"an agent-session `SELECT … WHERE id = k` read the ENTIRE table to keep one row —
        // O(table) per statement, where the plain path for the identical query is O(log N)"*. D55
        // pushed the predicate through the READ path and left all three WRITE paths on the old
        // form, so the fix was there to be copied, one call away, for however long.
        //
        // Measured before this change (`bench/d71_point_update_curve.txt`): a staged UPDATE is
        // 0.185 -> 2.385 ms across 16x the rows (12.91x, ms-per-1000-rows FLAT at ~0.17), while
        // the identical plain statement is 4.032 -> 5.396 ms (1.34x, ms-per-1000-rows FALLING).
        //
        // ⚠ The filter below STAYS, and that is deliberate rather than redundant: pushdown is a
        // CONSERVATIVE HINT — the planner may narrow the scan or ignore the predicate entirely —
        // so `evaluate` remains the sole authority on what matches and the semantics cannot drift
        // between the two paths. What changes is how many rows reach it, never which ones pass.
        let rows = self.visible_rows_where(
            &ctx.read(),
            Some(branch),
            table,
            None,
            where_clause.as_ref(),
            bound_where.as_ref(),
        )?;
        let mut staged: Vec<Staged> = Vec::new();
        // The rows this statement's own scan returned. See `record_write_scan`.
        let mut matched: Vec<(RowId, Vec<Value>)> = Vec::new();
        for (rid, row) in rows {
            if let Some(p) = &bound_where {
                if !matches!(evaluate(p, &row)?, Value::Boolean(true)) {
                    continue;
                }
            }
            matched.push((rid, row.clone()));
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
            // Collected, not staged: every row of this statement is decided before any of it is
            // recorded. Staging inside the loop is what let a refusal on row 2 leave row 1 rewritten.
            staged.push(Staged {
                row: rid,
                before: Some(row),
                after: RowState::Present(new_row),
                ops,
                guard,
            });
        }
        // Retained BEFORE the write is staged, deliberately: the scan happened whether or not
        // `stage_all` admits the write, and dropping retention when a statement is refused is the
        // same silent loss in a different place.
        self.record_write_scan(
            branch,
            tbl,
            &schema,
            &matched,
            where_clause.as_ref(),
            bound_where.as_ref(),
        )?;
        let touched = staged.len();
        self.stage_all(branch, tbl, table, &schema.columns[0].data_type, staged)?;
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
        // Same type-directed literal binding as the trunk INSERT path in `planner::plan`: a bare
        // numeric literal is read as the declared type of the column it lands in, so a BIGINT or
        // DECIMAL written on a branch is the same value it would have been on trunk.
        let column_types: Vec<&crate::catalog::column::DataType> =
            schema.columns.iter().map(|c| &c.data_type).collect();
        let bound = binder.bind_row_against(values, &column_types, &empty)?;
        let mut row = Vec::with_capacity(bound.len());
        for b in &bound {
            row.push(evaluate(b, &[])?);
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
        // **D71: the duplicate-key check is a POINT LOOKUP, not a table scan.**
        //
        // This was `visible_rows(..)`, which materialises every row of the table and then keeps
        // the one whose `RowId` matches — O(table) to answer "does this one key already exist".
        //
        // It is the third of the three agent write paths that did this, and the only one that
        // could NOT be fixed by passing a statement predicate, because an INSERT has no `WHERE`.
        // What it has is the key itself, so the predicate is BUILT from the row: `pk = <literal>`,
        // as an AST via `value_expr`, exactly as D69 does for `evaluate_merge`'s base lookups.
        // Building it as an AST rather than rendering SQL text is what makes it safe for every key
        // type — a key containing a quote cannot break a tree the way it breaks a query string.
        //
        // ⚠ The `find(|(r, _)| *r == rid)` below STAYS, for the same reason the post-filter stays
        // in `branch_update`: pushdown is a conservative hint and the planner may return a wider
        // set, so the `RowId` comparison remains the sole authority on what counts as a duplicate.
        // Narrowing what is read must never narrow what is checked.
        let pk_pred = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef {
                table: None,
                column: schema.columns[0].name.clone(),
            }),
            operator: TokenType::Equal,
            right: Box::new(value_expr(&row[0])),
        };
        // Bind before scanning: the binder borrows the catalog, and the scan needs it too.
        let bound_pk = {
            let scope = table_scope(table, &schema)?;
            Binder::new(ctx.catalog).bind_expr(pk_pred.clone(), &scope)?
        };
        let existing = self
            .visible_rows_where(
                &ctx.read(),
                Some(branch),
                table,
                None,
                Some(&pk_pred),
                Some(&bound_pk),
            )?
            .into_iter()
            .find(|(r, _)| *r == rid);
        if existing.is_some() {
            return Err(FerroError::Constraint(format!(
                "duplicate primary key in {}",
                table
            )));
        }
        let op = Op::new(tbl, rid, None, OpKind::RowCreate(row.clone()));
        self.stage(branch, tbl, table, &schema.columns[0].data_type, rid, None, RowState::Present(row), vec![op], None)?;
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
        // **D71: PUSH THE PREDICATE, exactly as the READ path already does.**
        //
        // This was `visible_rows(..)` — the UNPREDICATED form — which is
        // `visible_rows_where(.., None, None, None)` and therefore `scan_table_where(table, alias,
        // None, ctx)`: it materialises EVERY row of the table into a `Vec<Vec<Value>>` and the
        // loop below then throws away all but the matches. O(table) per statement.
        //
        // `scan_table_where`'s own doc comment describes this defect and credits D55 with fixing
        // it — *"an agent-session `SELECT … WHERE id = k` read the ENTIRE table to keep one row —
        // O(table) per statement, where the plain path for the identical query is O(log N)"*. D55
        // pushed the predicate through the READ path and left all three WRITE paths on the old
        // form, so the fix was there to be copied, one call away, for however long.
        //
        // Measured before this change (`bench/d71_point_update_curve.txt`): a staged UPDATE is
        // 0.185 -> 2.385 ms across 16x the rows (12.91x, ms-per-1000-rows FLAT at ~0.17), while
        // the identical plain statement is 4.032 -> 5.396 ms (1.34x, ms-per-1000-rows FALLING).
        //
        // ⚠ The filter below STAYS, and that is deliberate rather than redundant: pushdown is a
        // CONSERVATIVE HINT — the planner may narrow the scan or ignore the predicate entirely —
        // so `evaluate` remains the sole authority on what matches and the semantics cannot drift
        // between the two paths. What changes is how many rows reach it, never which ones pass.
        let rows = self.visible_rows_where(
            &ctx.read(),
            Some(branch),
            table,
            None,
            where_clause.as_ref(),
            bound_where.as_ref(),
        )?;
        let mut staged: Vec<Staged> = Vec::new();
        // The rows this statement's own scan returned. See `record_write_scan`.
        let mut matched: Vec<(RowId, Vec<Value>)> = Vec::new();
        for (rid, row) in rows {
            if let Some(p) = &bound_where {
                if !matches!(evaluate(p, &row)?, Value::Boolean(true)) {
                    continue;
                }
            }
            matched.push((rid, row.clone()));
            let guard = match &where_clause {
                Some(w) => Some(guard_from_expr(w, tbl, rid, &schema)?),
                None => None,
            };
            let op = Op::new(tbl, rid, None, OpKind::RowDelete);
            staged.push(Staged {
                row: rid,
                before: Some(row),
                after: RowState::Deleted,
                ops: vec![op],
                guard,
            });
        }
        // Same reason as on the UPDATE path: retained before the refusal point.
        self.record_write_scan(
            branch,
            tbl,
            &schema,
            &matched,
            where_clause.as_ref(),
            bound_where.as_ref(),
        )?;
        let n = staged.len();
        self.stage_all(branch, tbl, table, &schema.columns[0].data_type, staged)?;
        Ok(n)
    }

    /// Put one row change into the branch's buffer and append its ops and guard to the frame.
    ///
    /// A thin wrapper over [`AgentRuntime::stage_all`] so that single-row callers and multi-row callers
    /// go through exactly one refusal path. Two paths is how the multi-row case ended up with weaker
    /// atomicity than the single-row case for months.
    fn stage(
        &self,
        branch: BranchId,
        tbl: TableId,
        table: &str,
        pk_type: &DataType,
        row: RowId,
        before: Option<Vec<Value>>,
        after: RowState,
        ops: Vec<Op>,
        guard: Option<Guard>,
    ) -> Result<(), FerroError> {
        self.stage_all(branch, tbl, table, pk_type, vec![Staged { row, before, after, ops, guard }])
    }

    // ---- the capability envelope ------------------------------------------------------------

    /// Narrow what `branch` is permitted to write, durably.
    ///
    /// **Narrow, not set.** A branch already carrying an envelope cannot be granted authority it
    /// did not have — [`BranchRecord::restrict`] refuses any widening — because an envelope a
    /// governed party can widen is a suggestion. A branch with no envelope is ungoverned, so the
    /// first call installs freely; every child forked afterwards inherits it.
    ///
    /// Installing one on [`BranchId::TRUNK`] is how agent sessions become governed, since
    /// `BEGIN AGENT SESSION` forks out of trunk by default.
    ///
    /// **Sessions already open keep the envelope they forked with.** A capability is what its
    /// holder was handed at creation; changing it underneath a running holder is *revocation*, and
    /// revocation is a design decision this does not make — it has to say what happens to a
    /// statement in flight, and whether a branch may be narrowed below what it has already
    /// written. The lever that does exist for a live agent is
    /// [`AgentRuntime::quarantine`]: the branch stays readable and its `MERGE` is refused, so
    /// nothing it wrote can reach the shared tables.
    /// `installing_an_envelope_does_not_reach_a_session_that_is_already_open` pins both halves.
    ///
    /// **Cost.** Every governed statement that writes a row appends a full `BranchRecord` to the
    /// catalog log and fsyncs it. On the page-backed path `set_root` already does that once per
    /// row written, so this adds a fraction; on a map-backed runtime with a durable catalog it is
    /// the only fsync on the path. Nothing compacts `branches.log`, and `LogBranchCatalog::open`
    /// replays all of it, so a long-lived governed database pays for this at open time too. The
    /// alternative — charging in memory and flushing on a bound — trades exactly the property the
    /// field exists for, so it is a decision to make deliberately rather than a tuning knob.
    pub fn restrict_branch(
        &self,
        branch: BranchId,
        envelope: CapabilityEnvelope,
    ) -> Result<(), FerroError> {
        // **D41 — one catalog operation, not `get`/`restrict`/`put`.** The old shape compared the
        // new envelope against a SNAPSHOT and then wrote the whole record back, so two
        // restrictions racing left the loser's envelope standing: a branch could end up wider than
        // a narrowing that had already been accepted, which is a widening reached by losing a
        // write rather than by being granted one. `restrict_envelope` makes the comparison and the
        // write one atomic step inside the catalog, and touches nothing else in the record — a
        // `set_root`, a `renew_lease` or an `add_arena` landing in the window is no longer part of
        // this write and cannot be discarded by it.
        self.branches.restrict_envelope(branch, envelope)
    }

    /// What `branch` is currently permitted to write, read from its durable record. `None` means
    /// no envelope was ever installed, which is ungoverned.
    pub fn envelope_of(&self, branch: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        Ok(self.branches.get(branch)?.envelope)
    }

    /// Stage every row of ONE statement, or none of them.
    ///
    /// # The defect this shape exists to prevent
    ///
    /// `stage` used to charge escrow and record the row in the same pass, and `branch_update` called it
    /// per row with `?`. So a two-row `UPDATE` refused on its second row returned an error to the client
    /// with the FIRST row already written into the workspace, the frame and the log — and with the first
    /// row's escrow units already spent, so the caller's natural retry ("take less") was refused too,
    /// against a claim drained by a write that never landed. Measured before this change:
    /// `[(1, 20), (2, 20)]` became `[(1, 8), (2, 20)]` on a statement that returned an error.
    ///
    /// The comment that used to sit here said a refused over-draw "leaves no trace in the workspace, the
    /// frame or the log". That was true of one row and false of one statement, which is the more useful
    /// granularity: a client sees statements, not rows.
    ///
    /// # Decide, then apply
    ///
    /// Every spend across every row is computed and checked as a batch first, under one lock, and only
    /// then is anything charged or recorded. Summing per cell matters: one statement can lower the same
    /// cell twice, and checking each half against the full remaining balance would admit a batch that
    /// overdraws in aggregate.
    fn stage_all(
        &self,
        branch: BranchId,
        tbl: TableId,
        table: &str,
        pk_type: &DataType,
        items: Vec<Staged>,
    ) -> Result<(), FerroError> {
        // ---- decide -------------------------------------------------------------------------
        //
        // Governed by the CHANGE TO THE CELL, not by the shape of the op that produced it. Keying off
        // `Add(Int(d < 0))` looked equivalent and was not: `SET qty = -100` is an `Assign`, so it walked
        // straight past the bound and landed the counter at -100 against a floor of 0. A float decrement
        // slipped through the same gap. Comparing before against after catches every op that lowers the
        // value, including ones not yet invented.

        // The session has to exist before the branch record is consulted, so that writing to a
        // sealed branch still reports "no agent session" rather than "reaped". Same refusal, same
        // place in the order, just hoisted ahead of the record read.
        if !self.state.lock().unwrap().workspaces.contains_key(&branch) {
            return Err(FerroError::Branch(format!("no agent session on branch {}", branch)));
        }

        // **The capability envelope, read from the branch's own DURABLE record.**
        //
        // This is the one policy in this runtime that does not live in `Mutex<State>`. Merge
        // policy, escrow claims and quarantine reasons all do, which means a restart silently
        // un-governs every running agent — the envelope is in the record precisely so a reopen
        // finds it still in force.
        //
        // It is evaluated on the same before/after images the escrow check below uses, and for
        // the same reason, spelled out on `CapabilityEnvelope`: an allowlist keyed on the shape of
        // the op is walked around by any other shape with the same effect. An INSERT is the live
        // example — its `Op` carries `col: None`, so a column check reading the ops would see it
        // write no column while it writes every one of them.
        let charge = match self.branches.envelope_of(branch)? {
            None => 0,
            Some(envelope) => {
                let images: Vec<RowImage> = items
                    .iter()
                    .map(|i| RowImage {
                        row: i.row.0,
                        before: i.before.as_deref(),
                        after: match &i.after {
                            RowState::Present(v) => Some(v.as_slice()),
                            RowState::Deleted => None,
                        },
                    })
                    .collect();
                // The whole statement, before a single row is recorded — the same batch rule the
                // escrow check follows just below, and for the same reason. Nothing is charged
                // here: `admit` only answers how much this statement would cost.
                envelope.admit(tbl.0, table, &images)?
            }
        };

        let mut spends: Vec<((TableId, RowId, ColId), i64)> = Vec::new();
        {
            let state = self.state.lock().unwrap();
            for item in &items {
                if let (Some(before_row), RowState::Present(after_row)) = (&item.before, &item.after) {
                    for (idx, (b, a)) in before_row.iter().zip(after_row.iter()).enumerate() {
                        let cell = (tbl, item.row, ColId(idx as u32));
                        if !state.escrow.is_bounded(&cell) {
                            continue;
                        }
                        if let (Some(b), Some(a)) = (numeric(b), numeric(a)) {
                            // Only a decrease consumes headroom; raising the value gives it back and
                            // is always safe, so it is not charged.
                            let drop = b - a;
                            if drop > 0 {
                                spends.push((cell, drop));
                            }
                        }
                    }
                }
            }
            // The whole statement, before a single unit is charged. This is the line that makes the
            // refusal atomic.
            state.escrow.check_all(branch, &spends)?;
        }

        // **Every refusal has now been decided, so the budget can be charged.**
        //
        // The order is load-bearing and was wrong once: charging before `check_all` meant an
        // escrow-refused statement permanently spent envelope budget on rows it never wrote, and
        // since the escrow error tells the client to claim more and retry, an ordinary retry loop
        // burned the whole envelope budget on statements that wrote nothing.
        //
        // `charge_row_writes` is atomic against every other mutation of the record and re-checks
        // the budget under the catalog's own lock, so it is the charge — not `admit` above — that
        // decides. The window it leaves is an I/O failure further down (appending the frame,
        // mirroring to pages) with the budget already spent. That direction is deliberate:
        // charging afterwards would mean a failed record write leaves a row written and
        // unbudgeted, which is fail-open. Over-charging refuses a later write; under-charging
        // admits one.
        if charge > 0 {
            self.branches.charge_row_writes(branch, charge)?;
        }

        // ---- apply --------------------------------------------------------------------------
        //
        // Past this point nothing may fail on a per-row basis: `check_all` has already established that
        // every spend fits, so `spend` cannot refuse.
        for (cell, amount) in spends {
            self.state.lock().unwrap().escrow.spend(branch, cell, amount)?;
        }

        let mut mirrored: Vec<(RowId, RowState)> = Vec::with_capacity(items.len());
        let frame = {
            let mut state = self.state.lock().unwrap();
            let ws = state.workspaces.get_mut(&branch).ok_or_else(|| {
                FerroError::Branch(format!("no agent session on branch {}", branch))
            })?;
            ws.tables.insert(tbl.0, table.to_string());
            for item in items {
                let key = Workspace::key(tbl, item.row);
                ws.base_rows.insert_if_absent(key, item.before);
                if let RowState::Present(v) = &item.after {
                    let probeable = row_id_of(v) == item.row
                        && v.first().is_some_and(|c0| literal_matches(c0, pk_type));
                    if !probeable {
                        ws.unprobeable_rows += 1;
                    }
                }
                ws.rows.insert(key, item.after.clone());
                for op in item.ops {
                    ws.frame.push_op(op);
                }
                if let Some(g) = item.guard {
                    ws.frame.push_guard(g);
                }
                mirrored.push((item.row, item.after));
            }
            ws.frame.clone()
        };
        // Re-appending the task's frame replaces it rather than adding a second copy: `Add` is
        // not idempotent and two copies of one frame would double-count. Appended ONCE for the whole
        // statement, which is also why the frame is cloned after every row is folded in.
        self.log.append(&frame)?;

        // Mirror the staged rows onto the branch's OWN copy-on-write tree, when this runtime has a
        // page store. The workspace map above is still what `DIFF` and `MERGE` read; this is the
        // step that makes the branch's state exist as pages, so the isolation between branches is
        // a property of the page graph rather than of a map that happens not to be shared.
        //
        // Done outside the state lock deliberately: `put_row` takes the catalog lock to publish
        // the branch's new root, and taking the two in the opposite order elsewhere would deadlock.
        //
        // Guarded on `storage`, because `AgentRuntime::new()` is still map-backed and has no tree
        // to write to. A runtime without a page store keeps exactly its old behaviour.
        if self.storage.is_some() {
            debug_assert_eq!(
                table_id(table),
                tbl,
                "the caller's TableId disagrees with the table name it passed, so the tree and the \
                 workspace map would key the same row differently"
            );
            for (row, state) in mirrored {
                match state {
                    RowState::Present(vals) => self.put_row(branch, table, row.0, &vals)?,
                    RowState::Deleted => self.delete_row(branch, table, row.0)?,
                }
            }
        }
        Ok(())
    }

    // ---- DIFF ------------------------------------------------------------------------------

    /// The structured changeset a branch would merge. Exit criterion 4.
    pub fn diff(&self, ctx: &mut ExecCtx, branch: BranchId) -> Result<ChangeSet, FerroError> {
        let (target, rows_meta) = {
            let state = self.state.lock().unwrap();
            let ws = state.workspaces.get(&branch).ok_or_else(|| {
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
    /// Declare a cell bounded, with `slack` units of headroom above its floor.
    pub fn open_escrow(
        &self,
        table: &str,
        row: RowId,
        col: ColId,
        slack: i64,
    ) -> Result<(), FerroError> {
        self.state.lock().unwrap().escrow.open((table_id(table), row, col), slack)
    }

    /// Reserve part of a bounded cell's slack for `branch`.
    ///
    /// Refused when the slack is already spoken for, which is what stops two agents each taking
    /// 12 out of 20 — and it is refused here, where the agent can still ask for less.
    pub fn claim_escrow(
        &self,
        branch: BranchId,
        table: &str,
        row: RowId,
        col: ColId,
        amount: i64,
    ) -> Result<(), FerroError> {
        self.state.lock().unwrap().escrow.claim(branch, (table_id(table), row, col), amount)
    }

    /// Headroom on a bounded cell that nobody has reserved.
    pub fn unclaimed_escrow(&self, table: &str, row: RowId, col: ColId) -> Option<i64> {
        self.state.lock().unwrap().escrow.unclaimed(&(table_id(table), row, col))
    }

    /// What `branch` has reserved and not yet spent.
    pub fn remaining_escrow(
        &self,
        branch: BranchId,
        table: &str,
        row: RowId,
        col: ColId,
    ) -> Option<i64> {
        self.state.lock().unwrap().escrow.remaining(branch, &(table_id(table), row, col))
    }

    /// Hold a branch a verification gate declined: **unmerged, still queryable**.
    ///
    /// Both halves matter. Not merging is the point of declining; staying queryable is what makes
    /// quarantine different from rejection — a branch that tripped a *heuristic* has not been
    /// shown to be wrong, and throwing it away destroys the evidence needed to decide whether it
    /// was. `BranchState::Quarantined` reads as readable for exactly that reason.
    ///
    /// This is the mechanism, not a policy. Nothing decides on its own to call it: which findings
    /// warrant a hold is the gate's business, and the blind-write tier deliberately reports
    /// without deciding.
    pub fn quarantine(&self, branch: BranchId, reason: &str) -> Result<(), FerroError> {
        let rec = self.branches.get(branch)?;
        if rec.state == BranchState::Quarantined {
            return Ok(());
        }
        // **The reason is recorded BEFORE the state is published, and the order is the whole point.**
        //
        // These are two stores with two locks: the reason lives in this runtime's in-memory state, the
        // state flag in the durable branch record. Publishing `Quarantined` makes it visible to every
        // reader on every connection — the runtime is shared by all of them (`pgwire::ServerContext`)
        // — so publishing first left a window in which `ferro_quarantine` showed a held branch with a
        // NULL reason. `system_views` tells the reader that a NULL there means the reason did not
        // survive a restart, which would have been a false statement about a live process.
        //
        // Reversed, the only window left is a branch whose reason is recorded and whose state is still
        // `Live` — invisible to the view, because it selects on state, so a reader sees either nothing
        // or a hold with its reason. `release_from_quarantine` already has the safe order for the same
        // reason: it clears the state first and the reason after.
        //
        // **D41 — `set_state`, not a whole-record `put`, and this pair of stores is untouched by
        // that.** The old write carried the branch's whole record, so a `charge_row_writes` from
        // the write funnel landing between the `get` above and the write was discarded and a
        // governed branch got those row-writes for free — the direction a capability system must
        // not fail in. `set_state` writes the state and nothing else, so the ordering reasoned
        // about above still holds exactly as written: it replaces the durable half of the pair,
        // not the pair.
        self.state
            .lock()
            .unwrap()
            .quarantine_reasons
            .insert(branch.id, reason.to_string());
        if let Err(e) = self.branches.set_state(branch, rec.state, BranchState::Quarantined) {
            // The hold did not happen, so its reason must not outlive it. The two stores above are
            // ordered, not atomic, and this is the one inconsistency between them that the method
            // is in a position to undo.
            self.state.lock().unwrap().quarantine_reasons.remove(&branch.id);
            return Err(e);
        }
        Ok(())
    }

    /// Why this branch is being held, if it is.
    pub fn quarantine_reason(&self, branch: BranchId) -> Option<String> {
        self.state.lock().unwrap().quarantine_reasons.get(&branch.id).cloned()
    }

    /// Every branch currently held for inspection.
    pub fn quarantined_branches(&self) -> Result<Vec<BranchId>, FerroError> {
        // Asks the catalog for the quarantined branches rather than for every branch it holds.
        // The old shape could not have used `live_branches` — that filters to `Live` and so can
        // never return a quarantined branch — but it paid for the whole catalog to find a handful.
        Ok(self
            .branches
            .in_state(BranchState::Quarantined)?
            .into_iter()
            .map(|r| r.branch_id)
            .collect())
    }

    /// Return a held branch to normal service.
    pub fn release_from_quarantine(&self, branch: BranchId) -> Result<(), FerroError> {
        let rec = self.branches.get(branch)?;
        // Checked here as well as inside `set_state` so the ordinary refusal keeps the message a
        // caller can act on. `set_state`'s own compare-and-set is what covers the race between
        // this read and the write, and it can only fire on a branch that moved underneath us.
        if rec.state != BranchState::Quarantined {
            return Err(FerroError::Branch(format!("{branch} is not quarantined")));
        }
        // **D41 — `set_state`, not a whole-record `put`.** Same window, same class: the old write
        // carried the record's envelope, root and arenas back with it.
        self.branches.set_state(branch, BranchState::Quarantined, BranchState::Live)?;
        self.state.lock().unwrap().quarantine_reasons.remove(&branch.id);
        Ok(())
    }

    /// Three-way merge of a branch into its parent, published if the gate admits it. Exit
    /// criteria 5, 6 and 7.
    ///
    /// **This function is policy; the mechanism is [`AgentRuntime::evaluate_merge`].** Scoring a
    /// merge — composing the ops, re-checking the guards, running the verification gate — touches
    /// nothing at all, and lives there. What is left here is the two decisions only a production
    /// merge makes: hold a branch the gate declined, and leave a conflicting branch alive so the
    /// agent can retry against the predicate it was handed.
    ///
    /// A caller that wants the verdict without the consequences (`SIMULATE` scoring K candidate
    /// branches against one base) calls `evaluate_merge` directly and never reaches this function
    /// for the candidates it does not admit. That is the whole point of the split: the score of
    /// candidate 2 must not have been computed against a base candidate 1 already moved.
    pub fn merge(&self, ctx: &mut ExecCtx, branch: BranchId) -> Result<MergeReport, FerroError> {
        // A held branch is held. Letting a merge through would make quarantine advisory, and an
        // advisory hold is not a hold.
        if self.branches.get(branch)?.state == BranchState::Quarantined {
            return Err(FerroError::Branch(format!(
                "{branch} is quarantined and cannot be merged: {}",
                self.quarantine_reason(branch).unwrap_or_else(|| "no reason recorded".into())
            )));
        }

        // No assertions: a production merge is scored by the gate's own checks. `SIMULATE` adds
        // declared invariants to this same list rather than scoring anywhere else.
        let eval = self.evaluate_merge(ctx, branch, &[])?;
        let merge_id = self.next_merge_id();

        // The gate decides BEFORE publication, and its outcome is honoured rather than reported.
        //
        // A stale premise is routed to quarantine rather than rejection, deliberately: the branch's
        // work is not wrong, it was computed against state that has since moved, and quarantine keeps
        // it queryable so an operator or the agent can look at it. Rejecting would destroy it and
        // retrying blindly would recompute against a base that may move again.
        //
        // `HardReject` also quarantines rather than discarding, for the same reason the gate orders
        // `NotEvaluable` last: not knowing whether a merge is safe is worse than knowing it is not, and
        // the safe response to not knowing is to hold, not to throw away.
        if !eval.gate.is_pass() {
            let reason = eval.gate_reason();
            self.quarantine(branch, &reason)?;
            return Ok(eval.into_report(merge_id, false));
        }

        if eval.outcome.is_conflict() {
            // Nothing is published and the branch stays alive: the agent has the violated
            // predicate and can retry.
            return Ok(eval.into_report(merge_id, false));
        }

        self.publish_evaluation_as(ctx, eval, merge_id)
    }

    /// The next merge id. One per `MERGE` statement and one per admitted candidate, whatever the
    /// outcome, so an id names an admission attempt rather than only a success.
    fn next_merge_id(&self) -> String {
        let mut state = self.state.lock().unwrap();
        state.next_merge += 1;
        format!("m_{}", state.next_merge)
    }

    // ---- D103: CHERRY-PICK, on the production op log ----------------------------------------

    /// Published ops, in the order they landed — **the catalogue a cherry-pick selects from.**
    ///
    /// An agent cannot name an op it cannot see, so an operation that takes `seq` numbers needs a
    /// way to learn them. This is the same log `REVERT` addresses ops by, projected to the fields
    /// a selection is made on.
    pub fn pickable_ops(&self) -> Vec<PickableOp> {
        let state = self.state.lock().unwrap();
        let mut branch_of: BTreeMap<u64, BranchId> = BTreeMap::new();
        for rec in state.merges.values() {
            for t in &rec.txns {
                branch_of.insert(t.0, rec.branch);
            }
        }
        state
            .applied
            .iter()
            .map(|o| PickableOp {
                seq: o.seq,
                txn: o.txn,
                branch: branch_of.get(&o.txn.0).copied().unwrap_or(BranchId::TRUNK),
                table: o.table.clone(),
                row: o.row,
                col: o.col,
            })
            .collect()
    }

    /// Apply a **selected subset** of a published branch's ops onto a live agent branch.
    ///
    /// `MERGE` cannot express this: a merge's unit is a whole branch — every effect the source
    /// recorded or none — and no sequence of merges composes to "these three cells and nothing
    /// else". `branch::cherry` is the engine for it and had no caller; this is the caller.
    ///
    /// `ops` names the ops to pick by the sequence numbers the applied-op log recorded them at,
    /// which is what `REVERT` already addresses ops by. A `seq` rather than a `(txn, tbl, row,
    /// col)` tuple because one txn may write a cell more than once and the point of the operation
    /// is to name *one* of those writes.
    ///
    /// # ⚠ One table per pick, and it is a REFUSAL rather than a caveat
    ///
    /// `CherryTarget::commit_all` owes an all-or-nothing contract, and `cherry.rs` is explicit
    /// that the runtime is the one who owes it: "a runtime impl gets it from the one `Mutex` and
    /// the `PendingWrite` batch a merge already publishes under — which is a claim that will need
    /// its own test when that impl exists".
    ///
    /// The staging door that gives it is [`AgentRuntime::stage_all`], which decides every refusal
    /// — the capability envelope, then the escrow check over the whole statement — before it
    /// applies anything. That is atomic **per table**. Across tables it is not: a pick spanning
    /// two tables whose second table is refused would leave the first staged, which is exactly the
    /// half-applied state this module makes unrepresentable rather than merely avoids.
    ///
    /// So a selection touching more than one table is **refused**, not warned about. The dangerous
    /// state is removed rather than documented, and the alternative — widening `stage_all` to
    /// decide across tables — is a change to the door every INSERT, UPDATE and MERGE writes
    /// through, which is not a thing to do as a side effect of adding a caller.
    /// `a_pick_spanning_two_tables_is_refused_rather_than_half_applied` pins it.
    ///
    /// # What it costs
    ///
    /// O(picked + ops on the picked cells). The divergence question is read through D86's
    /// `applied_by_cell` index — the one that already exists — rather than by scanning the log,
    /// which is the contract [`CherryLog::ops_on_cell`] states. Nothing walks `State::applied`.
    pub fn cherry_pick(
        &self,
        ctx: &mut ExecCtx,
        from: BranchId,
        ops: &[u64],
        onto: BranchId,
    ) -> Result<CherryResult, FerroError> {
        if self.branches.get(onto)?.state == BranchState::Quarantined {
            return Err(FerroError::Branch(format!(
                "{onto} is quarantined and cannot be picked onto: {}",
                self.quarantine_reason(onto).unwrap_or_else(|| "no reason recorded".into())
            )));
        }
        let selectors: Vec<OpSelector> = ops.iter().copied().map(OpSelector::from).collect();

        // ---- the log projection, and the rows the plan can touch --------------------------
        let (log, rows_touched, policy) = {
            let state = self.state.lock().unwrap();
            let log = RuntimeCherryLog::project(&state, ops);
            let rows: BTreeSet<(u32, u64)> =
                log.ops.values().map(|o| (o.tbl.0, o.row.0)).collect();
            (log, rows, state.policy.clone())
        };

        // Every table the selection touches. Refused above one, for the reason in the doc.
        let tables: BTreeSet<String> = log.ops.values().map(|o| o.table.clone()).collect();
        if tables.len() > 1 {
            return Err(FerroError::Merge(format!(
                "this pick spans {} tables ({}), and a pick is staged one table at a time — so a \
                 refusal on the second would leave the first applied. Refusing rather than \
                 half-applying; pick each table separately.",
                tables.len(),
                tables.iter().cloned().collect::<Vec<_>>().join(", ")
            )));
        }

        // ---- the target's image of every row the plan can touch ---------------------------
        //
        // Resolved up front, because `CherryTarget::row_image` takes no context and must not do
        // I/O per call. The set is bounded by the selection, so this stays O(picked).
        let mut images: BTreeMap<(u32, u64), Vec<Value>> = BTreeMap::new();
        {
            let state = self.state.lock().unwrap();
            let ws = state.workspaces.get(&onto).ok_or_else(|| {
                FerroError::Branch(format!("no agent session on branch {onto}"))
            })?;
            for key in &rows_touched {
                match ws.rows.get(key) {
                    Some(RowState::Present(v)) => {
                        images.insert(*key, v.clone());
                    }
                    // Staged as deleted: the branch has no such row, and that is not the same as
                    // never having had one.
                    Some(RowState::Deleted) => {}
                    None => {
                        if let Some(Some(v)) = ws.base_rows.get(key) {
                            images.insert(*key, v.clone());
                        }
                    }
                }
            }
        }
        // Rows the branch has never touched are read from the shared tables, by point lookup
        // against the primary key carried in the op's own before-image. A scan here would make a
        // pick of three cells cost O(table), which is the defect D69 removed from `merge`.
        for key in &rows_touched {
            if images.contains_key(key) {
                continue;
            }
            let Some(op) = log.ops.values().find(|o| (o.tbl.0, o.row.0) == *key) else { continue };
            let Some(before) = op.before_row.as_ref().and_then(|r| r.first()).cloned() else {
                continue;
            };
            let Some(entry) = ctx.catalog.get_table(&op.table) else { continue };
            let Some(pk_col) = entry.schema.columns.first().map(|c| c.name.clone()) else {
                continue;
            };
            let pred = Expr::BinaryOp {
                left: Box::new(Expr::ColumnRef { table: None, column: pk_col }),
                operator: TokenType::Equal,
                right: Box::new(value_expr(&before)),
            };
            for row in scan_table_where(&op.table, None, Some(&pred), &ctx.read())? {
                if row_id_of(&row).0 == key.1 {
                    images.insert(*key, row);
                }
            }
        }

        // The schema each staged row lands in, resolved before anything is decided.
        let mut pk_types: BTreeMap<u32, (String, DataType)> = BTreeMap::new();
        for op in log.ops.values() {
            if pk_types.contains_key(&op.tbl.0) {
                continue;
            }
            let entry = ctx
                .catalog
                .get_table(&op.table)
                .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", op.table)))?;
            let ty = entry
                .schema
                .columns
                .first()
                .map(|c| c.data_type.clone())
                .ok_or_else(|| FerroError::Bind(format!("'{}' has no columns", op.table)))?;
            pk_types.insert(op.tbl.0, (op.table.clone(), ty));
        }

        let mut target = BranchCherryTarget {
            runtime: self,
            branch: onto,
            images,
            pk_types,
            committed: false,
        };
        let result = cherry_pick_ops(&log, from, &selectors, onto, &mut target, &policy)?;
        Ok(result)
    }

    // ---- D103: the branch-history chain -----------------------------------------------------

    /// The Merkle head over every attested branch event so far.
    ///
    /// **This is the value an operator publishes.** `AttestedHistory`'s own documentation is blunt
    /// about why it matters: a chain walk passes on a wholesale rewrite, because an adversary who
    /// can write the log can recompute every `prev` after the entry they altered. Only a root
    /// witnessed *before* the rewrite catches that, so "a deployment that never publishes a root
    /// anywhere gets far less from this file than it thinks".
    pub fn attestation_head(&self) -> TreeHead {
        self.attested.lock().unwrap().head()
    }

    /// The whole log, in append order.
    ///
    /// **Index into this is the index an [`InclusionProof`] is about**, which is why it exists
    /// rather than leaving a caller to reassemble log order from per-branch views: those interleave
    /// and reassembling them is a second, wrong definition of the order the proofs are stated in.
    pub fn attested_log(&self) -> Vec<HistoryEntry> {
        self.attested.lock().unwrap().entries().to_vec()
    }

    /// This branch's attested events, oldest first.
    pub fn attested_entries(&self, branch: BranchId) -> Vec<HistoryEntry> {
        let h = self.attested.lock().unwrap();
        h.entries().iter().filter(|e| e.branch == branch).copied().collect()
    }

    /// The branch's current chain head, or `None` if nothing has been attested for it.
    pub fn attestation_of(&self, branch: BranchId) -> Option<Attestation> {
        self.attested.lock().unwrap().head_of(branch)
    }

    /// Re-link this branch's chain and report how many entries verified.
    pub fn verify_attested_branch(&self, branch: BranchId) -> Result<usize, FerroError> {
        self.attested.lock().unwrap().verify_branch(branch).map_err(FerroError::from)
    }

    /// Inclusion proof for the entry at `index` in the log, against [`Self::attestation_head`].
    pub fn attested_inclusion_proof(&self, index: usize) -> Option<InclusionProof> {
        self.attested.lock().unwrap().inclusion_proof(index)
    }

    /// Prove the log at its current size is an append-only extension of the one at `old_size`.
    ///
    /// This is the call that answers "has anything already logged been rewritten", which the
    /// chain walk cannot.
    pub fn attested_consistency_proof(
        &self,
        old_size: usize,
    ) -> Option<crate::branch::attest::ConsistencyProof> {
        self.attested.lock().unwrap().consistency_proof(old_size)
    }

    /// Entries in the log. The size half of a published `(size, root)` witness.
    pub fn attested_len(&self) -> usize {
        self.attested.lock().unwrap().len()
    }

    /// Record a fork. The child's `prev` is the **parent's** head, which is what makes a
    /// verification walk of a child continue into the ancestry it forked from.
    ///
    /// The content is the run identity — agent, run, model and prompt digest — length-prefixed so
    /// the encoding is injective. That is genuine content and it is already in hand: it binds the
    /// branch to the agent run that created it, which is the half of the audit question
    /// (`which agent produced this`) provenance already answers, now bound into a chain that
    /// answers the other half.
    fn attest_fork(
        &self,
        child: BranchId,
        parent: BranchId,
        epoch: Epoch,
        agent_id: &str,
        run_id: &str,
        model: (&str, &str),
        prompt_hash: &[u8; 32],
    ) {
        let mut buf = Vec::with_capacity(96);
        for part in [agent_id.as_bytes(), run_id.as_bytes(), model.0.as_bytes(), model.1.as_bytes()]
        {
            buf.extend_from_slice(&(part.len() as u64).to_be_bytes());
            buf.extend_from_slice(part);
        }
        buf.extend_from_slice(prompt_hash);
        self.attested.lock().unwrap().append_fork(child, parent, epoch, ContentId::of(&buf));
    }

    /// Record a published merge on the branch it published INTO, committing to the row images it
    /// actually wrote.
    ///
    /// O(delta): only the rows this merge published are folded in, each length-prefixed. A merge
    /// that published nothing still gets an entry — "this merge landed and wrote no rows" is a
    /// fact worth being unable to erase.
    fn attest_merge(&self, into: BranchId, epoch: Epoch, images: &PublishedImages) {
        let mut buf = Vec::with_capacity(images.post.len() * 32);
        buf.extend_from_slice(&(images.post.len() as u64).to_be_bytes());
        for ((tbl, row), vals) in &images.post {
            buf.extend_from_slice(&tbl.to_be_bytes());
            buf.extend_from_slice(&row.to_be_bytes());
            // A row that will not encode is not a reason to abandon the attestation: the entry
            // still commits to the key, and a missing value reads as a distinct (empty) encoding
            // rather than as the row being absent.
            let enc = encode_row(vals).unwrap_or_default();
            buf.extend_from_slice(&(enc.len() as u64).to_be_bytes());
            buf.extend_from_slice(&enc);
        }
        self.attested.lock().unwrap().append(into, epoch, BranchOp::Merge, ContentId::of(&buf));
    }

    /// Record a reap, sealing the branch's chain.
    ///
    /// The content is the branch's own head at this moment, so the terminal entry commits to the
    /// entire history being closed: a later attempt to extend a reaped branch's chain has to
    /// contend with an entry that already named the end.
    fn attest_reap(&self, branch: BranchId, epoch: Epoch, published: bool) {
        let mut h = self.attested.lock().unwrap();
        let head = h.head_of(branch).unwrap_or_else(Attestation::genesis);
        let mut buf = Vec::with_capacity(33);
        buf.extend_from_slice(&head.0);
        buf.push(u8::from(published));
        h.append(branch, epoch, BranchOp::Reap, ContentId::of(&buf));
    }

    // ---- D103: ancestry, and the merge it makes possible ------------------------------------

    /// Put `branch` and every ancestor it needs into the ancestry index, deriving the chain from
    /// the branch catalog.
    ///
    /// The fast path is one hash lookup for a branch already present. Only a branch the index has
    /// never seen pays the walk to the first known ancestor, once — after which every ancestry
    /// query about it is O(log depth) through the jump tables.
    ///
    /// ⚠ **A reaped ancestor is an error here, not a missing edge.** If the walk reaches a
    /// `parent_id` the catalog no longer holds, this returns that error rather than treating the
    /// chain as rooted where it stopped. Rooting it there would invent a fork point, and
    /// `VersionGraph::lca` says exactly why that is the failure worth refusing: "a three-way merge
    /// against a fabricated fork point silently treats unrelated rows as concurrent edits."
    ///
    /// Never called with `state` held; see the field docs on [`AgentRuntime::ancestry`].
    fn ensure_ancestry(&self, branch: BranchId) -> Result<(), FerroError> {
        if self.ancestry.lock().unwrap().depth(branch).is_ok() {
            return Ok(());
        }
        // Collect upward, with no lock of ours held while the catalog is read.
        let mut chain: Vec<(BranchId, Option<BranchId>)> = Vec::new();
        let mut cursor = Some(branch);
        while let Some(b) = cursor {
            if self.ancestry.lock().unwrap().depth(b).is_ok() {
                break;
            }
            let record = self.branches.get(b).map_err(|e| {
                FerroError::Branch(format!(
                    "cannot establish the ancestry of {branch}: its ancestor {b} is not in the \
                     branch catalog ({e}). Refusing rather than treating {b} as a root, because a \
                     fabricated fork point makes a three-way merge read unrelated rows as \
                     concurrent edits."
                ))
            })?;
            chain.push((b, record.parent_id));
            cursor = record.parent_id;
        }
        let mut graph = self.ancestry.lock().unwrap();
        // Downward, so a child is never inserted before its parent exists. `continue` covers the
        // race with another thread that hydrated the same chain between the two locks.
        for (b, parent) in chain.into_iter().rev() {
            if graph.depth(b).is_ok() {
                continue;
            }
            let inserted = match parent {
                None => graph.insert_root(b),
                Some(p) => graph.insert_child(b, p),
            };
            inserted.map_err(ancestry_error)?;
        }
        Ok(())
    }

    /// The fork point of two branches, and what finding it cost.
    ///
    /// **This is the query ferrodb had no answer for.** Every merge path in this runtime reads
    /// `record.parent_id` and targets that, so the only fork point it could ever name was a direct
    /// parent — which is why two sibling branches could not be merged at all. `VersionGraph` makes
    /// the general question O(log depth), and this is where it enters the production path.
    pub fn fork_point(&self, a: BranchId, b: BranchId) -> Result<ForkPoint, FerroError> {
        self.ensure_ancestry(a)?;
        self.ensure_ancestry(b)?;
        let graph = self.ancestry.lock().unwrap();
        let (lca, hops) = graph.lca_hops(a, b).map_err(ancestry_error)?;
        let branch = lca.ok_or_else(|| {
            FerroError::Branch(format!(
                "{a} and {b} have no common ancestor, so there is no fork point to merge them \
                 against. Refusing rather than assuming trunk."
            ))
        })?;
        // What a parent-pointer walk to the same answer costs: one dereference per level on each
        // side, down to the fork point. The control for `hops`, in the same unit.
        let (da, db, dl) = (
            graph.depth(a).map_err(ancestry_error)?,
            graph.depth(b).map_err(ancestry_error)?,
            graph.depth(branch).map_err(ancestry_error)?,
        );
        Ok(ForkPoint {
            branch,
            depth: dl,
            hops,
            walk_hops: u64::from(da - dl) + u64::from(db - dl),
        })
    }

    /// **Merge one branch into a SIBLING**, composed three-way against their computed fork point.
    ///
    /// # The capability this adds
    ///
    /// Every other merge path in this runtime targets `record.parent_id` — `merge`,
    /// `evaluate_merge` and `diff` all open with `parent_id.unwrap_or(TRUNK)`. That is not a
    /// policy, it is the only thing they could do: with no LCA there is no fork point for any
    /// other pair, and `ThreeWayMerger` says as much by taking `_lca` and never reading it. The
    /// consequence is the one `SIMULATE` runs into: K candidates forked off one base can only be
    /// admitted **one at a time into that base**, and every candidate after the first is re-scored
    /// against a base the previous admission moved.
    ///
    /// This composes candidate into candidate. The fork point comes from
    /// [`AgentRuntime::fork_point`], so it is the real LCA rather than an assumed parent, and
    /// nothing is published to the shared tables: the composed rows are staged onto `target`,
    /// which then carries both branches' work and merges to the parent **once**.
    ///
    /// # Where the LCA is load-bearing, stated exactly
    ///
    /// Not as a record. `ThreeWayMerger::merge` documents why: "The LCA record itself is not
    /// consulted: what the merge needs from the fork point is its *state*." What a three-way merge
    /// consumes is `CellMerge::base` — the value the cell held at the fork point — and that is
    /// what the fork point supplies here. Give the same two branches a different base and
    /// `resolve_cell` returns a different verdict, which is what makes this a real dependency
    /// rather than a parameter passed for appearance. `only_one_side_moved_the_cell` and
    /// `both_siblings_moved_the_same_cell_and_it_conflicts` pin both directions.
    ///
    /// # Where the base state comes from
    ///
    /// Each workspace's `base_rows` is filled by `insert_if_absent` at the branch's first touch of
    /// a row, so it holds the image that row had **at the fork point** and keeps holding it. Both
    /// siblings forked from the LCA, so either side's entry is the LCA's image of that row; the
    /// source's is preferred and the target's is the fallback for a row only the target touched.
    /// Nothing is read from the shared tables, which is also what keeps this O(delta).
    ///
    /// # Refusals
    ///
    /// * No common ancestor — refused by `fork_point` rather than assumed to be trunk.
    /// * The fork point **is** one of the two branches — that is an ancestor/descendant pair, not
    ///   a fork. Merging a branch into its own ancestor is `merge`, which publishes; this would
    ///   quietly do something else under the same name.
    /// * Either branch quarantined. A hold that a second merge path walks around is not a hold.
    /// * The two sides forked from different shapes of a table they both touched. Composing across
    ///   that would compare images of different widths cell by cell and silently drop the extra
    ///   columns — no conflict, no report, `Clean`.
    ///
    /// # What this does NOT do, rather than leaving it to be discovered
    ///
    /// * **No verification gate and no read-premise check.** Those are admission checks for
    ///   publishing to the shared tables and this publishes nothing; `target` still faces the full
    ///   gate when it merges to the parent, carrying the source's rows and guards with it.
    /// * **Atomic per table, not across tables.** The composed rows are staged one `stage_all` call
    ///   per table, so a per-table refusal (an escrow bound, a capability envelope) leaves earlier
    ///   tables staged. Same shape, and the same reason, as the multi-table residue
    ///   `publish_evaluation_as` documents.
    pub fn merge_into(
        &self,
        ctx: &mut ExecCtx,
        source: BranchId,
        target: BranchId,
    ) -> Result<SiblingMergeReport, FerroError> {
        if source == target {
            return Err(FerroError::Branch(format!(
                "{source} cannot be merged into itself"
            )));
        }
        for b in [source, target] {
            if self.branches.get(b)?.state == BranchState::Quarantined {
                return Err(FerroError::Branch(format!(
                    "{b} is quarantined and cannot take part in a merge: {}",
                    self.quarantine_reason(b).unwrap_or_else(|| "no reason recorded".into())
                )));
            }
        }

        let fork = self.fork_point(source, target)?;
        if fork.branch == source || fork.branch == target {
            return Err(FerroError::Branch(format!(
                "the fork point of {source} and {target} is {} — one is an ancestor of the other, \
                 not a sibling. Use MERGE, which publishes to the ancestor; this path composes two \
                 branches that diverged and would otherwise silently do something different.",
                fork.branch
            )));
        }

        // Both workspaces, taken together under one lock so they describe the same instant.
        let (src, tgt, policy) = {
            let state = self.state.lock().unwrap();
            let src = state.workspaces.get(&source).ok_or_else(|| {
                FerroError::Branch(format!("no agent session on branch {source}"))
            })?;
            let tgt = state.workspaces.get(&target).ok_or_else(|| {
                FerroError::Branch(format!("no agent session on branch {target}"))
            })?;
            (
                SiblingSide {
                    rows: src.rows.clone(),
                    base_rows: src.base_rows.clone(),
                    tables: src.tables.clone(),
                    ops: src.frame.ops.clone(),
                    guards: src.frame.guards.clone(),
                    base_shapes: src.base_shapes.clone(),
                },
                SiblingSide {
                    rows: tgt.rows.clone(),
                    base_rows: tgt.base_rows.clone(),
                    tables: tgt.tables.clone(),
                    ops: tgt.frame.ops.clone(),
                    guards: tgt.frame.guards.clone(),
                    base_shapes: tgt.base_shapes.clone(),
                },
                state.policy.clone(),
            )
        };

        // The shapes both sides forked from must agree for every table they both touched. See the
        // refusal list above for why this is fatal rather than conformed.
        for (name, shape) in src.base_shapes.iter() {
            if let Some(other) = tgt.base_shapes.get(name) {
                if other != shape {
                    return Err(FerroError::Branch(format!(
                        "{source} and {target} forked from different shapes of '{name}', so their \
                         row images cannot be compared cell by cell. Merge each to {} first.",
                        fork.branch
                    )));
                }
            }
        }

        // **The state a guard is re-checked against**: what the target holds for each touched row
        // BEFORE the source's ops land. A guard is a precondition, so it reads the image the write
        // is about to be applied to — the same reading, and the same reason, as `admit_state` on
        // the parent-merge path.
        let mut merged_state = CellState::new();
        let mut row_outcomes: Vec<RowMergeOutcome> = Vec::new();
        // Composed rows to stage, grouped by table so each table is one atomic `stage_all`.
        let mut staged: BTreeMap<u32, Vec<Staged>> = BTreeMap::new();

        for ((t, r), after) in src.rows.iter() {
            let tbl = TableId(*t);
            let row = RowId(*r);
            let table = src
                .tables
                .get(t)
                .cloned()
                .or_else(|| tgt.tables.get(t).cloned())
                .unwrap_or_default();

            // The fork point's image of this row, and the target's image of it now.
            let base = src
                .base_rows
                .get(&(*t, *r))
                .cloned()
                .or_else(|| tgt.base_rows.get(&(*t, *r)).cloned())
                .flatten();
            let on_target = match tgt.rows.get(&(*t, *r)) {
                Some(RowState::Present(v)) => Some(v.clone()),
                Some(RowState::Deleted) => None,
                // The target never touched this row, so it still sees the fork point's image.
                None => base.clone(),
            };
            let mut applied_ops: Vec<Op> = Vec::new();
            let mut discarded = Vec::new();
            let mut conflicts: Vec<ConflictReport> = Vec::new();
            let mut composed: Vec<Op> = Vec::new();
            let mut produced: Option<RowState> = None;

            // The image a guard sees: an insert has no prior image, so its own new row is what a
            // guard on it can refer to.
            let guard_image = match (base.as_ref(), after) {
                (None, RowState::Present(v)) => Some(v.clone()),
                _ => on_target.clone(),
            };

            match (base.as_ref(), after) {
                // insert
                (None, RowState::Present(v)) => {
                    if on_target.is_some() {
                        conflicts.push(ConflictReport {
                            kind: ConflictKind::ContradictoryAssign,
                            tbl,
                            row,
                            col: None,
                            violated_guard: None,
                            ours: Some(Op::new(tbl, row, None, OpKind::RowCreate(v.clone()))),
                            theirs: None,
                            detail: format!("{target} already has a row with this key"),
                        });
                    } else {
                        applied_ops.push(Op::new(tbl, row, None, OpKind::RowCreate(v.clone())));
                        produced = Some(RowState::Present(v.clone()));
                    }
                }
                // delete
                (Some(b), RowState::Deleted) => match &on_target {
                    Some(n) if n != b => conflicts.push(ConflictReport {
                        kind: ConflictKind::DeleteVsWrite,
                        tbl,
                        row,
                        col: None,
                        violated_guard: None,
                        ours: Some(Op::new(tbl, row, None, OpKind::RowDelete)),
                        theirs: None,
                        detail: format!("{target} wrote this row after the two branches forked"),
                    }),
                    Some(_) => {
                        applied_ops.push(Op::new(tbl, row, None, OpKind::RowDelete));
                        produced = Some(RowState::Deleted);
                    }
                    // Already gone on the target: both sides agree, nothing to stage.
                    None => {}
                },
                // update
                (Some(b), RowState::Present(v)) => {
                    let mut new_row = on_target.clone().unwrap_or_else(|| b.clone());
                    if on_target.is_none() {
                        conflicts.push(ConflictReport {
                            kind: ConflictKind::DeleteVsWrite,
                            tbl,
                            row,
                            col: None,
                            violated_guard: None,
                            ours: Some(Op::new(tbl, row, None, OpKind::RowCreate(v.clone()))),
                            theirs: Some(Op::new(tbl, row, None, OpKind::RowDelete)),
                            detail: format!("{target} deleted this row after the two branches forked"),
                        });
                    } else {
                        for idx in 0..v.len().min(b.len()) {
                            if v[idx] == b[idx] {
                                continue;
                            }
                            let col = ColId(idx as u32);
                            let ours = compose_ops(&ours_ops_on_cell(&src.ops, tbl, row, col))
                                .unwrap_or(OpKind::Assign(v[idx].clone()));
                            // The SIBLING's op for the same cell, which is what makes this a
                            // three-way merge rather than a replay. `concurrent_op` cannot answer
                            // here: it reads `state.applied`, the log of what has been PUBLISHED,
                            // and a sibling has published nothing.
                            let theirs = self.sibling_op(&tgt, tbl, row, col, b, idx);
                            let cell = CellMerge {
                                tbl,
                                row,
                                col,
                                base: Some(b[idx].clone()),
                                target: on_target.as_ref().map(|n| n[idx].clone()),
                                ours,
                                theirs,
                            };
                            match resolve_cell(&cell, source, &policy)? {
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
                            produced = Some(RowState::Present(new_row));
                        }
                    }
                }
                (None, RowState::Deleted) => {}
            }

            if let Some(img) = &guard_image {
                for (idx, val) in img.iter().enumerate() {
                    merged_state.set(tbl, row, ColId(idx as u32), val.clone());
                }
            }

            if conflicts.is_empty() {
                if let Some(after) = produced {
                    // The source's guards for this row travel with it: the target now owns the
                    // write, and the predicate that made it legal is part of the write.
                    let guard = src.guards.iter().find(|g| {
                        g.expr
                            .referenced_cells()
                            .iter()
                            .any(|(gt, gr, _)| *gt == tbl && *gr == row)
                    });
                    staged.entry(*t).or_default().push(Staged {
                        row,
                        before: on_target.clone(),
                        after,
                        ops: applied_ops.clone(),
                        guard: guard.cloned(),
                    });
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

        // Guards, re-checked after composition against the state the merge would land on — the
        // same order the parent-merge path uses, and the same reason: a precondition read against
        // its own result would make every ordinary decrement self-conflict.
        for c in check_guards(&src.guards, &merged_state) {
            if let Some(items) = staged.get_mut(&c.tbl.0) {
                items.retain(|s| s.row != c.row);
            }
            match row_outcomes.iter_mut().find(|r| r.tbl == c.tbl && r.row == c.row) {
                Some(r) => {
                    r.conflicts.push(c);
                    r.outcome = MergeOutcome::Conflict(r.conflicts.clone());
                    r.applied.clear();
                }
                None => row_outcomes.push(RowMergeOutcome {
                    table: src.tables.get(&c.tbl.0).cloned().unwrap_or_default(),
                    tbl: c.tbl,
                    row: c.row,
                    outcome: MergeOutcome::Conflict(vec![c.clone()]),
                    applied: Vec::new(),
                    discarded: Vec::new(),
                    conflicts: vec![c],
                }),
            }
        }

        let outcome = MergeReport::aggregate(&row_outcomes, &[]);
        let merge_id = self.next_merge_id();
        if outcome.is_conflict() {
            // Nothing is staged and both branches stay alive, so the agent can retry against the
            // predicate it was handed — the same contract a conflicting `MERGE` has.
            return Ok(SiblingMergeReport {
                merge_id,
                from: source,
                into: target,
                fork: fork.clone(),
                outcome,
                rows: row_outcomes,
                applied: false,
            });
        }

        for (t, items) in staged {
            let name = src
                .tables
                .get(&t)
                .cloned()
                .or_else(|| tgt.tables.get(&t).cloned())
                .ok_or_else(|| {
                    FerroError::Internal(format!("no table name for table id {t} in this merge"))
                })?;
            let entry = ctx
                .catalog
                .get_table(&name)
                .ok_or_else(|| FerroError::Bind(format!("unknown table: {name}")))?;
            let pk_type = entry
                .schema
                .columns
                .first()
                .map(|c| c.data_type.clone())
                .ok_or_else(|| FerroError::Bind(format!("'{name}' has no columns")))?;
            self.stage_all(target, TableId(t), &name, &pk_type, items)?;
        }

        Ok(SiblingMergeReport {
            merge_id,
            from: source,
            into: target,
            fork,
            outcome,
            rows: row_outcomes,
            applied: true,
        })
    }

    /// The sibling's own op for one cell, or `None` if it did not move that cell.
    ///
    /// Deliberately **not** `concurrent_op`: that reads `state.applied`, which is the log of what
    /// has been PUBLISHED to the shared tables. A sibling branch has published nothing, so asking
    /// it returns `None` for every cell and the merge degenerates to a replay of the source over
    /// the target — no conflict ever detected. The sibling's frame is where its writes are.
    fn sibling_op(
        &self,
        tgt: &SiblingSide,
        tbl: TableId,
        row: RowId,
        col: ColId,
        base: &[Value],
        idx: usize,
    ) -> Option<OpKind> {
        let ops: Vec<OpKind> = ours_ops_on_cell(&tgt.ops, tbl, row, col);
        if !ops.is_empty() {
            return compose_ops(&ops).ok();
        }
        // The target moved the cell without an op recorded against it — a whole-row write, for
        // instance. The move is still real, so it is reported as the assignment it amounts to
        // rather than dropped, which would read as "the sibling did not touch this cell".
        match tgt.rows.get(&(tbl.0, row.0)) {
            Some(RowState::Present(v)) if v.get(idx) != base.get(idx) => {
                v.get(idx).cloned().map(OpKind::Assign)
            }
            _ => None,
        }
    }

    /// **Score a merge without performing it.**
    ///
    /// Everything a merge decides — three-way composition against the target, the guard re-check,
    /// the blind-write metric, the read-premise check, any declared assertions, and the
    /// verification gate's verdict over all of them — computed against the target as it stands
    /// now, publishing nothing and mutating no shared state.
    ///
    /// # Why this is a separate function
    ///
    /// `SIMULATE` forks K sibling branches off one base and scores every one of them. If scoring
    /// were merging, candidate 1 would land on the target before candidate 2 was scored, and every
    /// score after the first would be a score against a different database. The evaluations are
    /// therefore all computed against one base, and admission is a second pass that re-evaluates.
    ///
    /// # The staleness the split creates, and the guard on it
    ///
    /// An evaluation is an **optimistic read**: DESIGN.md section 4 says the gate "must run as an
    /// optimistic transaction — it reads the base snapshot to reach a verdict, so if base moves
    /// before merge the verdict is stale. Textbook TOCTOU." Splitting evaluate from publish opens
    /// exactly that window, so the evaluation carries a fingerprint of every row it read and
    /// [`AgentRuntime::publish_evaluation`] refuses to publish against a base that no longer
    /// matches it. The window is closed by refusing, not by hoping it is short.
    ///
    /// `assertions` are declared invariants, checked against the state this merge would leave
    /// behind — see [`crate::agent_sql::gate::AssertionResult`] for why that is not what a guard
    /// does. A production `MERGE` passes none and is scored by the gate's own checks alone.
    pub fn evaluate_merge(
        &self,
        ctx: &mut ExecCtx,
        branch: BranchId,
        assertions: &[Assertion],
    ) -> Result<MergeEvaluation, FerroError> {
        let target = self.branches.get(branch)?.parent_id.unwrap_or(BranchId::TRUNK);
        let snapshot = {
            let state = self.state.lock().unwrap();
            let ws = state.workspaces.get(&branch).ok_or_else(|| {
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
                // B4's read-set, from `captures` rather than a per-workspace copy.
                reads: state.captures.get(&ws.txn.0).map(|c| c.read_sets()).unwrap_or_default(),
                schema_edits: ws.schema_edits.clone(),
                base_shapes: ws.base_shapes.clone(),
            }
        };

        // ---- schema, before rows -----------------------------------------------------------
        //
        // B11. The shape a merge publishes rows into is the merged shape, so the schema has to
        // compose before anything decides what a row means. A schema conflict is reported exactly
        // as a cell conflict is — same `ConflictReport`, same violated predicate — and publishes
        // nothing, because a merge that applied half a shape is worse than one that applied none.
        //
        // Every table the branch touched is considered, not only the ones it altered: a branch
        // that forked before a sibling's `ADD COLUMN` has rows one value short of the target, and
        // that is a schema question about a branch with no schema edits of its own.
        let mut schema_reports: Vec<SchemaMergeReport> = Vec::new();
        let mut schema_conflicts: Vec<ConflictReport> = Vec::new();
        let mut altered_tables: BTreeSet<String> = BTreeSet::new();
        for (_, name) in &snapshot.tables {
            altered_tables.insert(name.clone());
        }
        for (name, _) in snapshot.schema_edits.iter() {
            altered_tables.insert(name.clone());
        }
        let mut merged_shapes: BTreeMap<String, Schema> = BTreeMap::new();
        for name in &altered_tables {
            let entry = ctx
                .catalog
                .get_table(name)
                .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", name)))?;
            let target_now = entry.schema.clone();
            let base = snapshot.base_shapes.get(name).cloned().unwrap_or_else(|| target_now.clone());
            let ours: Vec<SchemaEdit> = snapshot
                .schema_edits
                .iter()
                .filter(|(t, _)| t == name)
                .map(|(_, e)| e.clone())
                .collect();
            let merged = merge_schema(name, table_id(name), &base, &target_now, &ours)?;
            schema_conflicts.extend(merged.outcome.conflicts().iter().cloned());
            merged_shapes.insert(name.clone(), merged.shape.clone());
            schema_reports.push(SchemaMergeReport {
                table: name.clone(),
                outcome: merged.outcome,
                to_apply: merged.to_apply,
                shape: merged.shape.columns.iter().map(|c| c.name.clone()).collect(),
            });
        }

        // Current shared state for every table this branch touched.
        // **B11's schema merge, evaluated here and applied in `publish_evaluation`.**
        //
        // B6 split `merge` into a half that decides and a half that applies, so B11's schema
        // work splits the same way: conflict detection is a question about state and belongs
        // with every other admission check, while the DDL that changes the shape must not run
        // until a decision has been taken. Left whole in either half it would be wrong - in
        // `evaluate` it would alter tables during a dry run, in `publish` its conflicts would
        // arrive after the gate had already passed the merge.
        //
        // `merged_shapes` from B11's version is dropped: it was written and never read.
        let mut schema_reports: Vec<SchemaMergeReport> = Vec::new();
        let mut schema_conflicts: Vec<ConflictReport> = Vec::new();
        let mut altered_tables: BTreeSet<String> = BTreeSet::new();
        for (_, name) in &snapshot.tables {
            altered_tables.insert(name.clone());
        }
        for (name, _) in snapshot.schema_edits.iter() {
            altered_tables.insert(name.clone());
        }
        for name in &altered_tables {
            let entry = ctx
                .catalog
                .get_table(name)
                .ok_or_else(|| FerroError::Bind(format!("unknown table: {}", name)))?;
            let target_now = entry.schema.clone();
            let base = snapshot.base_shapes.get(name).cloned().unwrap_or_else(|| target_now.clone());
            let ours: Vec<SchemaEdit> = snapshot
                .schema_edits
                .iter()
                .filter(|(t, _)| t == name)
                .map(|(_, e)| e.clone())
                .collect();
            let merged = merge_schema(name, table_id(name), &base, &target_now, &ours)?;
            schema_conflicts.extend(merged.outcome.conflicts().iter().cloned());
            schema_reports.push(SchemaMergeReport {
                table: name.clone(),
                outcome: merged.outcome,
                to_apply: merged.to_apply,
                shape: merged.shape.columns.iter().map(|c| c.name.clone()).collect(),
            });
        }

        let mut current: BTreeMap<(u32, u64), Vec<Value>> = BTreeMap::new();
        let mut schemas: BTreeMap<u32, Schema> = BTreeMap::new();
        let mut table_names: BTreeMap<u32, String> =
            snapshot.tables.iter().map(|(t, n)| (*t, n.clone())).collect();
        // ...and for every table an assertion ranges over, which need not be one this branch
        // wrote to. An assertion over a table the candidate never touched is still a claim about
        // the state the merge would leave behind, and it is read here so the fingerprint below
        // covers it: the evaluation depended on those rows, so a change to them invalidates it.
        // ⚠ ASSERTION TABLES MUST BE SCANNED IN FULL — D69 REGRESSION, fixed here.
        //
        // An assertion is a claim about a WHOLE TABLE (`id >= 0` over `audit`), so it needs every
        // row, not the handful this branch touched. D69 replaced the blanket scan below with point
        // lookups over the branch's own rows, on the stated grounds that `current` had exactly two
        // consumers. That was WRONG: `evaluate_assertions(assertions, &schemas, &current, ..)` is a
        // THIRD, and the error was methodological — a grep for `current.get(` finds the two and
        // misses the one passed as `&current`. The result was an assertion examining ZERO rows and
        // refusing admission with "an assertion that never ran is not an assertion that held",
        // which is the gate behaving correctly on a base this function had emptied.
        let mut assertion_tables: BTreeSet<u32> = BTreeSet::new();
        for a in assertions {
            let id = table_id(&a.table).0;
            assertion_tables.insert(id);
            table_names.entry(id).or_insert_with(|| a.table.clone());
        }
        for (t, name) in &table_names {
            let Some(entry) = ctx.catalog.get_table(name) else {
                // An unknown table is not an empty one. A table this branch wrote to must exist;
                // a table only an assertion names is reported by the assertion itself as
                // unevaluable, which the gate hard-rejects.
                if snapshot.tables.contains_key(t) {
                    return Err(FerroError::Bind(format!("unknown table: {}", name)));
                }
                continue;
            };
            schemas.insert(*t, entry.schema.clone());
            // Full scan ONLY for tables an assertion ranges over. A merge with no assertions —
            // the common case — scans nothing and stays O(delta).
            if assertion_tables.contains(t) {
                for row in scan_table(name, &ctx.read())? {
                    current.insert((*t, row_id_of(&row).0), row);
                }
            }
        }

        // **D69 — POINT LOOKUPS, NOT A SCAN.** `current` is consulted at exactly two places, and
        // both are keyed by rows THIS BRANCH touched: the per-row comparison below
        // (`current.get(&(*t, *r))`) and `pre_images` (`pending_writes[].row_key()`). Nothing ever
        // iterates it. It used to be filled by scanning every row of every touched table, which
        // made a merge cost O(table) no matter how little the branch changed — measured at
        // 1.87 us/row, about 1.9 SECONDS per merge against a million-row table to write four rows
        // (D68, `bench/d68_merge_is_o_table.txt`).
        //
        // The row id cannot be turned back into a key — `row_id_of` is a one-way FNV for Varchar,
        // Boolean, Float and Decimal primary keys — but it does not need to be: the branch's own
        // images carry the key in column 0. `after` for rows it wrote, `base_rows` for rows it
        // deleted, and the union covers both.
        //
        // ⚠ This is only safe because `base_fingerprint` NO LONGER READS `current` (D69 step 2).
        // While it did, narrowing this map would have quietly turned a whole-table staleness check
        // into a touched-rows one — a weakening of isolation wearing a speedup's clothes.
        {
            let mut wanted: BTreeMap<u32, Vec<Value>> = BTreeMap::new();
            let mut want = |t: u32, row: &[Value]| {
                if let Some(pk) = row.first() {
                    wanted.entry(t).or_default().push(pk.clone());
                }
            };
            for ((t, _), st) in &snapshot.rows {
                if let RowState::Present(row) = st {
                    want(*t, row);
                }
            }
            for ((t, _), before) in &snapshot.base_rows {
                if let Some(row) = before {
                    want(*t, row);
                }
            }
            for (t, mut pks) in wanted {
                let Some(name) = table_names.get(&t) else { continue };
                let Some(entry) = ctx.catalog.get_table(name) else { continue };
                let Some(pk_col) = entry.schema.columns.first().map(|c| c.name.clone()) else {
                    continue;
                };
                // One branch can touch the same row through several ops.
                pks.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                pks.dedup();
                for key in pks {
                    let pred = Expr::BinaryOp {
                        left: Box::new(Expr::ColumnRef { table: None, column: pk_col.clone() }),
                        operator: TokenType::Equal,
                        right: Box::new(value_expr(&key)),
                    };
                    for row in scan_table_where(name, None, Some(&pred), &ctx.read())? {
                        current.insert((t, row_id_of(&row).0), row);
                    }
                }
            }
        }

        // Every row this branch READ by exact version. The gate's read-premise verdict is computed
        // from `state.versions` for exactly these rows, and `tables_read` cannot cover them: a read
        // does not put its table into the workspace's table map (only `stage_all` does), and a
        // `TableId` is a one-way hash of the name, so there is no table to re-scan. They are
        // fingerprinted directly instead.
        //
        // Without this, an evaluation of a branch that READ `oncall` and WROTE `roster` carried a
        // `Pass` from the read-premise check and a fingerprint over `roster` alone — so a
        // concurrent publication to `oncall` left the fingerprint matching and the stale verdict
        // publishable. That is the hospital case the check exists for, arriving through the door
        // the split opened.
        let premise_rows = premise_rows_of(&snapshot.reads);
        // The base this evaluation is about, as one number. See `publish_evaluation`.
        // D69 — the SAME function over the SAME table set the publish-time check uses
        // (`fingerprint_tables(ctx, &eval.tables_read)`, and `tables_read` IS `table_names`).
        // Computing the two sides differently is how a staleness check stops comparing anything;
        // they are deliberately one call, not two implementations that happen to agree today.
        let base_fingerprint = fnv64_update(
            self.fingerprint_tables(ctx, &table_names)?,
            &self.fingerprint_premises(&premise_rows).to_be_bytes(),
        );

        let mut row_outcomes: Vec<RowMergeOutcome> = Vec::new();
        let mut pending_writes: Vec<PendingWrite> = Vec::new();
        // The state guards are re-checked against; see the comment where it is filled in.
        let mut admit_state = CellState::new();
        // The image this merge would LEAVE for each row it touches — `None` where it would remove
        // the row. Distinct from `admit_state` on purpose: guards are preconditions and read the
        // image before the ops land, assertions are claims about the result and read this one.
        let mut produced: BTreeMap<(u32, u64), Option<Vec<Value>>> = BTreeMap::new();
        let policy_snapshot = { self.state.lock().unwrap().policy.clone() };

        for ((t, r), after) in &snapshot.rows {
            let table = snapshot.tables.get(t).cloned().unwrap_or_default();
            let tbl = TableId(*t);
            let row = RowId(*r);
            let schema = schemas.get(t).cloned().unwrap_or_else(|| Schema::new(Vec::new()));
            let before = snapshot.base_rows.get(&(*t, *r)).cloned().flatten();
            let now = current.get(&(*t, *r)).cloned();

            // **B11 — read this branch's images in the shape the target has NOW, before anything
            // compares them.**
            //
            // A branch's rows were written against its fork-point shape. If a sibling agent merged
            // an `ADD COLUMN` or a widening retype since, those images are the wrong width or the
            // wrong type for the table they are about to be compared against and published into.
            // Conforming here rather than at publication is deliberate: the cell loop below walks
            // `0..v.len().min(b.len())`, so two images of different widths would have their extra
            // columns silently dropped from the merge — no conflict, no report, `Clean`.
            let (before, after) = match (snapshot.base_shapes.get(&table), schemas.get(t)) {
                (Some(base_shape), Some(target_shape)) if base_shape != target_shape => {
                    let b = match &before {
                        Some(v) => Some(conform_row(v, base_shape, target_shape)?),
                        None => None,
                    };
                    let a = match after {
                        RowState::Present(v) => {
                            RowState::Present(conform_row(v, base_shape, target_shape)?)
                        }
                        RowState::Deleted => RowState::Deleted,
                    };
                    (b, a)
                }
                _ => (before, after.clone()),
            };
            let after = &after;
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
                        produced.insert((*t, *r), Some(v.clone()));
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
                        // `None` is the row being GONE, which is not the same as the row being
                        // unchanged: an assertion must be scored over the table without it.
                        produced.insert((*t, *r), None);
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
                        let ours = compose_ops(&ours_ops_on_cell(&snapshot.ops, tbl, row, col))
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
                        // The COMPOSED image, not the branch's own after-image: a cell the target
                        // also moved resolves to `target + ours`, and that is what an assertion has
                        // to be scored against. Scoring the branch's after-image would test a state
                        // no merge produces — and it is exactly the composed one that goes negative
                        // when a third candidate lands on a pair that already composed.
                        produced.insert((*t, *r), Some(new_row.clone()));
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
            // A row whose guard failed publishes nothing, so it must not appear in the image the
            // assertions are scored against either — scoring a candidate against a row it was
            // refused permission to write is scoring a state that will never exist.
            produced.remove(&(c.tbl.0, c.row.0));
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

        // ---- the verification gate, at the one admission point ---------------------------------
        //
        // This used to compute the blind-write metric and stop, with a comment saying the outcome for
        // a heuristic is quarantine "which does not exist yet". Quarantine has existed end to end
        // since `integration_quarantine.rs`; the comment outlived it. So the gate now runs here, and
        // its outcome is honoured.
        //
        // **This is the only place a `VerificationGate` is built on the merge path**, and that is
        // what makes "a simulated candidate is scored by the same gate a production merge uses" a
        // property of the code rather than a claim about it: `SIMULATE` reaches this line through
        // `evaluate_merge` exactly as `MERGE` does, and the only difference between them is the
        // list of declared assertions handed in.
        let blind = blind_writes_of(&snapshot.rows, &snapshot.reads);

        // The premise check: every version this branch READ, against the version the base holds now.
        // `state.versions` is the live map the read recorder itself reads from, so this is the same
        // notion of "current version" on both sides rather than two definitions that can drift.
        let (moved, approximate) = {
            let state = self.state.lock().unwrap();
            let mut moved: Vec<(TableId, RowId, u64, u64)> = Vec::new();
            let mut approximate = false;
            for rs in &snapshot.reads {
                match rs {
                    crate::provenance::readset::ReadSet::ExactVersions(versions) => {
                        for v in versions {
                            // Four cases, and the interesting one is the first. `begin_ts == 0` means
                            // the branch read a row that no merge had published — so if a version
                            // exists for it NOW, someone published it in between and the premise moved.
                            // Treating that zero as "nothing to compare" was the first attempt and it
                            // silently exempted exactly the rows agents read before anyone had written
                            // them, which is most of them.
                            match state.versions.get(&(v.tbl.0, v.row.0)) {
                                Some(now) if now.begin_ts != v.begin_ts => {
                                    moved.push((v.tbl, v.row, v.begin_ts, now.begin_ts));
                                }
                                // Same version: the premise genuinely holds.
                                Some(_) => {}
                                // ABSENT. This used to share an arm with the line above under the
                                // comment "still unpublished, or the same version: the premise
                                // holds", which conflates two different facts. A fresh-context
                                // reader of this code (B9) called that fail-open.
                                //
                                // MEASURED, because the conclusion changes what this arm is for: it
                                // is UNREACHABLE today, and the claim that it closed a live hole was
                                // wrong. `versions` is written only by `record_applied` and NOTHING
                                // in the crate removes from it (`grep versions.remove|retain|clear`
                                // -> no hits, checked across all ten feature branches). And a read
                                // of a row no merge has published is retained as `begin_ts == 0`,
                                // not as a real timestamp - pinned by
                                // `integration_read_premise::a_read_of_an_unpublished_row_is_recorded_as_version_zero`.
                                // So an absent entry always means "unpublished at read time", and
                                // the zero case below is the one that actually runs.
                                //
                                // The arm is kept, split out, as defence for the day something DOES
                                // remove from `versions` - a `DROP TABLE` purge being the obvious
                                // candidate, and B9's own `forget_table` already purges `row_author`
                                // while leaving `versions` alone. On that day an absent entry against
                                // a real version would mean "a premise I verified and have since
                                // lost", and the honest answer is to stop claiming exactness rather
                                // than to report that it holds.
                                //
                                // Deliberately NOT pushed onto `moved`: escalating an unseen row to
                                // a violation would quarantine branches over rows this gate simply
                                // cannot see, which is how wiring `BlindWriteCheck` into admission
                                // took the suite from 871 to 590.
                                None if v.begin_ts != 0 => approximate = true,
                                // Read a row nobody had published, and nobody has since. Holds.
                                None => {}
                            }
                        }
                    }
                    // A scan retains bounds rather than versions, so the most it can support is "some
                    // row in this table moved". That is an over-approximation, and it is why the check
                    // downgrades itself to heuristic rather than pretending to be exact.
                    crate::provenance::readset::ReadSet::Predicate(_) => approximate = true,
                }
            }
            (moved, approximate)
        };

        // Declared invariants, scored against the image this merge would leave behind.
        let assertion_results = evaluate_assertions(assertions, &schemas, &current, &produced);

        // **Only the premise check and the declared assertions gate the merge; the blind-write
        // metric deliberately does not.**
        //
        // `BlindWriteCheck` is `Heuristic`, and its own documentation says why: "a blind write is
        // genuinely suspicious and genuinely not proof of anything — the agent may have had every right
        // to set that row without looking." Every INSERT is a blind write by construction. Wiring it in
        // and honouring the outcome quarantined every ordinary merge in the suite, which is the correct
        // behaviour for the code and the wrong behaviour for the system: an informational metric that
        // starts blocking merges has been promoted without anyone deciding to promote it.
        //
        // So it stays where it was, reported on `MergeReport::blind_writes`, and the gate carries the
        // checks that are actually decidable.
        let mut gate = crate::agent_sql::gate::VerificationGate::new()
            .with(Box::new(crate::agent_sql::gate::ReadPremiseCheck::new(moved, approximate)));
        for r in &assertion_results {
            // One check per assertion, not one check for all of them: the gate runs every check in
            // a tier even after one has fired, so a caller with three broken assertions learns all
            // three in one round trip.
            gate = gate.with(Box::new(crate::agent_sql::gate::AssertionCheck::new(r.clone())));
        }
        let gate = gate.run();

        let outcome = MergeReport::aggregate(&row_outcomes, &schema_reports);

        Ok(MergeEvaluation {
            from: branch,
            into: target,
            outcome,
            rows: row_outcomes,
            blind_writes: blind,
            gate,
            assertions: assertion_results,
            produced,
            base_fingerprint,
            tables_read: table_names,
            premise_rows,
            snapshot,
            pre_images: pending_writes
                .iter()
                .filter_map(|w| w.row_key())
                .filter_map(|k| current.get(&k).map(|row| (k, row.clone())))
                .collect(),
            schema: schema_reports,
            pending: pending_writes,
        })
    }

    /// Publish an evaluation the gate admitted, under a fresh merge id.
    pub fn publish_evaluation(
        &self,
        ctx: &mut ExecCtx,
        eval: MergeEvaluation,
    ) -> Result<MergeReport, FerroError> {
        let merge_id = self.next_merge_id();
        self.publish_evaluation_as(ctx, eval, merge_id)
    }

    /// The publishing half of the split, with the two refusals that make the split safe.
    ///
    /// **1. An evaluation the gate did not admit cannot be published.** Otherwise the split would
    /// have moved the decision from the gate to whoever remembered to look at its verdict.
    ///
    /// **2. An evaluation whose base has moved cannot be published.** This is the TOCTOU window
    /// that splitting evaluate from publish creates, and DESIGN.md section 4 names it: the gate is
    /// an optimistic read, so a verdict computed against a base that has since changed is a
    /// verdict about a database that no longer exists. The fingerprint covers every row of every
    /// table the evaluation read — the tables the branch wrote to and the tables its assertions
    /// ranged over — so any change to them refuses here rather than publishing a stale merge.
    ///
    /// The refusal is the point. `SIMULATE` never trips it, because it re-evaluates each candidate
    /// against the base as it stands at that candidate's turn; a caller that holds an evaluation
    /// across another merge gets an error instead of a silently wrong publication.
    ///
    /// **Two costs this imposes on every ordinary `MERGE`, stated rather than discovered:**
    ///
    /// * The touched tables are scanned twice — once to evaluate, once to fingerprint here. On a
    ///   large table that doubles the merge's scan. The alternative is publishing against a base
    ///   that may have moved, and there is no cheaper sufficient signal: a row count cannot see an
    ///   update, and the version map only moves when a merge publishes.
    /// * `MERGE` can now return an `Err` where it previously always returned a `MergeReport`. It
    ///   happens only when another connection changed the base between this merge's own evaluate
    ///   and publish, and the answer is to run `MERGE` again. The pre-split code had the same race
    ///   and resolved it by publishing the stale merge, which is the failure this replaces.
    fn publish_evaluation_as(
        &self,
        ctx: &mut ExecCtx,
        eval: MergeEvaluation,
        merge_id: String,
    ) -> Result<MergeReport, FerroError> {
        if !eval.gate.is_pass() {
            return Err(FerroError::Merge(format!(
                "refusing to publish {}: the verification gate returned {} — {}",
                eval.from,
                eval.gate.name(),
                eval.gate_reason()
            )));
        }
        if eval.outcome.is_conflict() {
            return Err(FerroError::Merge(format!(
                "refusing to publish {}: the merge conflicts — {}",
                eval.from,
                eval.violated_predicates().join("; ")
            )));
        }

        let now = fnv64_update(
            self.fingerprint_tables(ctx, &eval.tables_read)?,
            &self.fingerprint_premises(&eval.premise_rows).to_be_bytes(),
        );
        if now != eval.base_fingerprint {
            return Err(FerroError::Merge(format!(
                "refusing to publish {} against a base that moved after it was scored: this \
                 evaluation was computed against fingerprint {:x} and the base is now {:x}. \
                 Re-evaluate; publishing would apply a merge decided against a state that no \
                 longer exists.",
                eval.from, eval.base_fingerprint, now
            )));
        }

        let MergeEvaluation {
            from, into, outcome, rows, blind_writes, snapshot, pending, pre_images,
            schema: schema_reports, ..
        } = eval;

        // **The images each row moves BETWEEN, which is what a retained predicate is tested
        // against.** A scan kept a region, not versions, so the only way to ask "did what this merge
        // published fall inside the region you scanned" is against values — and both ends are
        // needed, not just the new one:
        //
        // - `post` is what a scan running after this merge SAW. A write into the scanned range is
        //   the phantom case.
        // - `pre` is what such a scan no longer saw. A write that moved a value OUT of the range, or
        //   deleted the row entirely, is a dependency too: the scan observed an ABSENCE that this
        //   merge caused, and reverting the merge puts the row back inside the range. Phantom
        //   coverage is exactly about the rows that were not there, so leaving `pre` out would drop
        //   half of it.
        //
        // `pre` comes from `current`, the target's image at merge time, rather than from the
        // branch's fork-point `base_rows`: what matters is what the row held immediately before this
        // merge published, which is not the same thing once a concurrent branch has merged.
        // `pre` is carried on the evaluation rather than recomputed here: `current` is
        // evaluate-time state, and B6's `base_fingerprint` refuses to publish onto a base that
        // moved since, so the evaluate-time image IS "what the target held immediately before this
        // merge published". Recomputing it here would re-scan for a value the guard already pinned.
        let images = PublishedImages {
            pre: pre_images,
            post: pending.iter().filter_map(|w| w.published_image()).collect(),
        };

        // **Everything this merge can refuse, it refuses HERE — before the first byte is written.**
        //
        // E82. This function used to publish the rows, commit them, and only then execute the
        // branch's schema edits, one `Catalog::alter_table` call at a time. Each of those calls is
        // individually atomic — `catalog::alter` argues at length that because the heap rewrite is
        // unlogged, "refused" and "unchanged" have to be the same state — and the merge around them
        // was not atomic at all. An edit refused at position *k* left edits `1..k-1` installed in
        // the catalog, flushed to disk and emitted to the change feed, and left every row the
        // branch wrote published and visible on the target, from a statement that returned `Err`.
        // Retrying then found a branch whose rows were already on the target, reported
        // `applied_to_target = false`, and silently dropped the edit it had never applied.
        //
        // So the merge is a plan and then an execution, which is the split one alteration already
        // had, raised to the whole statement:
        //
        // 1. plan every table's whole chain of edits, which makes every refusal any of them can
        //    make while the tables are untouched;
        // 2. carry every row this merge is about to publish into the shape it will land in, and
        //    measure it there;
        // 3. apply the plans;
        // 4. publish the rows.
        //
        // **The schema now goes first, and the order is load-bearing rather than incidental.** The
        // comment this replaces justified rows-then-schema on the grounds that the alter's rewrite
        // widens every row in the table including the ones just published, so publishing narrow and
        // altering afterwards was the same single pass the alter was always going to make. That is
        // true, and it is not the constraint. The constraint is that a plan's decision is only
        // valid for the heap it read: publish rows between deciding and writing and the plan no
        // longer describes the heap it is about to lay down. Schema first, and the deciding pass
        // and the writing pass see the same heap — while the rows go straight into the shape the
        // edits produced, carried there by `conform_row`, the same function that already carries a
        // branch's rows across a sibling's merged `ADD COLUMN`.
        //
        // **What is still fallible after step 3, stated exactly rather than generously.** It is what
        // `catalog::alter` already calls environmental — a disk write that fails, a buffer pool with
        // no evictable frame, the arena floor exhausted by a relocation that `reserve_free_space`
        // could not see coming. Two shapes of residue, and the second is not the first:
        //
        // - **one table**: its shape is applied and no row is published. The merge returns `Err`
        //   and the target is a table with the edit and none of the branch's rows.
        // - **more than one table**: the tables before the failure are altered and the ones after
        //   it are not. `Catalog::apply_plan` persists as it installs, so table X is durable before
        //   table Y is attempted, and there is no undo because the rewrite is unlogged. A merge
        //   over two altered tables is atomic in its REFUSALS and not in its FAILURES.
        //
        // Neither is closed here, for the reason `catalog::alter` gives for refusing rather than
        // rolling back: an undo would be a second unlogged mutation whose own failure would have no
        // repair at all. Closing them needs the heap rewrite logged, which is a larger change than
        // this row. What IS closed, below, is the change feed — it never carries half of a
        // multi-table merge.
        let prov = Arc::clone(self.provenance());
        let mut plans: Vec<(usize, AlterPlan)> = Vec::new();
        for (i, report) in schema_reports.iter().enumerate() {
            if report.to_apply.is_empty() {
                continue;
            }
            let actions: Vec<AlterAction> = report.to_apply.iter().map(|e| e.as_action()).collect();
            let plan = ctx
                .catalog
                .plan_alters(&report.table, &actions, &ctx.txn, Some(&prov))
                .map_err(in_a_merge_the_narrowing_comes_first)?;
            plans.push((i, plan));
        }

        // The shape each table's rows will land in — the one this merge's edits produce where it
        // has any, and the table's current shape everywhere else.
        //
        // A table this merge does not alter is in the map too, and deliberately: a row too wide for
        // a table nobody altered would otherwise be refused by `Tuple::serialize` from inside the
        // publish transaction, after a DIFFERENT table's edits had already landed. One merge, one
        // decision, over every table it touches.
        let mut landing: BTreeMap<String, (Schema, Schema)> = BTreeMap::new();
        for w in &pending {
            let name = w.table();
            if landing.contains_key(name) {
                continue;
            }
            let from = ctx.catalog.require_table(name)?.schema.clone();
            let to = plans
                .iter()
                .find(|(i, _)| schema_reports[*i].table == name)
                .map(|(_, plan)| plan.final_shape().clone())
                .unwrap_or_else(|| from.clone());
            landing.insert(name.to_string(), (from, to));
        }

        let mut ready: Vec<PendingWrite> = Vec::with_capacity(pending.len());
        for w in pending {
            let (from, to) = landing.get(w.table()).ok_or_else(|| {
                FerroError::Internal(format!(
                    "no landing shape for '{}', which this merge is about to write to",
                    w.table()
                ))
            })?;
            ready.push(w.conform_to(from, to)?);
        }

        // ---- the schema, applied while no row of this merge has been written -------------------
        //
        // Executed through exactly the path a non-agent `ALTER TABLE` takes — the catalog's own
        // plan and apply, then a flush, then `log_ddl` — so a column an agent added reaches the
        // change feed as the same in-band, in-log-order event as one a human added, and the
        // retained declaration is updated the same way. A second producer of schema events would be
        // a second chance to disagree with the consumer.
        //
        // One DDL record per edit, carrying the shape THAT edit produced, even though the chain is
        // laid down in one pass: the feed is a sequence of events, and a consumer applying them in
        // order arrives at the same final shape it would have reached from the statements typed one
        // at a time.
        //
        // Flushed rather than checkpointed, for the reason spelled out in the executor's
        // `AlterTable` arm: truncating the log here would delete the change history of the very
        // table this merge is about to publish rows into.
        let mut records: Vec<crate::wal::txn::DdlRecord> = Vec::new();
        for (i, plan) in plans {
            let table = schema_reports[i].table.clone();
            let (dir_root, tt_root) = {
                let entry = ctx.catalog.require_table(&table)?;
                (entry.first_directory_page_id, entry.time_travel_root)
            };
            // Read BEFORE the change, and from the plan rather than the catalog: a rename's old
            // name and a retype's old type live only in the shape the action was applied to, and
            // for every action after the first, that shape never exists in the catalog at all.
            let alterations = plan
                .steps()
                .map(|(action, before)| alteration_of(action, before))
                .collect::<Result<Vec<_>, FerroError>>()?;
            let shapes = ctx.catalog.apply_plan(plan, &ctx.txn)?;
            for (alteration, columns) in alterations.into_iter().zip(shapes) {
                records.push(crate::wal::txn::DdlRecord {
                    op: crate::wal::log::DdlOp::AlterColumn(alteration),
                    table: table.clone(),
                    dir_root,
                    time_travel_root: tt_root,
                    columns,
                });
            }
        }

        // **One flush and one run of feed records for the whole merge**, after every table's
        // rewrite has succeeded — rather than one of each per table, inside the loop above.
        //
        // It does not make a multi-table merge atomic; nothing short of logging the heap rewrite
        // can, for the reason given above the loop. What it does buy is that the CHANGE FEED never
        // carries half of one. A consumer rebuilding from the feed sees every table this merge
        // altered or none of them, where flushing and logging per table would have handed it table
        // X's new column and no word about table Y, permanently and with no later record to
        // reconcile it against.
        if !records.is_empty() {
            ctx.bp.flush_all()?;
            ctx.bp.disk_manager.sync()?;
            for record in records {
                ctx.txn.log_ddl(record)?;
            }
        }

        // **Reserve the version sequence BEFORE the rows become visible.**
        //
        // `record_applied` runs after `commit`, and it used to be where `apply_seq` advanced. That
        // left a window in which the published rows were readable while the clock still said they
        // did not exist: a scan landing there would take `observed_at = apply_seq + 1`, land at or
        // below the versions it had just read, and the temporal rule in
        // `DependencyGraphBuilder::build` would drop a real edge — a silently missing dependent,
        // which is the failure mode this whole lane is about.
        //
        // Reserving first inverts the error: in that window the clock is ahead of visibility, so a
        // scan that could NOT see the rows may still be named a dependent. Over-reporting is
        // recoverable — the operator sees a name in the halt tree and dismisses it — and
        // under-reporting cascades a revert through work that depended on the write.
        //
        // Not reachable through today's server, which serves one connection at a time
        // (`pgwire::serve`), so this closes a hole rather than fixing an observed failure. The
        // numbering is unchanged: the same ops, in the same order, get the same sequence values.
        //
        // **And the reservation is checked for freshness against an INDEPENDENT record before the
        // publish transaction opens.** The invariant that matters is the one the assertion in
        // `record_applied` describes and cannot test: no two versions ever share a `begin_ts`. That
        // assertion compares the reservation to a re-count of the same `rows` list the stamping loop
        // walks, so both sides are the same sum and no input can falsify it. `State::applied` can:
        // it is appended to once per stamped version, never pruned, and written by nobody but the
        // stamping loop, so it answers "has this number been handed out" without consulting the
        // arithmetic that produced the number. Refused here rather than asserted after `commit`,
        // because here the rows are not yet visible.
        //
        // **"Clean" now means clean of published rows, and not clean of everything.** This sits
        // after the schema apply above, so a refusal here returns with the schema edits already
        // durable. It is left here rather than hoisted above them deliberately: hoisting would
        // trade a refusal that can only fire if the no-two-versions-share-a-`begin_ts` invariant is
        // ALREADY broken for a leaked reservation on every ORDINARY refusal — and the schema
        // refusals above fire whenever an agent stages an edit its table cannot take, which is a
        // thing that happens.
        let reserved: std::ops::Range<u64> = {
            let mut state = self.state.lock().unwrap();
            let base = state.apply_seq;
            fresh_reservation(highest_applied_seq(&state.applied), base)
                .map_err(FerroError::Merge)?;
            state.apply_seq += rows.iter().map(|r| r.applied.len() as u64).sum::<u64>();
            base..state.apply_seq
        };

        // Publish every row in ONE transaction. Row-at-a-time commits would leave a merge that
        // failed halfway visible on the target, which is exactly the state a merge exists to
        // avoid: the report says the merge landed or it says it did not.
        //
        // The rows are `ready` rather than `pending`: already carried into the shape the schema
        // edits above left the table in, and already measured against it. `ALTER` is refused
        // inside a transaction, so the edits could never have run inside this one; they ran
        // before it opened.
        let publish_txn = ctx.txn.begin()?;
        // **Bind the run to the publishing transaction, so the LOG says who wrote these rows.**
        //
        // The loop below already stamps each version's author through `apply_in`, and
        // `apply_publish` records the *logical* row's author in the provenance store, which is
        // what `who_wrote_row` and `ferro_row_authors` read — but the provenance store is a
        // different artifact from the WAL, so a *reader of the log* has access to neither, and
        // since E79c that holds whether the store is in memory or on disk. `bind_run` appends a
        // `RecKind::RunIdentity` chained to this transaction, which is the only thing the logical
        // decoder can turn into a `writer` on a change event. Nothing called it, so every event on
        // the feed carried `"writer":null` no matter which agent produced it — the attribution B5
        // built was real and stopped at the process boundary.
        //
        // Skipped when the branch has no run: `ProvId::NONE` is the slot that MEANS unattributed,
        // and `bind_run` refuses it rather than writing an identity record that claims a writer and
        // names none. A plain SQL write has no run and must keep its honest null.
        if !snapshot.prov.is_none() {
            match self.provenance().lookup(snapshot.prov) {
                Ok(run) => {
                    if let Err(e) = ctx.txn.bind_run(publish_txn, run) {
                        ctx.txn.abort(publish_txn)?;
                        return Err(e);
                    }
                }
                Err(e) => {
                    ctx.txn.abort(publish_txn)?;
                    return Err(e);
                }
            }
        }
        let mut published = 0usize;
        for w in ready {
            // Crash point for D8. Inert in every normal run; see `crash_after_rows`.
            crash_after_rows(published);
            let author = Some((Arc::clone(self.provenance()), snapshot.prov));
            if let Err(e) = w.apply_in(ctx, publish_txn, author) {
                ctx.txn.abort(publish_txn)?;
                return Err(e);
            }
            published += 1;
        }
        ctx.txn.commit(publish_txn)?;

        self.record_applied(
            from,
            snapshot.txn,
            &rows,
            &snapshot,
            &merge_id,
            &images,
            reserved,
        )?;

        // **D103 — the merge is attested AFTER the publish transaction committed**, and that
        // order is the whole point. An entry appended before the commit would attest a merge that
        // a failed commit then never performed, which is worse than no attestation: it is a
        // tamper-evident record of something that did not happen.
        //
        // It commits to `images.post`, the row images this merge actually wrote — real content,
        // O(delta), and the one place on the branch lifecycle where a content commitment is
        // affordable. See the field docs on `AgentRuntime::attested` for why fork and commit get
        // no such commitment.
        self.attest_merge(into, self.branches.next_epoch(), &images);
        self.seal(from, true)?;

        Ok(MergeReport {
            blind_writes,
            merge_id,
            from,
            into,
            outcome,
            rows,
            schema: schema_reports,
            applied_to_target: true,
        })
    }

    /// Fingerprint of `tables` **by their change counters**, not by their contents — D69.
    ///
    /// # What this answers, and why a counter answers it
    ///
    /// The question is "did any of these tables move since this evaluation was scored?", asked
    /// once at scoring and once at publication and compared. It used to be answered by scanning
    /// every row of every listed table and hashing it. D68 measured that at 1.87 us/row — about
    /// 1.9 SECONDS per merge against a million-row table, to write four rows
    /// (`bench/d68_merge_is_o_table.txt`). Folding one `u64` per table is O(tables).
    ///
    /// # Why this is not weaker than the hash it replaces
    ///
    /// It is STRICTLY STRONGER, in the one direction that matters. A content hash cannot see a
    /// change that was reverted between the two observations — write, revert, hash matches, and
    /// the merge publishes against a base it never scored. `Catalog::bump_table_version` is
    /// monotone, so it catches that too. Every committed write bumps it, including ordinary DML
    /// outside any agent session, which is what `tests/d69_table_version.rs` pins.
    ///
    /// A table absent from the catalog contributes its id and nothing else, exactly as the scan
    /// version contributed no rows for it.
    fn fingerprint_tables(
        &self,
        ctx: &mut ExecCtx,
        tables: &BTreeMap<u32, String>,
    ) -> Result<u64, FerroError> {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for (t, name) in tables {
            h = fnv64_update(h, &t.to_be_bytes());
            if ctx.catalog.get_table(name).is_none() {
                continue;
            }
            h = fnv64_update(h, &ctx.catalog.table_version(*t).to_be_bytes());
        }
        Ok(h)
    }

    /// Fingerprint of the published version of every row an evaluation READ.
    ///
    /// `state.versions` is the map the read-premise check compares against, so folding it in is
    /// what makes that check's verdict part of what the staleness guard protects. It also catches
    /// a republication of byte-identical rows, which moves a version without moving any row image.
    fn fingerprint_premises(&self, rows: &[(TableId, RowId)]) -> u64 {
        let state = self.state.lock().unwrap();
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for (t, r) in rows {
            h = fnv64_update(h, &t.0.to_be_bytes());
            h = fnv64_update(h, &r.0.to_be_bytes());
            let v = state.versions.get(&(t.0, r.0)).map(|v| v.begin_ts).unwrap_or(0);
            h = fnv64_update(h, &v.to_be_bytes());
        }
        h
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
        // **D86: ask the index, do not rescan the log.**
        //
        // This filtered ALL of `state.applied` — a never-pruned Vec — on `tbl/row/col/seq`, once
        // per changed cell. Measured at 1.60x degradation across 400 merges, against a control
        // that moved the other way. The predicate's first three conjuncts ARE a key; only `seq`
        // is a range, and it is applied to the handful of entries the key selects.
        // **D86: binary search the range, do not scan the cell's history.**
        //
        // Indexing by `(tbl, row, col)` alone was NOT enough, and the measurement said so: drift
        // across 400 merges fell only 1.60x -> 1.25x. The reason is that an agent workload writes
        // the SAME cells over and over, so a cell's own history grows by one entry per merge and
        // `O(ops for this cell)` is the same order as `O(applied)` for the case that matters.
        //
        // The surviving filter is `seq > fork_seq` — a RANGE over a key that is already sorted,
        // because `push_applied` appends in increasing `seq` and therefore each cell's position
        // list is increasing in both position and seq. So the answer is a `partition_point`, and
        // the cost becomes O(log k) plus the entries actually returned.
        let at = state.applied_at_cell(tbl, row, col);
        let from = at.partition_point(|&i| {
            state.applied.get(i as usize).map(|a| a.seq <= fork_seq).unwrap_or(true)
        });
        let kinds: Vec<OpKind> = at[from..]
            .iter()
            .filter_map(|&i| state.applied.get(i as usize))
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
        images: &PublishedImages,
        reserved: std::ops::Range<u64>,
    ) -> Result<(), FerroError> {
        let mut state = self.state.lock().unwrap();
        let mut written: Vec<WriteRecord> = Vec::new();
        let mut next_seq = reserved.start;
        for r in rows {
            for op in &r.applied {
                // The sequence `merge` reserved before publishing, consumed in the same order it
                // was counted. `state.apply_seq` is not touched here: it already stands at the end
                // of this reservation.
                next_seq += 1;
                let seq = next_seq;
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
                state.push_applied(AppliedOp {
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
                // **The valued writes a scan's retained region is checked against.** One per COLUMN
                // of each image, because `PredicateSummary::covers` matches a write only against the
                // column its predicate names: a summary over `qty` cannot see a write recorded
                // against `id`, so recording only the op's own cell would leave every scan over
                // every other column blind. A write with no image left to name (a delete of a row
                // whose before-image we never held) still records the version itself, so the exact
                // read-after-write edge survives; it simply cannot answer a region query.
                let key = (op.tbl.0, op.row.0);
                let mut seen: Vec<(u32, Value)> = Vec::new();
                for img in [images.post.get(&key), images.pre.get(&key)].into_iter().flatten() {
                    for (idx, val) in img.iter().enumerate() {
                        let cell = (idx as u32, val.clone());
                        if seen.contains(&cell) {
                            continue;
                        }
                        seen.push(cell);
                        written.push(WriteRecord::new(
                            v,
                            Some(ColId(idx as u32)),
                            Some(val.clone()),
                        ));
                    }
                }
                if seen.is_empty() {
                    written.push(WriteRecord::new(v, op.col, None));
                }
                // Authorship of the published row, kept past `seal` AND past the process
                // (exit criterion 9). This is the write that makes `who_wrote_row` durable.
                self.prov_store
                    .stamp_row(op.tbl.0, op.row.0, snapshot.prov)?;
            }
        }
        // One `get_mut` after the loop rather than one per op: `TxnCapture` lives in the same
        // `State` the loop is mutating, and holding a mutable borrow of it across `apply_seq += 1`
        // does not borrow-check.
        // **What this assertion is, stated honestly, because it was oversold.** Both sides are the
        // same sum over the same `&[RowMergeOutcome]`: `reserved.end - reserved.start` is
        // `rows.iter().map(|r| r.applied.len()).sum()` from the one call site, and
        // `next_seq - reserved.start` counts iterations of the loop above over that same list, which
        // nothing mutates in between and which has no interior mutability. So **no input can
        // falsify it** — it is a structural invariant written as a runtime check, and it earns its
        // place only as a tripwire on a future edit that changes one of the two expressions without
        // the other. It is also a `debug_assert`, so it is absent from release builds.
        //
        // ITS BLIND SPOTS, both measured:
        //  * it is silent when a merge stamps N versions and publishes fewer rows — the reservation
        //    counts OPS while the publish loop iterates ROWS, and a two-column UPDATE reserves two
        //    slots for one published row, so the two quantities are genuinely different and this
        //    comparison is not between them;
        //  * it never runs at all when the publish fails, because `record_applied` is not reached.
        //
        // The property its old comment claimed — that no two versions share a `begin_ts` — is
        // guarded by `fresh_reservation` in `publish_evaluation_as`, against `State::applied`
        // rather than against a re-count of this loop's own list, and it refuses before publishing
        // rather than asserting afterwards.
        debug_assert_eq!(
            next_seq, reserved.end,
            "merge reserved versions {:?} but record_applied consumed {} of them",
            reserved,
            next_seq - reserved.start
        );
        // `or_insert_with` for the same reason as on the read path: a publish whose valued writes
        // go nowhere leaves cascade unable to see this merge at all, and it would look identical to
        // a merge that published nothing.
        let capture = state
            .captures
            .entry(txn.0)
            .or_insert_with(|| TxnCapture::new(txn, snapshot.prov, branch));
        for w in written {
            capture.on_write(w);
        }
        state.merges.insert(
            merge_id.to_string(),
            MergeRecord { branch, txns: vec![txn] },
        );
        Ok(())
    }

    // ---- ABANDON ---------------------------------------------------------------------------

    /// Forget the in-memory state of every branch this runtime holds that the catalog no longer
    /// does, returning how many were dropped.
    ///
    /// **The gap this closes.** `seal` is what removes a workspace, its `b_N` name and its escrow
    /// claim, and `seal` is reached only from a merge or an `ABANDON`. A branch reclaimed by the
    /// lease reaper is reclaimed *without client cooperation* — that is the whole point of the
    /// lease — so nothing here is told, and its workspace stays in the map for the life of the
    /// process. Its pages are gone; its bookkeeping is not. A server running simulations, where
    /// most branches end this way by design, would grow without bound.
    ///
    /// Escrow is released rather than settled: a branch the reaper took never published anything,
    /// so its claim goes back to the pool exactly as an abandoned branch's does.
    ///
    /// Safe to call at any time. A branch that is still live is left completely alone.
    ///
    /// **Not atomic, deliberately.** The walk releases the state lock every [`FORGET_CHUNK`]
    /// entries rather than holding it across the whole map, so this does not observe one
    /// consistent snapshot of `workspaces` and does not promise to: a session opened while it runs
    /// may or may not be examined, and either way it is not reap-eligible. What the return value
    /// counts is what this call actually removed. Callers that want a total are asking a question
    /// no reconciliation can answer, since the answer changes while it is being computed.
    pub fn forget_reaped_branches(&self) -> usize {
        let mut forgotten = 0usize;
        // Where the next chunk starts. `workspaces` is a `BTreeMap<BranchId, _>` ordered by
        // `(slot, generation)`, so the last key seen is a resumable cursor into it and a chunk
        // boundary needs nothing kept across the gap. `None` is "the start of the map"; an
        // EXCLUDED bound is what resumes, rather than key + 1, because there is no successor to
        // add to at the top of the generation space.
        let mut cursor: Option<BranchId> = None;
        loop {
            // ---- phase 1: ONE CHUNK of candidates, under the lock ---------------------------
            //
            // **Bounded on purpose.** This walk used to run to the end of the map in a single
            // acquisition, so the per-statement lock was held across O(open sessions) work and an
            // unrelated statement waited for all of it. Taking it `FORGET_CHUNK` at a time makes
            // the longest wait a function of the chunk, not of how many agents are connected.
            //
            // Releasing between chunks means the map can change under us, and both directions are
            // fine: a workspace opened behind the cursor is not reap-eligible (it was just forked)
            // and a workspace removed is one this sweep no longer has to remove.
            //
            // What a branch reaped after its own chunk's phase 2 waits for is the NEXT sweep, and
            // that is a real dependency rather than a free guarantee. This used to say "nothing is
            // missed permanently"; it is stronger than the call sites support. `scan_once` runs
            // the reconciliation only on its ERROR arm, and `simulate.rs` only when a simulation
            // starts, so a workspace missed here waits for one of those rather than for a timer.
            // Reaching the gap at all takes a second reaper running concurrently with this walk,
            // which is why it is a note and not a defect.
            let (chunk, last_seen, examined) = {
                let state = self.state.lock().unwrap();
                let mut chunk: Vec<BranchId> = Vec::with_capacity(FORGET_CHUNK);
                let mut last_seen: Option<BranchId> = None;
                let mut examined = 0usize;
                let lo = cursor.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                for (branch, _) in
                    state.workspaces.range((lo, std::ops::Bound::Unbounded)).take(FORGET_CHUNK)
                {
                    examined += 1;
                    last_seen = Some(*branch);
                    // **The key is the branch**, generation included, so there is nothing to
                    // recover. This used to look the generation up in `names` by `ws.name` and
                    // skip any workspace whose name had been rebound to another slot — a detour
                    // the slot-keyed map forced, which also meant a rebound name silently
                    // stranded its workspace here for ever. D158 item 1 removed the need for it.
                    chunk.push(*branch);
                }
                (chunk, last_seen, examined)
            };

            // ---- phase 2: ask the catalog, with the state lock NOT held ---------------------
            //
            // Not held because the catalog takes its own lock and the two are taken in the other
            // order elsewhere. It is also the expensive half in wall-clock terms, and it costs no
            // statement anything.
            let gone: Vec<BranchId> =
                chunk.into_iter().filter(|bid| self.branches.get(*bid).is_err()).collect();

            // ---- phase 3: forget them, under the lock ---------------------------------------
            if !gone.is_empty() {
                let mut state = self.state.lock().unwrap();
                for bid in &gone {
                    if forget_one_branch(&mut state, *bid) {
                        forgotten += 1;
                    }
                }
            }

            // A short chunk means the range ran out, which is the only end condition: `examined`
            // counts what was LOOKED AT, not what survived the filter, so a chunk that matched
            // nothing still advances rather than stopping the sweep early.
            if examined < FORGET_CHUNK {
                return forgotten;
            }
            // A full chunk always saw a last key, so this cannot lose the rest of the map; the
            // `None` arm is unreachable and returning is the safe direction if it ever is not.
            match last_seen {
                Some(last) => cursor = Some(last),
                None => return forgotten,
            }
        }
    }

    /// Forget exactly the branches a reaper says it took, rather than re-deriving the set by
    /// walking every open session. Returns how many were dropped.
    ///
    /// **Why this exists next to [`AgentRuntime::forget_reaped_branches`], which already does
    /// this.** The reconciliation answers "which of my workspaces has the catalog lost?", and the
    /// only way to answer that is to ask about all of them: O(open sessions), of which the catalog
    /// half is the expensive part. But `scan_once` already holds the exact answer — `reap_expired`
    /// hands it the list — and then throws it away and pays for the search anyway. That search ran
    /// inside the pgwire server's per-statement lock (`src/branch/lease_thread.rs`), so a timer
    /// stopped the whole database for as long as the walk took. Asking about `reaped.len()`
    /// branches instead is O(branches actually reaped), which is what a tick costs when nothing has
    /// gone wrong.
    ///
    /// Measured in `bench/w4/statement-lock-FASTPATH.txt`: the reconciliation's wall time
    /// rises 91x across 100x open sessions (269 us -> 24.5 ms at 10⁵) while this call shows no
    /// trend. A larger figure for the same walk is reported on branch S15-runtime-at-1e6 (commit
    /// 0ac1931, `bench/runtime_at_1e6.txt`, W4) — not in this worktree, and NOT reproduced here.
    ///
    /// **This is a fast path and NOT a replacement.** `reap_expired` can reap several branches and
    /// then return `Err`, discarding the ids it had already accumulated
    /// (`src/branch/reaper.rs`, the `Err(e) => return Err(e)` arm, and `sweep_empty_extents`
    /// after it) — so a caller that only ever forgets what it is told about would leak exactly the
    /// branches reaped before a failure. The reconciliation is what covers that, and the lease
    /// thread still runs it on precisely that path.
    ///
    /// Each branch is confirmed gone from the catalog before anything is dropped, so passing a
    /// live branch here does nothing rather than deleting a working agent's session.
    pub fn forget_branches(&self, reaped: &[BranchId]) -> usize {
        let mut forgotten = 0usize;
        // Chunked for the same reason the reconciliation is: `reaped` is unbounded in principle
        // (a simulation can expire thousands of candidate branches at once), and a lock hold that
        // is O(that) is the same wall this work exists to remove, reached by the other door.
        for slice in reaped.chunks(FORGET_CHUNK) {
            // The catalog, with the state lock NOT held -- the same order rule as phase 2 of the
            // reconciliation, for the same reason.
            let gone: Vec<BranchId> =
                slice.iter().copied().filter(|bid| self.branches.get(*bid).is_err()).collect();
            if gone.is_empty() {
                continue;
            }
            let mut state = self.state.lock().unwrap();
            for bid in gone {
                if forget_one_branch(&mut state, bid) {
                    forgotten += 1;
                }
            }
        }
        forgotten
    }

    /// Drop a branch and everything buffered on it.
    ///
    /// The buffered writes were never in the shared tables, so an abandoned agent task costs
    /// exactly one metadata record. This is the cooperative form of what the lease reaper does
    /// with no client cooperation at all.
    pub fn abandon(&self, branch: BranchId) -> Result<(), FerroError> {
        self.seal(branch, false)
    }

    /// Retire a branch. `published` says whether its writes reached the shared tables, which is
    /// what decides the fate of any escrow it holds.
    fn seal(&self, branch: BranchId, published: bool) -> Result<(), FerroError> {
        {
            let mut state = self.state.lock().unwrap();
            // The escrow outcome depends on whether the writes landed, and conflating the two was
            // a real hole: releasing a MERGED branch returns its spend to the pool as though the
            // resource had not been consumed, so five sequential agents each took 12 from a pool
            // of 20 and drove the counter to -40. Measured, not theorised.
            //
            // - published  -> settle: the resource really went, so the pool shrinks by what was spent
            // - abandoned  -> release: the writes never landed, so every unit goes back
            //
            // Either way the branch stops holding headroom, because an agent that dies with a
            // claim outstanding must not strand the resource for everyone else.
            if published {
                state.escrow.settle_all(branch);
            } else {
                state.escrow.release(branch);
            }
            // Record WHOSE writes just became readable, before the workspace goes. A merge
            // publishes this branch's staged rows and every ancestor's row it inherited at fork
            // time, so all of those captures are now the premises of published rows and none of
            // them may be dropped by a later abandon or reap.
            if published {
                if let Some(ws) = state.workspaces.get(&branch) {
                    let mut newly: Vec<u64> = ws.inherited.iter().map(|t| t.0).collect();
                    newly.push(ws.txn.0);
                    for t in newly {
                        state.published_txns.insert(t);
                    }
                }
            }
            if let Some(ws) = state.remove_workspace(&branch) {
                state.names.remove(&ws.name);
                // **A task that published nothing is not a dependent of anything.**
                //
                // Captures outlive the workspace on purpose, and that is right for a MERGE: it
                // retires the branch at the moment its rows become readable, so the graph has to
                // survive `seal` for the same reason `row_author` does. An ABANDON is the opposite
                // case. The buffered writes never landed, so there is nothing downstream to
                // protect — and keeping the capture made every scan the task ever ran block
                // reverts FOREVER.
                //
                // Measured before this: a ghost task scans `WHERE qty >= 20 AND qty < 50`, runs
                // `ABANDON`, and `REVERT MERGE m_1` reports `blocked_by = [TxnId(2)]`
                // permanently. `undo_txn` then finds no applied ops for it, so `CASCADE`
                // "reverts" a task that published nothing — which means the only way past the
                // name is the dangerous mode, training the operator away from the default that
                // exists to protect them. Over-reporting is the safe direction for a REGION; a
                // name with nothing behind it is not a region error, it is a dead entry.
                //
                // **But "published" here means THIS branch's seal came from a merge, and that is
                // not the same question as whether its staged writes reached the shared tables.**
                // A fork copies the parent's staged rows, so a child's `MERGE` publishes writes
                // its ancestors staged; the ancestor then seals through ABANDON or the reaper with
                // `published = false`, and dropping its capture deletes the read premise of a row
                // that is live in the shared tables right now. Measured before this:
                // `REVERT MERGE m1` reported `blocked_by = []` and went on to remove row 7, the
                // row that published row 9 was derived from. So the decision is delegated.
                if !published {
                    forget_captures_unless_published(&mut state, &ws);
                }
            }
        }
        // With a reaper attached, retiring a branch means reclaiming it: the reaper does
        // everything below AND frees the extents this branch allocated, which nothing else will.
        // It is the same call the lease scan makes, so a branch that is merged and a branch that
        // was walked away from end in exactly the same state.
        // **D103 — read the fork epoch BEFORE anything reaps.** The reap bumps the id slot's
        // generation, so `get(branch)` afterwards is a hard error and the epoch would be
        // unavailable exactly where the attestation needs it.
        let fork_epoch = self.branches.get(branch).ok().map(|r| r.fork_epoch);
        if let Some(reaper) = &self.reaper {
            reaper.reap(branch)?;
            // `with_reaper` cannot check that the reaper was built over this runtime's catalog —
            // the `Reaper` trait exposes no catalog to compare. What it CAN do is check the
            // result: after a successful reap, this runtime's own catalog must no longer hold the
            // branch as live. If it does, the reaper reaped a record nobody here wrote, and the
            // branch is now retired in name only with its pages still charged. Loud beats silent.
            if let Ok(rec) = self.branches.get(branch) {
                if rec.state != BranchState::Reaped {
                    return Err(FerroError::Branch(format!(
                        "the attached reaper reported success for {branch}, but this runtime's \
                         catalog still holds it as {:?}. The reaper is built over a different \
                         branch catalog than this runtime, so nothing it reaps belongs to these \
                         branches and their pages are still charged.",
                        rec.state
                    )));
                }
            }
            // **This arm has to attest too, and it did not.** `seal` returns early when a reaper
            // is attached, so an attestation placed only at the end of the fallback arm is
            // silently absent on every runtime built with `with_reaper` — which is the production
            // shape — and the gap reads exactly like "no branch was ever reaped": a missing entry
            // is indistinguishable from a lifecycle event that never happened. Found by reading
            // the control flow after the wiring was written; pinned by
            // `integration_branch_attestation::a_reap_through_an_attached_reaper_is_attested_too`,
            // which fails with `left: [Fork], right: [Fork, Reap]` when this call is removed.
            //
            // Two call sites rather than one because the alternative — restructuring the early
            // return — changes the control flow of the reap path itself, which is not a thing to
            // do as a side effect of adding an attestation.
            if let Some(epoch) = fork_epoch {
                self.attest_reap(branch, epoch, published);
            }
            return Ok(());
        }

        // Reap through the `BranchCatalog` trait only, so this works against the durable engine
        // as written: mark the record reaped (which bumps the generation, making the old id a
        // hard error) and drop our fork epoch from the parent's live-children array so the
        // parent's pages stop being pinned on our behalf.
        //
        // **D41 — both halves are now narrow catalog operations.** The parent's live set was
        // being edited as `get`/`remove_live_child`/`put`, which `BranchCatalog::detach_child`
        // exists to replace and says so at its own declaration: against a catalog that keeps
        // children in an INDEX the record's live set comes back empty, so `remove_live_child`
        // reported "nothing to do", the write was skipped, and the entry outlived its branch for
        // ever. The reap mark was the six-caller `get`/`mark_reaped`/`put` spelling.
        //
        // The `get(parent)` is kept as a guard rather than folded into `detach_child`, because it
        // is what scopes this to a parent that is still readable — the behaviour this path has
        // today. A reaped parent's stale entries are the reaper's business (`detach_from_parent`,
        // which cascades); this is the reaper-less fallback and does not take that on.
        let record = self.branches.get(branch)?;
        if let Some(parent) = record.parent_id {
            if self.branches.get(parent).is_ok() {
                self.branches.detach_child(parent.id, record.fork_epoch)?;
            }
        }
        self.branches.set_state(branch, record.state, BranchState::Reaped)?;
        // **D103 — attested after the state change lands**, for the same reason the merge entry is
        // appended after its commit: a record of a reap that did not happen is worse than none.
        // The entry seals this branch's chain, and it carries whether the branch's writes were
        // published, because "merged" and "abandoned" are different facts about a retired branch
        // and the record must not conflate them.
        //
        // The reaper arm above carries the same call; see the note there for why there are two.
        self.attest_reap(branch, record.fork_epoch, published);
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
            (rec.txns.clone(), rec.branch, dependency_graph_of(&state.captures))
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

    /// Undo one task's published writes.
    ///
    /// The versions this produces are deliberately **unattributed**. A revert is not a write by
    /// the agent whose work is being undone, and stamping it with that agent's `ProvId` would
    /// make the provenance query answer "this agent wrote this row" about a row the agent never
    /// wrote — the exact question criterion 9 exists to answer correctly.
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
                        .apply(ctx, None)?;
                }
                (OpKind::RowDelete, _) => {
                    let row = a.before_row.clone().ok_or_else(|| {
                        FerroError::Merge("cannot revert a delete with no before-image".into())
                    })?;
                    PendingWrite::Insert { table: a.table.clone(), row }.apply(ctx, None)?;
                }
                (kind, Some(col)) => {
                    let inverse = invert(kind, a.before.as_ref())?;
                    let rows = scan_table(&a.table, &ctx.read())?;
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
                    .apply(ctx, None)?;
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

/// **Everything a merge decided, against a base it did not touch.**
///
/// Produced by [`AgentRuntime::evaluate_merge`] and consumed by
/// [`AgentRuntime::publish_evaluation`]. Between those two calls the target is untouched, which is
/// what lets K candidate branches be scored against one identical base.
///
/// It is deliberately not `Clone`: an evaluation names a specific base state, and two copies of
/// one evaluation invite publishing it twice.
pub struct MergeEvaluation {
    pub from: BranchId,
    pub into: BranchId,
    /// The aggregate outcome this merge would report: `Clean`, `Commuting`, `Conflict` or
    /// `ResolvedWithLoss`.
    pub outcome: MergeOutcome,
    pub rows: Vec<RowMergeOutcome>,
    /// Rows the branch changed without ever reading. Reported, never decisive — see the gate.
    pub blind_writes: Vec<(TableId, RowId)>,
    /// The verdict of the one verification gate on the merge path.
    pub gate: GateOutcome,
    /// One result per declared assertion, in declaration order. Empty for a production `MERGE`,
    /// which declares none.
    pub assertions: Vec<AssertionResult>,
    /// The image this merge would leave for each row it touches; `None` where it would remove the
    /// row. Rows that conflict are absent, because a conflicting merge publishes nothing.
    produced: BTreeMap<(u32, u64), Option<Vec<Value>>>,
    /// Fingerprint of every row this evaluation read, so publishing can refuse a base that moved.
    base_fingerprint: u64,
    /// The tables that fingerprint covers.
    tables_read: BTreeMap<u32, String>,
    /// The rows this branch read by exact version, whose published versions the fingerprint also
    /// covers. A read-set is not a table the fingerprint can re-scan, so it is folded in directly.
    premise_rows: Vec<(TableId, RowId)>,
    snapshot: WorkspaceSnapshot,
    pending: Vec<PendingWrite>,
    /// What the target held for each row this merge will touch, as of evaluation. Publishing needs
    /// both ends of every move to derive predicate-derived dependency edges: `post` is what a later
    /// scan sees, `pre` is what it no longer sees, and an absence a merge caused is a dependency
    /// too. Captured here because `current` is evaluate-time state and `base_fingerprint` refuses a
    /// base that moved, which is what makes the evaluate-time image still correct at publish.
    pre_images: BTreeMap<(u32, u64), Vec<Value>>,
    /// One result per table whose shape this merge decided - B11's three-way schema merge, evaluated
    /// alongside every other admission check and applied by `publish_evaluation`.
    schema: Vec<SchemaMergeReport>,
}

impl MergeEvaluation {
    /// Would this merge be admitted? The gate passed **and** nothing conflicted.
    ///
    /// Both halves are load-bearing and neither implies the other: a conflicting merge can pass a
    /// gate that has nothing to say about it, and a clean merge can be stopped by the gate.
    pub fn is_admissible(&self) -> bool {
        self.gate.is_pass() && !self.outcome.is_conflict()
    }

    /// Assertions that held, over assertions declared. `None` when none were declared — a merge
    /// nothing asserted about has no score, which is not the same as a perfect one.
    pub fn score(&self) -> Option<f64> {
        if self.assertions.is_empty() {
            return None;
        }
        let held = self.assertions.iter().filter(|a| a.holds()).count();
        Some(held as f64 / self.assertions.len() as f64)
    }

    /// Every declared assertion that did not hold, by the predicate as it was written.
    pub fn failed_assertions(&self) -> Vec<String> {
        crate::agent_sql::gate::failed_assertions(&self.assertions)
    }

    /// The gate's verdict as the sentence a quarantine reason is written from.
    pub fn gate_reason(&self) -> String {
        let detail: Vec<String> = self
            .gate
            .findings()
            .iter()
            .map(|f| format!("[{:?} {}] {}: {}", f.tier, f.status, f.check, f.detail))
            .collect();
        format!("{} at merge admission — {}", self.gate.name(), detail.join("; "))
    }

    /// Every violated predicate this merge would report, verbatim.
    pub fn violated_predicates(&self) -> Vec<String> {
        self.rows.iter().flat_map(|r| r.violated_predicates()).collect()
    }

    /// The image this merge would leave for one row: `Some(None)` means it would delete the row,
    /// `None` means this merge does not touch it.
    pub fn produced_row(&self, tbl: TableId, row: RowId) -> Option<Option<&Vec<Value>>> {
        self.produced.get(&(tbl.0, row.0)).map(|v| v.as_ref())
    }

    /// The base state this evaluation was computed against, as one number.
    pub fn base_fingerprint(&self) -> u64 {
        self.base_fingerprint
    }

    /// Turn an evaluation that will NOT be published into the report for it.
    fn into_report(self, merge_id: String, applied_to_target: bool) -> MergeReport {
        MergeReport {
            merge_id,
            from: self.from,
            into: self.into,
            outcome: self.outcome,
            rows: self.rows,
            blind_writes: self.blind_writes,
            // An unpublished evaluation still reports what the schema merge DECIDED. A refused merge
            // whose schema half conflicted has to be able to say so, or the caller sees a row-level
            // report and no reason.
            schema: self.schema,
            applied_to_target,
        }
    }
}

/// The row a compiled assertion predicate refers to. Every row's cells are presented under this
/// id in turn; the real row id is carried alongside, by the loop that does the presenting.
const ASSERTION_PROBE_ROW: RowId = RowId(0);

/// Score declared assertions against the state a merge would leave behind.
///
/// The row set is the asserted table **as the merge would leave it**: the shared table now,
/// overlaid with the images this merge would produce, minus the rows it would delete. Scoring only
/// the rows the candidate touched was the first shape and it is weaker in a way that matters — a
/// candidate that changes nothing would then satisfy every assertion by touching nothing.
///
/// Each row is checked through [`Guard::check`], which is what `check_guards` calls for the
/// production merge's own guards, so an assertion and a guard cannot disagree about what a
/// predicate means.
///
/// The predicate is compiled **once**, against a fixed probe row, and each row's cells are then
/// presented under that same probe id — a `GuardExpr` bakes the row it refers to, and compiling one
/// per row cost a full parse-and-bind for every row of the table on every candidate, twice per
/// simulation. The row a violation names comes from the loop, not from the guard.
fn evaluate_assertions(
    assertions: &[Assertion],
    schemas: &BTreeMap<u32, Schema>,
    current: &BTreeMap<(u32, u64), Vec<Value>>,
    produced: &BTreeMap<(u32, u64), Option<Vec<Value>>>,
) -> Vec<AssertionResult> {
    let mut out = Vec::with_capacity(assertions.len());
    for a in assertions {
        let tbl = table_id(&a.table);
        let source = a.source();
        let mut result = AssertionResult {
            source: source.clone(),
            table: a.table.clone(),
            tbl,
            rows_checked: 0,
            violations: Vec::new(),
            unevaluable: Vec::new(),
        };
        let Some(schema) = schemas.get(&tbl.0) else {
            // Not "no rows, therefore true": a predicate over a table this database does not have
            // could not be evaluated, and the gate hard-rejects that rather than passing it.
            result.unevaluable.push((RowId(0), format!("unknown table: {}", a.table)));
            out.push(result);
            continue;
        };

        // Borrowed, not cloned: the row images live in `current` and `produced` for the whole call.
        let mut rows: BTreeMap<u64, &Vec<Value>> = current
            .iter()
            .filter(|((t, _), _)| *t == tbl.0)
            .map(|((_, r), row)| (*r, row))
            .collect();
        for ((t, r), image) in produced {
            if *t != tbl.0 {
                continue;
            }
            match image {
                Some(row) => {
                    rows.insert(*r, row);
                }
                None => {
                    rows.remove(r);
                }
            }
        }

        let guard = match guard_from_expr(&a.predicate, tbl, ASSERTION_PROBE_ROW, schema) {
            Ok(g) => g,
            Err(e) => {
                result.unevaluable.push((RowId(0), e.to_string()));
                out.push(result);
                continue;
            }
        };
        result.rows_checked = rows.len();
        for (rid, row) in rows {
            let row_id = RowId(rid);
            let mut state = CellState::new();
            for (idx, v) in row.iter().enumerate() {
                state.set(tbl, ASSERTION_PROBE_ROW, ColId(idx as u32), v.clone());
            }
            match guard.check(&state) {
                Ok(true) => {}
                Ok(false) => result.violations.push((row_id, cells_text(&guard, schema, row))),
                // An error is NOT a violation: the predicate could not be decided on this row, and
                // the gate hard-rejects that rather than treating it as a failure the caller can
                // retry against.
                Err(e) => result.unevaluable.push((row_id, e.to_string())),
            }
        }
        out.push(result);
    }
    out
}

/// The cells a predicate referred to, rendered for the caller who has to act on the violation.
///
/// `qty >= 0` failing is not actionable on its own; `qty >= 0 (qty = -4)` is.
fn cells_text(guard: &Guard, schema: &Schema, row: &[Value]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for (_, _, col) in guard.expr.referenced_cells() {
        let idx = col.0 as usize;
        let name = schema.columns.get(idx).map(|c| c.name.clone()).unwrap_or_else(|| format!("col{idx}"));
        if parts.iter().any(|p| p.starts_with(&format!("{name} = "))) {
            continue;
        }
        match row.get(idx) {
            Some(v) => parts.push(format!("{name} = {}", cell_text(v))),
            None => parts.push(format!("{name} = <absent>")),
        }
    }
    parts.join(", ")
}

/// One cell as text. Deliberately not a float rendering of a decimal: the digits are the value.
///
/// **This is the fourth copy of this match in the tree** — `cli::display_value`,
/// `optimizer::format_value` and `tel::guard::write_value` are the others, each private to its
/// module, and the Decimal comment has already been copy-pasted between them. The right fix is one
/// `impl Display for Value` in `catalog::column` and four deletions; it is not done here because
/// three of those four files belong to other work in flight, and a fifth copy is cheaper to delete
/// later than a merge conflict is to resolve now.
fn cell_text(v: &Value) -> String {
    match v {
        Value::Boolean(b) => b.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Varchar(s) => s.clone(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Decimal(d) => d.clone(),
        Value::Timestamp(ms) => ms.to_string(),
        Value::Null => "NULL".to_string(),
    }
}

/// Every row an evaluation read by exact version, sorted and de-duplicated so the fingerprint over
/// them is stable. A scan-shaped read retains a predicate summary rather than versions, and the
/// read-premise check already downgrades itself to a heuristic for those; there is nothing here to
/// name.
fn premise_rows_of(reads: &[crate::provenance::readset::ReadSet]) -> Vec<(TableId, RowId)> {
    let mut out: Vec<(TableId, RowId)> = Vec::new();
    for rs in reads {
        if let crate::provenance::readset::ReadSet::ExactVersions(versions) = rs {
            out.extend(versions.iter().map(|v| (v.tbl, v.row)));
        }
    }
    out.sort_by_key(|(t, r)| (t.0, r.0));
    out.dedup();
    out
}

struct WorkspaceSnapshot {
    txn: TxnId,
    /// The run behind this task, carried into the merge so authorship outlives the workspace.
    prov: ProvId,
    fork_seq: u64,
    rows: PersistentMap<(u32, u64), RowState>,
    base_rows: PersistentMap<(u32, u64), Option<Vec<Value>>>,
    tables: PersistentMap<u32, String>,
    ops: Vec<Op>,
    guards: Vec<Guard>,
    reads: Vec<crate::provenance::readset::ReadSet>,
    schema_edits: Arc<Vec<(String, SchemaEdit)>>,
    base_shapes: PersistentMap<String, Schema>,
}

/// A cell's value as an integer, for escrow accounting. `None` for anything not numeric.
///
/// Floats are truncated toward zero deliberately: escrow counts whole units of a bounded resource,
/// and a fractional claim is not a thing the ledger models. Truncation rounds a decrease DOWN,
/// which under-charges by at most one unit rather than letting a fractional write escape entirely.
fn numeric(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(i) => Some(*i as i64),
        Value::Float(f) if f.is_finite() => Some(*f as i64),
        _ => None,
    }
}

/// The row-width refusal, told what it means inside a `MERGE`.
///
/// `catalog::alter`'s message ends "Narrow the row first ... and run the ALTER again". That is
/// complete advice for a statement and incomplete advice for a merge, and the difference is not
/// cosmetic: a merge decides its schema edits against the target **as it stands when the merge
/// begins**, so a narrowing `UPDATE` this same merge is carrying has not reached the target yet and
/// re-running the merge refuses identically, forever. The narrowing has to land in a merge of its
/// own first.
///
/// That is a consequence of deciding before writing rather than an oversight, and it is the price
/// of the whole row: the heap rewrite is unlogged, so a merge's schema edits have to be decided
/// against a heap that is not moving underneath them, and the only heap that is not moving is the
/// one before this merge publishes anything. Publishing first and deciding after is what E82 was.
///
/// Recognised through the marker `catalog::alter` exports rather than by matching its prose, and
/// every other error is passed through untouched.
fn in_a_merge_the_narrowing_comes_first(e: FerroError) -> FerroError {
    // Destructured rather than stringified: `FerroError::Constraint` Displays with a
    // "constraint error: " prefix, so wrapping `e.to_string()` in another `Constraint` produces a
    // refusal that says it twice.
    let FerroError::Constraint(msg) = e else { return e };
    if !msg.contains(NARROW_THE_ROW_FIRST) {
        return FerroError::Constraint(msg);
    }
    FerroError::Constraint(format!(
        "{msg} Inside a MERGE the narrowing has to reach the target first: merge the UPDATE that \
         shortens the row, then merge the schema edit. This merge's own rows have not been \
         published yet — a merge decides its schema edits against the target as it stands when it \
         begins, because the heap rewrite is unlogged and cannot be decided against a heap that is \
         still moving."
    ))
}

/// Fault injection for crash-safety testing (D8). **Inert unless `FERRODB_CRASH_AFTER_ROWS` is
/// set**, and read once rather than per row.
///
/// `abort()` and not `exit()`: no destructors, no Rust-level flush, no chance for anything to tidy
/// up on the way out. That models a process being killed.
///
/// What it does **not** model is power loss. Bytes already handed to `write()` sit in the OS page
/// cache and outlive the process, so this exercises the recovery path against a dead process, not
/// against a dead machine. Saying otherwise would be claiming a durability property nothing here
/// tested.
fn crash_after_rows(published: usize) {
    use std::sync::OnceLock;
    static POINT: OnceLock<Option<usize>> = OnceLock::new();
    let point = POINT.get_or_init(|| {
        std::env::var("FERRODB_CRASH_AFTER_ROWS").ok().and_then(|v| v.parse::<usize>().ok())
    });
    if *point == Some(published) {
        std::process::abort();
    }
}

/// Rows a branch changed **without ever looking at them** — DESIGN.md section 4's cheap metric.
///
/// Nobody else in the data-quality literature has this, for the mundane reason that nobody else
/// retains read-sets. It costs one set difference and has no threshold to tune.
///
/// **Deliberately biased toward precision over recall**, because the metric is only useful if a
/// hit means something. A predicate read (a range or a full scan) suppresses reporting for the
/// whole table, not just the rows inside the range: computing exact range membership would need
/// the row's column value at read time, and guessing it would manufacture false positives. So a
/// blind write reported here really is one; some real ones are missed when the agent scanned the
/// same table for an unrelated reason. Under-reporting is the safe direction — an operator who
/// stops trusting the metric gets nothing from it.
fn blind_writes_of(
    rows: &PersistentMap<(u32, u64), RowState>,
    reads: &[crate::provenance::readset::ReadSet],
) -> Vec<(TableId, RowId)> {
    use crate::provenance::readset::ReadSet;
    let mut looked_at: BTreeSet<(u32, u64)> = BTreeSet::new();
    let mut scanned_tables: BTreeSet<u32> = BTreeSet::new();
    for r in reads {
        match r {
            ReadSet::ExactVersions(vs) => {
                for v in vs {
                    looked_at.insert((v.tbl.0, v.row.0));
                }
            }
            ReadSet::Predicate(p) => {
                scanned_tables.insert(p.tbl.0);
            }
        }
    }
    rows.keys()
        .filter(|(t, r)| !looked_at.contains(&(*t, *r)) && !scanned_tables.contains(t))
        .map(|(t, r)| (TableId(*t), RowId(*r)))
        .collect()
}

/// The highest version sequence any merge has already handed out, read from the record of what was
/// applied rather than from the counter that produced it.
///
/// `State::applied` is the independent record: appended to once per stamped version, never pruned,
/// and written by nobody but `record_applied`'s stamping loop. Asking it means the freshness check
/// does not consult `apply_seq`, which is the arithmetic under suspicion — the check this replaced
/// compared the reservation to a re-count of the SAME list the stamping loop walks, so no input
/// could falsify it.
///
/// `max` rather than `last`, deliberately: if the invariant being checked is already broken, the
/// final element is not necessarily the largest. One pass per merge, the same order of cost
/// `undo_txn` already pays per revert over the same vector.
fn highest_applied_seq(applied: &[AppliedOp]) -> Option<u64> {
    applied.iter().map(|a| a.seq).max()
}

/// Refuse a reservation that would re-issue a version sequence already handed out.
///
/// `record_applied` stamps `base + 1 ..= base + n`, so freshness is exactly `base >= highest`.
///
/// Two versions sharing a `begin_ts` mis-answer every visibility comparison downstream, including
/// `DependencyGraphBuilder::build`'s `begin_ts < observed_at` — which is the rule that decides
/// whether a scan depended on a write, and therefore the whole of exit criterion 10. A reservation
/// that STARTS above the high water mark is fine and is not refused: a range leaked by a failed
/// publication only ever shifts later versions upward, which over-reports rather than corrupts.
fn fresh_reservation(highest: Option<u64>, base: u64) -> Result<(), String> {
    match highest {
        Some(h) if base < h => Err(format!(
            "refusing to publish: this merge would stamp version sequences from {} while {} has \
             already been handed out. Two versions sharing a begin_ts mis-answer every visibility \
             comparison downstream, including the one that decides whether a scan depended on a \
             write.",
            base + 1,
            h
        )),
        _ => Ok(()),
    }
}

/// Why a read was taken, which is what decides whether it counts as an INSPECTION.
///
/// **Causality and inspection are different questions, and this is the one place they part.** Both
/// purposes retain the region for the causal graph, because both really did decide what got written.
/// Only `Inspection` reaches the read-set builder, and therefore only `Inspection` moves
/// `blind_writes` and `ReadPremiseCheck`.
///
/// `UPDATE ... WHERE id = 7` is the case that forces the distinction. It causally depends on row 7 —
/// a revert of whatever published that row has a dependent to name — and it inspected no *value*: it
/// addressed the row. Counting it as an inspection would stop every `UPDATE ... WHERE <pk> = <lit>`
/// from being a blind write, which is the entire shape DESIGN.md section 4's metric exists to catch,
/// and would downgrade `ReadPremiseCheck` to `Heuristic` for a branch that named exact versions and
/// nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadPurpose {
    /// A `SELECT`, or a write whose `WHERE` clause compared a VALUE: the task looked.
    Inspection,
    /// A write statement's `WHERE` clause that only addressed rows by key: the task did not look.
    RowTargeting,
}

/// The images a merge moved its published rows between. See where it is built in `merge`.
#[derive(Debug, Default)]
struct PublishedImages {
    /// What the target held immediately before this merge published.
    pre: BTreeMap<(u32, u64), Vec<Value>>,
    /// What it holds after. Absent for a delete.
    post: BTreeMap<(u32, u64), Vec<Value>>,
}

/// The dependency graph over everything every task retained — exact and predicate-derived alike.
///
/// **One derivation, and it lives in `ProvenanceLog::dependency_graph`.** That function is the only
/// code in the tree that composes read-after-write edges over exact versions with the edges derived
/// by re-evaluating `PredicateSummary::covers` against published values; the runtime deliberately
/// does not keep a second copy of it, which is the reconciliation this lane exists to make.
/// Drop the captures of a workspace that is going away WITHOUT having published anything -- and of
/// any ancestor whose protection lapsed at the same moment.
///
/// F6's rule is right: a task that published nothing is not a dependent of anything, and keeping its
/// capture made every scan it ever ran block reverts forever. What F6 got wrong is WHO published.
/// `seal`'s `published` flag answers "did this branch's own seal come from a merge", while the
/// question a capture's lifetime turns on is "did the writes this capture is the premise for reach
/// the shared tables". Those differ whenever a fork is involved, because a fork snapshots the
/// parent's staged rows: the child's `MERGE` publishes them and the parent then seals through
/// ABANDON or the reaper with `published = false`.
///
/// So a capture survives while any of these holds:
///
/// - its own writes were published (`published_txns`, recorded by `seal` at the publish);
/// - a descendant already published writes it staged (also `published_txns`, because `seal` records
///   the whole inherited chain, not just the merging branch);
/// - a LIVE workspace still carries its staged writes and could publish them yet.
///
/// Ancestors are considered deepest-first: an ancestor's protection can only lapse once the
/// descendant that was holding it is gone, and the caller has already removed `ws` from the map.
fn forget_captures_unless_published(state: &mut State, ws: &Workspace) {
    let mut candidates: Vec<TxnId> = ws.inherited.clone();
    candidates.push(ws.txn);
    for txn in candidates.into_iter().rev() {
        if capture_is_protected(state, txn) {
            continue;
        }
        state.captures.remove(&txn.0);
    }
}

/// Drop one branch's in-memory state, if the runtime still holds exactly that branch. `true` when
/// something was dropped. The caller holds the state lock.
///
/// **It cannot touch anything but this exact branch, because the map key IS this exact branch.**
/// The catalog releases a reaped branch's id SLOT for reuse and bumps the generation, so a new
/// session forking into slot 5 takes the same `b_5` name. Acting on a stale answer would then
/// delete a LIVE agent's workspace, release its escrow and unbind its name, for a branch the
/// catalog had never been asked about.
///
/// This used to be a re-validation written here — `workspaces.get(&id)` followed by a comparison
/// of that workspace's name against `names` — because `workspaces` was keyed by the slot alone
/// and this was the one path that had taken the argument. D158 item 1 keyed the map by the whole
/// `BranchId`, which turns the check into a miss: a recycled slot is a different key. The name is
/// removed only when the workspace was ours, which is exactly when the slot cannot have been
/// recycled — `insert_workspace` evicts a slot's previous occupant, so a surviving key at `bid`
/// is proof no fork took the slot (the invariant stated on [`State::workspaces`]). One authority,
/// not two: a second guard on `names` here would mask every mutant of the key.
///
/// Shared by the reconciliation and by [`AgentRuntime::forget_branches`] so that the fast path
/// cannot drift from the backstop: the two differ in which branches they consider, and in nothing
/// else.
fn forget_one_branch(state: &mut State, bid: BranchId) -> bool {
    // Reaped without client cooperation, so nothing this branch buffered was published BY IT: the
    // same reasoning as the ABANDON arm of `seal`, and reached by a different door. A descendant
    // may still have published what this branch staged, so the same helper decides.
    let Some(ws) = state.remove_workspace(&bid) else {
        // The slot was recycled under us, or another door already forgot this branch: the
        // workspace, the name and the quarantine reason all belong to the LIVE branch now and
        // none of them may be touched. The escrow claim is the exception and must still go. It is
        // keyed by the whole `BranchId` (see `Pool::claimed`), so releasing names the dead
        // generation and only the dead generation — the reborn branch's own claims are a
        // different key and are untouched. Skipping it outright left a reaped branch holding pool
        // headroom with nothing alive that could ever give it back, which is exactly the
        // resource-stranding the ABANDON arm of `seal` releases to prevent.
        state.escrow.release(bid);
        return false;
    };
    forget_captures_unless_published(state, &ws);
    state.names.remove(&ws.name);
    state.escrow.release(bid);
    state.quarantine_reasons.remove(&bid.id);
    true
}

/// Is anything still relying on `txn`'s capture -- a publish that happened, or one that still could?
///
/// The second half reads [`State::txn_refs`] rather than scanning `workspaces`. The scan was
/// O(open sessions) and ran under the per-statement lock once per branch being forgotten; see that
/// field for the measurement.
///
/// The `debug_assert` re-derives THIS ONE answer the slow way — but it only runs where this
/// predicate does, which is the abandon and reap arms and nowhere else: a fork or a published merge
/// never reaches here. Coverage of the insert path comes from [`State::audit_txn_refs`], not from
/// this line. Saying otherwise was an overstatement this comment used to make.
fn capture_is_protected(state: &State, txn: TxnId) -> bool {
    let indexed = state.txn_refs.contains_key(&txn.0);
    debug_assert_eq!(
        indexed,
        state.workspaces.values().any(|w| w.txn == txn || w.inherited.contains(&txn)),
        "txn_refs disagrees with a scan of workspaces about txn {txn:?}"
    );
    state.published_txns.contains(&txn.0) || indexed
}

fn dependency_graph_of(captures: &BTreeMap<u64, TxnCapture>) -> DependencyGraph {
    let mut log = ProvenanceLog::new();
    for c in captures.values() {
        log.record(c.clone().finish());
    }
    log.dependency_graph()
}

/// The region a range or full scan looked at, retained so that a write landing inside it later is a
/// phantom the read depended on.
///
/// **The bounds are narrowed only when the clause proves a narrowing**, and that asymmetry is the
/// whole safety argument. A conjunction of literal comparisons over one column gives a real
/// interval; anything else — an `OR`, a `!=`, a column compared to a column, no `WHERE` at all —
/// keeps the region unbounded over the table. A region WIDER than the truth names a dependent that
/// may not be one, which an operator sees in the halt tree and can dismiss; a region NARROWER than
/// the truth silently cascades a revert through work that did depend on it. Only one of those two
/// errors is recoverable, so only one of them is allowed.
///
/// The `residual` keeps the clause verbatim either way, so a re-check is always possible.
fn predicate_summary(
    tbl: TableId,
    where_clause: Option<&Expr>,
    bound: Option<&BoundExpr>,
    rows_observed: u64,
) -> PredicateSummary {
    let (col, lo, hi) = match bound.and_then(column_range) {
        Some((c, lo, hi)) => (Some(ColId(c as u32)), lo, hi),
        None => (None, Bound::Unbounded, Bound::Unbounded),
    };
    PredicateSummary { tbl, col, lo, hi, residual: where_clause.map(|w| w.to_sql()), rows_observed }
}

/// `(column, lo, hi)` for a clause that bounds ONE column with literals; `None` otherwise.
///
/// Derived from the BOUND expression rather than the parsed one: the binder has already resolved the
/// column to its index, coerced the literal to the column's type and refused a cross-category
/// comparison, so the values compared here are the values the executor compared. Re-deriving that
/// from `Expr` would be a second, weaker copy of the binder's type rules.
fn column_range(e: &BoundExpr) -> Option<(usize, Bound, Bound)> {
    match e {
        BoundExpr::BinaryOp { left, operator: TokenType::And, right } => {
            match (column_range(left), column_range(right)) {
                // Both conjuncts bound the same column: the region is their intersection.
                (Some((lc, llo, lhi)), Some((rc, rlo, rhi))) if lc == rc => {
                    Some((lc, llo.tighter_lo(rlo), lhi.tighter_hi(rhi)))
                }
                // Two different columns, or one side unusable: keep one side and DROP the other
                // conjunct. Dropping a conjunct widens the region, which is the safe direction; a
                // summary has one column and cannot express both.
                (Some(l), _) => Some(l),
                (None, r) => r,
            }
        }
        // An `OR` is a union, and a union of two intervals is not an interval. Reporting either arm
        // would understate the region, so the whole clause falls back to unbounded.
        BoundExpr::BinaryOp { left, operator, right } => comparison_range(left, *operator, right),
        _ => None,
    }
}

fn comparison_range(
    left: &BoundExpr,
    operator: TokenType,
    right: &BoundExpr,
) -> Option<(usize, Bound, Bound)> {
    let (col, v, op) = match (left, right) {
        (BoundExpr::Column(c), BoundExpr::Literal(v)) => (*c, v.clone(), operator),
        // `20 <= qty` is `qty >= 20`. Mirroring rather than refusing matters because refusing here
        // is not neutral: it widens the region to the whole table for a clause that was perfectly
        // precise, just written the other way round.
        (BoundExpr::Literal(v), BoundExpr::Column(c)) => (*c, v.clone(), mirror(operator)?),
        _ => return None,
    };
    let (lo, hi) = match op {
        TokenType::Equal => (Bound::Included(v.clone()), Bound::Included(v)),
        TokenType::Less => (Bound::Unbounded, Bound::Excluded(v)),
        TokenType::LessEqual => (Bound::Unbounded, Bound::Included(v)),
        TokenType::Greater => (Bound::Excluded(v), Bound::Unbounded),
        TokenType::GreaterEqual => (Bound::Included(v), Bound::Unbounded),
        // `!=` is the complement of a point: two intervals, and a single one could only over- or
        // under-state it. Under-stating is not allowed, so it stays unbounded.
        _ => return None,
    };
    Some((col, lo, hi))
}

/// The same comparison with its operands swapped.
fn mirror(operator: TokenType) -> Option<TokenType> {
    Some(match operator {
        TokenType::Equal => TokenType::Equal,
        TokenType::Less => TokenType::Greater,
        TokenType::LessEqual => TokenType::GreaterEqual,
        TokenType::Greater => TokenType::Less,
        TokenType::GreaterEqual => TokenType::LessEqual,
        _ => return None,
    })
}

/// Who to attribute the versions a write produces to. `None` leaves them `ProvId::NONE`.
type Author = Option<(Arc<dyn ProvenanceStore>, ProvId)>;

/// A write the merge will publish to the shared tables, expressed as ordinary SQL so it goes
/// through the same executor, WAL and indexes as any other write.
enum PendingWrite {
    Insert { table: String, row: Vec<Value> },
    Update { table: String, schema: Schema, key: Value, row: Vec<Value>, before: Vec<Value> },
    Delete { table: String, key: Value },
}

impl PendingWrite {
    /// The table this write lands on.
    fn table(&self) -> &str {
        match self {
            PendingWrite::Insert { table, .. }
            | PendingWrite::Update { table, .. }
            | PendingWrite::Delete { table, .. } => table,
        }
    }

    /// Carry this write from the shape the target had when the merge was scored (`from`) into the
    /// shape it will have when the write lands (`to`), and refuse it if the result does not fit a
    /// page.
    ///
    /// **Both halves belong together, and both belong before anything is written.** A merge that
    /// alters a table publishes into the ALTERED shape, so an image scored against the old one has
    /// the wrong arity and possibly the wrong type — and `Tuple::serialize` would say so from
    /// inside the publish transaction, after the schema edits had already landed. The width is the
    /// same story one step further on: `INTEGER -> BIGINT` moves every column after the retyped one
    /// and an appended column costs at least the bytes the null bitmap grows by, so a row that fit
    /// when it was scored can fail to fit when it lands. Measured here, a merge that cannot publish
    /// its rows refuses before it has changed anything; measured at insertion, it refuses after.
    ///
    /// `from == to` — the ordinary merge, which alters nothing — makes `conform_row` a copy and the
    /// width check a serialization the executor was about to do anyway.
    ///
    /// A delete carries nothing. It names a primary key, and a primary key's type cannot be
    /// altered (`catalog::alter::resulting_schema` refuses it), so the value it names means the
    /// same thing under both shapes. The key's column NAME can change, and `into_stmt` reads that
    /// from the catalog at the moment it builds the statement — which is after the rename landed.
    fn conform_to(self, from: &Schema, to: &Schema) -> Result<PendingWrite, FerroError> {
        match self {
            PendingWrite::Insert { table, row } => {
                let row = conform_row(&row, from, to)?;
                let which = format!("the row this merge inserts with key {:?}", row.first());
                refuse_if_the_row_cannot_land(&table, to, &row, &which)?;
                Ok(PendingWrite::Insert { table, row })
            }
            PendingWrite::Update { table, schema: _, key, row, before } => {
                let row = conform_row(&row, from, to)?;
                let before = conform_row(&before, from, to)?;
                let which = format!("the row this merge updates with key {key:?}");
                refuse_if_the_row_cannot_land(&table, to, &row, &which)?;
                // The landing shape, not the scored one: `into_stmt` builds the `UPDATE`'s
                // assignments from these column names, and after a rename the scored shape names a
                // column the table no longer has.
                Ok(PendingWrite::Update { table, schema: to.clone(), key, row, before })
            }
            PendingWrite::Delete { table, key } => Ok(PendingWrite::Delete { table, key }),
        }
    }

    /// `(table, row)` this write lands on, or `None` for a write with no row image to key by.
    fn row_key(&self) -> Option<(u32, u64)> {
        match self {
            PendingWrite::Insert { table, row } | PendingWrite::Update { table, row, .. } => {
                Some((table_id(table).0, row_id_of(row).0))
            }
            // A delete names the key rather than the row, and `row_id_of` takes the primary key as
            // the first column of a row — which is exactly what `key` is.
            PendingWrite::Delete { table, key } => {
                Some((table_id(table).0, row_id_of(std::slice::from_ref(key)).0))
            }
        }
    }

    /// The image this write leaves behind, for the rows that still have one. `None` for a delete:
    /// what a scan can depend on there is the row's absence, which is answered from the `pre` image.
    fn published_image(&self) -> Option<((u32, u64), Vec<Value>)> {
        match self {
            PendingWrite::Insert { row, .. } | PendingWrite::Update { row, .. } => {
                self.row_key().map(|k| (k, row.clone()))
            }
            PendingWrite::Delete { .. } => None,
        }
    }

    /// Publish inside an already-open transaction.
    fn apply_in(self, ctx: &mut ExecCtx, txn_id: u64, author: Author) -> Result<usize, FerroError> {
        let stmt = self.into_stmt(ctx)?;
        let stmt = match stmt {
            Some(s) => s,
            None => return Ok(0),
        };
        apply_dml_in(stmt, ctx, txn_id, author)
    }

    /// Publish in a transaction of its own.
    fn apply(self, ctx: &mut ExecCtx, author: Author) -> Result<usize, FerroError> {
        let stmt = match self.into_stmt(ctx)? {
            Some(s) => s,
            None => return Ok(0),
        };
        apply_dml(stmt, ctx, author)
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
fn apply_dml(stmt: Stmt, ctx: &mut ExecCtx, author: Author) -> Result<usize, FerroError> {
    let txn_id = ctx.txn.begin()?;
    match apply_dml_in(stmt, ctx, txn_id, author) {
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
fn apply_dml_in(
    stmt: Stmt,
    ctx: &mut ExecCtx,
    txn_id: u64,
    author: Author,
) -> Result<usize, FerroError> {
    let snapshot = ctx.txn.snapshot_of(txn_id)?;
    let view = Arc::new(ReadView { snapshot: Arc::new(snapshot), txn_id });
    match plan(stmt, ctx.catalog, ctx.bp.clone(), Some((ctx.txn.clone(), txn_id)), view)? {
        Plan::Write(mut op) => {
            if let Some((prov, id)) = author {
                op.set_author(prov, id);
            }
            op.execute(ctx.catalog)
        }
        Plan::Read(_) => Err(FerroError::Bind("expected a write plan".into())),
    }
}

/// Every row of a table as the shared (merged) state has it.
/// Every row of `table`, unfiltered. The callers that diff, merge and sweep want exactly that.
pub fn scan_table(table: &str, ctx: &ReadCtx) -> Result<Vec<Vec<Value>>, FerroError> {
    scan_table_where(table, None, None, ctx)
}

/// The rows of `table` that satisfy `where_clause`, with the predicate PUSHED INTO THE PLANNER.
///
/// # Why this exists
///
/// `scan_table` built `SELECT * FROM t` with `where_clause: None` and materialised every row,
/// and `visible_rows` only applied the statement's own `WHERE` afterwards. So an agent-session
/// `SELECT … WHERE id = k` read the ENTIRE table to keep one row — O(table) per statement,
/// where the plain path for the identical query is O(log N) through the B+tree. D55 measured
/// the gap on this box before this change; `bench/d55_*` holds the numbers. This is the same
/// shape as D28 (system views materialise, then filter), at the site every branch-per-agent
/// read goes through.
///
/// Passing the predicate here is what lets the planner do what it already does for the plain
/// path: bind it, and pick the index. Nothing is invented; the predicate was simply never
/// handed over. `alias` must match the statement's, or the planner cannot bind a qualified
/// column reference in the predicate.
pub fn scan_table_where(
    table: &str,
    alias: Option<&str>,
    where_clause: Option<&Expr>,
    ctx: &ReadCtx,
) -> Result<Vec<Vec<Value>>, FerroError> {
    // D59: the cached snapshot — one Acquire load when no transaction has begun or ended
    // since this thread last asked. `read_snapshot` remains the uncached truth.
    let view = Arc::new(ReadView { snapshot: ctx.txn.read_snapshot_cached(), txn_id: 0 });
    let stmt = Stmt::Select {
        from: TableRef::plain(table.to_string(), alias.map(|a| a.to_string())),
        columns: vec![Expr::ColumnRef { table: None, column: "*".into() }],
        where_clause: where_clause.cloned(),
        joins: Vec::new(),
    };
    match plan(stmt, ctx.catalog, ctx.bp.clone(), None, view)? {
        Plan::Read(mut root) => {
            // D176 ATTRIBUTION — bracket THIS call's own executor tree.
            //
            // Process-global seq-scan and index-scan counts cannot say which of a merge's two
            // per-delta statement loops is scanning: `evaluate_merge`'s point lookups here, and
            // the publish path's `PendingWrite -> UPDATE`. Both issue `delta` statements, so a
            // window showing `delta` sequential scans and `delta` index scans is consistent with
            // either one doing the scanning. Reading the seq-scan counter either side of this
            // call's OWN drain resolves it by construction instead of by inference.
            //
            // `drop(root)` is explicit and load-bearing: `SeqScan` flushes its tally in `Drop`, so
            // the closing read must happen after the tree is gone or it sees nothing.
            let seq_before = crate::execution::seq_scan::seq_scan_counters().1;
            let mut out = Vec::new();
            while let Some(next) = root.next() {
                out.push(next?.1);
            }
            drop(root);
            let seq_here = crate::execution::seq_scan::seq_scan_counters().1 - seq_before;
            // D176 — one relaxed add per SCAN, never per row. See `SCAN_TABLE_ROWS`.
            SCAN_TABLE_CALLS.fetch_add(1, AtomicOrdering::Relaxed);
            SCAN_TABLE_ROWS.fetch_add(out.len() as u64, AtomicOrdering::Relaxed);
            SCAN_TABLE_SEQ_TUPLES.fetch_add(seq_here, AtomicOrdering::Relaxed);
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
        // The wide types render their exact digits, never a float rendering of them. Re-binding
        // the literal against the target column's declared type is what turns those digits back
        // into a BigInt/Decimal/Timestamp — see `Binder::literal_for_column`.
        Value::BigInt(i) if *i < 0 => neg(i.unsigned_abs().to_string()),
        Value::BigInt(i) => Expr::Literal { value_type: TokenType::Number, value: i.to_string() },
        Value::Decimal(d) => match d.strip_prefix('-') {
            Some(rest) => neg(rest.to_string()),
            None => Expr::Literal { value_type: TokenType::Number, value: d.clone() },
        },
        Value::Timestamp(ms) if *ms < 0 => neg(ms.unsigned_abs().to_string()),
        Value::Timestamp(ms) => Expr::Literal { value_type: TokenType::Number, value: ms.to_string() },
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

/// Strip any nesting of parentheses. `((x))` is `x`, and an access shape must not depend on how many
/// of them the writer typed.
fn ungroup(e: &Expr) -> &Expr {
    let mut cur = e;
    while let Expr::Grouping(inner) = cur {
        cur = inner;
    }
    cur
}

fn ungrouped(e: Option<&Expr>) -> Option<&Expr> {
    e.map(ungroup)
}

/// Classify a read by its **shape**, which is the only admissible input to the read-set form.
/// Size is deliberately not consulted: coarsening scattered point reads into one interval covers
/// most of the table by `k = 3`.
fn access_shape(where_clause: Option<&Expr>, schema: &Schema) -> AccessShape {
    let pk = match schema.columns.first() {
        Some(c) => c.name.clone(),
        None => return AccessShape::FullScan,
    };
    // **Both operand orders.** `7 = id` is `id = 7`, and classifying only one of them is not a
    // neutral omission: the two spellings then get different answers out of every consumer of the
    // shape. Measured before this was mirrored — `UPDATE inventory SET qty = 99 WHERE id = 7`
    // reported row 7 as a blind write and `... WHERE 7 = id` reported nothing, because the second
    // fell through to `FullScan` and so counted as an inspection. Identical semantics, opposite
    // outcome, decided by syntax, which is the same defect shape as the point-lookup absence case.
    //
    // `comparison_range` already mirrors, with the same reasoning written out at `mirror`. This is
    // that rule applied at the one other place operand order is read, rather than a second copy of
    // it: equality is its own mirror, so there is nothing to translate here beyond accepting the
    // swap.
    // **Parentheses are not an access shape either.** `guard_expr` already unwraps `Expr::Grouping`
    // before it looks at anything; this is the same unwrap at the same depth, for the same reason.
    // Measured before it: `WHERE (id = 7)` and `WHERE ((7 = id))` were both classified as scans
    // while `WHERE id = 7` was a lookup. Recursive rather than one level, because `((x))` is two.
    match ungrouped(where_clause) {
        Some(Expr::BinaryOp { left, operator: TokenType::Equal, right }) => {
            let names_pk = |e: &Expr| matches!(ungroup(e), Expr::ColumnRef { column, .. } if *column == pk);
            let is_literal = |e: &Expr| matches!(ungroup(e), Expr::Literal { .. });
            let points_at_pk = (names_pk(left) && is_literal(right))
                || (is_literal(left) && names_pk(right));
            if points_at_pk {
                AccessShape::IndexLookup
            } else {
                AccessShape::FullScan
            }
        }
        _ => AccessShape::FullScan,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A workspace with nothing in it but the two fields `txn_refs` indexes.
    fn ws(name: &str, txn: u64, inherited: &[u64]) -> Workspace {
        let branch = BranchId::new(1, 0);
        Workspace {
            name: name.into(),
            prov: ProvId::NONE,
            txn: TxnId(txn),
            fork_seq: 0,
            fork_root: 0,
            // D27 made these structurally shared (`PersistentMap` / `Arc<Vec>`). None of them is
            // read by `txn_refs_of`, which is why the index survived that change untouched — only
            // this fixture's spelling had to follow.
            rows: PersistentMap::new(),
            unprobeable_rows: 0,
            base_rows: PersistentMap::new(),
            inherited: inherited.iter().map(|t| TxnId(*t)).collect(),
            tables: PersistentMap::new(),
            frame: TxnFrame::new(TxnId(txn), branch, CommitHash::ZERO, 0, 1),
            schema_edits: Arc::new(Vec::new()),
            base_shapes: PersistentMap::new(),
        }
    }

    /// The two doors keep `txn_refs` balanced — including over a RECYCLED id slot, where the
    /// insert lands on a slot that is already occupied and the old value is discarded.
    ///
    /// Asserted against a brute-force scan of `workspaces` rather than against a second copy of
    /// the arithmetic: the index exists to give the same answer as that scan, so that is what it
    /// is compared to.
    ///
    /// **D158 item 1 changed the spelling of "recycled", not the scenario.** `workspaces` is
    /// keyed by the whole `BranchId` now, so a slot coming back is `(8, g0)` then `(8, g1)`
    /// rather than `8` twice; the displacement it exercises is the same one, and it is now
    /// `insert_workspace`'s own range scan rather than a `BTreeMap` key collision that performs
    /// it. Every assertion below is unchanged.
    #[test]
    fn the_txn_ref_index_tracks_a_scan_of_the_workspaces() {
        let scan = |st: &State, t: u64| {
            st.workspaces.values().any(|w| w.txn == TxnId(t) || w.inherited.contains(&TxnId(t)))
        };
        let mut st = State::default();
        st.insert_workspace(BranchId::new(7, 0), ws("b_7", 100, &[]));
        // A child of 7 inherits its parent's staged txn, so txn 100 now has TWO referents.
        st.insert_workspace(BranchId::new(8, 0), ws("b_8", 101, &[100]));
        assert_eq!(st.txn_refs.get(&100), Some(&2));
        for t in [100, 101] {
            assert_eq!(st.txn_refs.contains_key(&t), scan(&st, t), "txn {t} after inserts");
        }

        // Slot 8 is reaped and recycled: the same slot at a new generation, a different branch
        // and a different txn. The discarded workspace's references must go with it.
        st.insert_workspace(BranchId::new(8, 1), ws("b_8", 102, &[]));
        assert_eq!(st.txn_refs.get(&100), Some(&1), "the recycled slot kept its old reference");
        for t in [100, 101, 102] {
            assert_eq!(st.txn_refs.contains_key(&t), scan(&st, t), "txn {t} after recycling");
        }

        assert!(st.remove_workspace(&BranchId::new(7, 0)).is_some());
        assert!(st.remove_workspace(&BranchId::new(8, 1)).is_some());
        assert!(st.txn_refs.is_empty(), "left over: {:?}", st.txn_refs);
        assert!(
            st.remove_workspace(&BranchId::new(7, 0)).is_none(),
            "removing twice must not double-decrement"
        );
    }

    /// **The differential assertion has to be able to FAIL**, and nothing in a passing suite shows
    /// that it can: `capture_is_protected`'s `debug_assert` is the only thing standing between a
    /// desynchronised index and the F6 data loss, so it is worth one input that makes it fire.
    ///
    /// The desync is done by reaching past the two doors and editing `txn_refs` directly — which
    /// is exactly the mistake the doors exist to prevent, so this is the failure being simulated
    /// rather than an artificial one.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "txn_refs disagrees with a scan")]
    fn the_index_scan_assertion_fires_on_a_desynchronised_index() {
        let mut st = State::default();
        st.insert_workspace(BranchId::new(7, 0), ws("b_7", 100, &[]));
        assert!(capture_is_protected(&st, TxnId(100)), "a live workspace holds txn 100");

        // The under-count: the index forgets a reference a live workspace still holds. Left
        // unchecked, `forget_captures_unless_published` would drop a capture that is the read
        // premise of a published row.
        st.txn_refs.remove(&100);
        capture_is_protected(&st, TxnId(100));
    }

    /// **The audit has to be able to FAIL at a door**, and a passing suite does not show that it
    /// can. `capture_is_protected`'s assertion only runs on the abandon and reap arms, so it is
    /// this one that covers a desync introduced on the fork path — which is the constraint any
    /// rewrite of `begin_session` is held to. Here is an input that makes it fire.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "txn_refs disagrees with a scan")]
    fn the_door_audit_fires_on_a_desynchronised_index() {
        let mut st = State::default();
        st.insert_workspace(BranchId::new(7, 0), ws("b_7", 100, &[]));
        st.insert_workspace(BranchId::new(8, 0), ws("b_8", 101, &[100]));
        // An OVER-count: a reference to a txn no workspace holds. That is the direction a rewrite
        // going straight to `BTreeMap::insert` produces, and the one the audit alone can see — an
        // under-count is caught earlier and more loudly by `drop_txn_refs`, which refuses to
        // decrement below zero (proven by this test's first draft, which tripped that instead).
        st.txn_refs.insert(999, 1);
        st.remove_workspace(&BranchId::new(8, 0));
    }

    /// The capture of a workspace displaced by a RECYCLED SLOT is dropped, not stranded.
    ///
    /// Nothing else was releasing the displaced value, so each recycled slot leaked one
    /// `captures` entry forever — the unbounded growth this module exists to prevent, through a
    /// door that is not the sweep.
    ///
    /// **This is also the guard on D158 item 1's eviction.** Keying `workspaces` by the whole
    /// `BranchId` means a recycled slot no longer collides on the key, so `insert_workspace` has
    /// to find the slot's previous occupant by range and evict it explicitly; delete that range
    /// scan and the first assertion below fails with the stranded capture named.
    #[test]
    fn a_recycled_slot_does_not_strand_the_displaced_workspaces_capture() {
        let mut st = State::default();
        st.insert_workspace(BranchId::new(7, 0), ws("b_7", 100, &[]));
        st.captures.insert(100, TxnCapture::new(TxnId(100), ProvId::NONE, BranchId::new(7, 0)));

        // Slot 7 comes back at a new generation with a new txn, displacing the old workspace.
        st.insert_workspace(BranchId::new(7, 1), ws("b_7", 200, &[]));
        assert!(
            !st.captures.contains_key(&100),
            "the displaced workspace's capture was stranded: {:?}",
            st.captures.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            st.workspaces.len(),
            1,
            "a recycled slot must hold ONE workspace, not one per generation: {:?}",
            st.workspaces.keys().collect::<Vec<_>>()
        );

        // And a capture that IS still needed survives the same path: `published_txns` is the
        // protection `forget_captures_unless_published` consults, so this proves the displacement
        // uses that rule rather than deleting unconditionally.
        st.captures.insert(300, TxnCapture::new(TxnId(300), ProvId::NONE, BranchId::new(9, 0)));
        st.published_txns.insert(300);
        st.insert_workspace(BranchId::new(9, 0), ws("b_9", 300, &[]));
        st.insert_workspace(BranchId::new(9, 1), ws("b_9", 301, &[]));
        assert!(st.captures.contains_key(&300), "a PUBLISHED capture must survive the displacement");
    }

    /// **The new workspace is INSTALLED before the displaced one is released, not after.**
    ///
    /// `insert_workspace` counts the incoming workspace's txn references first, then has to let
    /// the slot's previous occupant go. Release before installing and any txn the incoming
    /// workspace holds reads as indexed while no workspace in the map carries it, so
    /// `capture_is_protected`'s differential assertion fires on a state that is in fact
    /// consistent — and in release builds, where that assertion is compiled out, the capture is
    /// kept for the wrong reason.
    ///
    /// The slot-keyed map could not get this wrong: `BTreeMap::insert` installed the new value
    /// and handed the old one back in ONE call. Keying by `BranchId` (D158 item 1) split that in
    /// two, which turned a free property into a decision — so it gets an input that tells the two
    /// orders apart, rather than a comment claiming the order is right.
    ///
    /// That input is a session forking into a recycled slot while INHERITING a txn the displaced
    /// workspace owned, which is what makes the incoming workspace the only holder of it at the
    /// moment of release. Measured: with the two steps swapped this test panics inside
    /// `capture_is_protected` with "txn_refs disagrees with a scan of workspaces about txn
    /// TxnId(100)". The discriminator is a `debug_assert`, as it is for the two door-audit tests
    /// above, so this one discriminates in debug and merely holds in release.
    #[test]
    fn a_recycled_slot_installs_the_new_workspace_before_releasing_the_old() {
        let mut st = State::default();
        st.insert_workspace(BranchId::new(7, 0), ws("b_7", 100, &[]));
        st.captures.insert(100, TxnCapture::new(TxnId(100), ProvId::NONE, BranchId::new(7, 0)));

        // Slot 7 comes back, and the new session inherits txn 100 — the displaced workspace's own.
        st.insert_workspace(BranchId::new(7, 1), ws("b_7", 200, &[100]));

        assert!(
            st.captures.contains_key(&100),
            "txn 100 is still held by the workspace that just took the slot, so its capture must \
             survive the displacement: {:?}",
            st.captures.keys().collect::<Vec<_>>()
        );
        assert_eq!(st.txn_refs.get(&100), Some(&1), "exactly the new workspace holds txn 100");
        assert_eq!(st.workspaces.len(), 1, "one workspace per slot");
    }

    /// **The chunked reconciliation must visit EVERY workspace, across a chunk boundary.**
    ///
    /// `forget_reaped_branches` walks `workspaces` `FORGET_CHUNK` at a time, releasing the state
    /// lock between chunks and resuming from the last key it saw. D158 item 1 changed that key
    /// from a `u64` slot to a `BranchId`, which changed how the cursor resumes: `key + 1` has no
    /// meaning at the top of the generation space, so it is an EXCLUDED bound now. An off-by-one
    /// there either skips workspaces — the unbounded growth this function exists to prevent — or
    /// loops for ever on one key.
    ///
    /// **Nothing in the suite crossed that boundary.** Every other fixture that reaches this
    /// function holds a handful of sessions, so the multi-chunk path ran zero times and a broken
    /// cursor would have been invisible. That is what this test is for, and it lives in this
    /// module rather than in `tests/` for one reason: `FORGET_CHUNK` is in scope here, so the
    /// fixture is sized **by the constant**. A literal `1025` in an integration test silently
    /// stops crossing the boundary the day the constant grows, which is the one way this could
    /// pass while testing nothing.
    #[test]
    fn the_reconciliation_crosses_its_chunk_boundary_and_forgets_every_branch() {
        let catalog = Arc::new(LogBranchCatalog::in_memory(1));
        let rt = AgentRuntime::with_catalog(Arc::clone(&catalog) as Arc<dyn BranchCatalog>);

        // One more than a chunk, so the walk must resume at least once.
        let n = FORGET_CHUNK + 1;
        let mut opened: Vec<BranchId> = Vec::with_capacity(n);
        for i in 0..n {
            let s = rt
                .begin_session("chunky", Some(&format!("r{i}")), BranchId::TRUNK)
                .expect("fork");
            opened.push(s.branch);
        }
        assert_eq!(rt.state.lock().unwrap().workspaces.len(), n, "fixture did not open n sessions");

        // Reaped behind the runtime's back, which is what the reconciliation exists to notice.
        // The slots are deliberately NOT released: recycling is a different property, tested in
        // `tests/w4_sweep_slot_recycle.rs`, and releasing here would let a later fork displace a
        // workspace this walk is supposed to find.
        for b in &opened {
            let rec = rt.branches().get(*b).expect("live before the reap");
            rt.branches().set_state(*b, rec.state, BranchState::Reaped).expect("mark reaped");
        }

        assert_eq!(
            rt.forget_reaped_branches(),
            n,
            "the walk must forget every branch it was left holding, not one chunk's worth"
        );
        let left = rt.state.lock().unwrap().workspaces.len();
        assert_eq!(left, 0, "{left} workspaces survived a full reconciliation");
    }

    fn applied_at(seq: u64) -> AppliedOp {
        AppliedOp {
            seq,
            txn: TxnId(1),
            table: "inventory".into(),
            tbl: TableId(1),
            row: RowId(1),
            col: None,
            kind: OpKind::RowDelete,
            before: None,
            before_row: None,
        }
    }

    /// **The guard has to be able to REFUSE**, which the `debug_assert` it stands behind cannot:
    /// both of that assertion's sides are the same sum over the same list, so no input falsifies it.
    /// This one is answerable from the record of what was applied, and here is an input it refuses.
    #[test]
    fn a_reservation_that_would_re_issue_a_version_sequence_is_refused() {
        let applied = vec![applied_at(1), applied_at(3), applied_at(2)];
        // `max`, not `last` — a broken invariant is exactly when the order stops holding.
        assert_eq!(highest_applied_seq(&applied), Some(3));

        // `record_applied` stamps `base + 1 ..= base + n`, so a base of 2 re-issues 3.
        let err = fresh_reservation(Some(3), 2).expect_err("re-issuing version 3 was allowed");
        assert!(err.contains("already been handed out"), "{err}");
        assert!(err.contains("begin_ts"), "the refusal does not say what breaks: {err}");
        assert!(err.contains('3'), "the refusal does not name the sequence: {err}");

        // And the shape the reservation arithmetic actually produces when it goes wrong: a merge
        // that reserved for one row and stamped three leaves `apply_seq` two behind, so the NEXT
        // merge's base is below the high water mark.
        assert!(fresh_reservation(Some(3), 1).is_err(), "a base two behind was allowed");
        assert!(fresh_reservation(Some(1), 0).is_err(), "a base one behind was allowed");
    }

    /// Anti-vacuity: a guard that refused every merge would satisfy the test above completely.
    #[test]
    fn the_ordinary_reservation_and_a_leaked_range_are_both_allowed() {
        assert!(highest_applied_seq(&[]).is_none(), "a database where nothing has been applied");
        assert!(fresh_reservation(None, 0).is_ok(), "the first merge of a fresh database");
        assert!(
            fresh_reservation(Some(3), 3).is_ok(),
            "the ordinary case: the next fresh sequence is 4"
        );
        assert!(
            fresh_reservation(Some(3), 9).is_ok(),
            "a range leaked by a failed publication only shifts later versions upward, which \
             over-reports rather than corrupts, so it must not be refused"
        );
    }
}
