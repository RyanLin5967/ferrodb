# 1. Constructors/types taking `Arc<dyn EffectLog>`

The trait itself: `pub trait EffectLog: Send + Sync` — `src/tel/mod.rs:43`, two methods only:
```rust
fn append(&self, frame: &TxnFrame) -> Result<(), FerroError>;                                    // src/tel/mod.rs:44
fn frames_for(&self, branch: BranchId, from_seq: u64) -> Result<Vec<TxnFrame>, FerroError>;      // src/tel/mod.rs:47
```
Public path `ferrodb::tel::EffectLog`. **Only one production impl exists in the whole tree**: `MemEffectLog` (`src/tel/log.rs:88`). The other is a test-local `NoLog` at `src/agent_sql/merge_engine.rs:573`.

| Type | Constructor | file:line | module path |
|---|---|---|---|
| `ThreeWayMerger` | `pub fn with_log(log: Arc<dyn EffectLog>) -> Self` | `src/tel/engine.rs:314` | `ferrodb::tel::engine::ThreeWayMerger`, re-exported `ferrodb::tel::ThreeWayMerger` (`src/tel/mod.rs:26`) |
| `ThreeWayMerger` | `pub fn new() -> Self` (log = `None`) | `src/tel/engine.rs:310` | same; also `#[derive(Clone, Default)]` at `src/tel/engine.rs:304`, so `ThreeWayMerger::default()` works |
| `SurfaceMerger` | `pub fn new(log: Arc<dyn EffectLog>) -> Self` (log **not** optional) | `src/agent_sql/merge_engine.rs:330` | `ferrodb::agent_sql::merge_engine::SurfaceMerger`, re-exported `ferrodb::agent_sql::SurfaceMerger` (`src/agent_sql/mod.rs:43`) |
| `AgentRuntime` | `pub fn with_parts(branches: Arc<dyn BranchCatalog>, log: Arc<dyn EffectLog>) -> Self` | `src/agent_sql/runtime.rs:395` | `ferrodb::agent_sql::AgentRuntime` |
| `AgentRuntime` | `pub fn with_storage(branches: Arc<dyn BranchCatalog>, log: Arc<dyn EffectLog>, store: Arc<dyn PageStore>) -> Result<Self, FerroError>` | `src/agent_sql/runtime.rs:437-441` | same |
| `AgentRuntime` | `pub fn reopen_with_storage(branches: Arc<dyn BranchCatalog>, log: Arc<dyn EffectLog>, store: Arc<dyn PageStore>) -> Result<Self, FerroError>` | `src/agent_sql/runtime.rs:474-478` | same |
| `AgentRuntime` | accessor `pub fn log(&self) -> &Arc<dyn EffectLog>` | `src/agent_sql/runtime.rs:639` | same |

Field declarations: `ThreeWayMerger.log: Option<Arc<dyn EffectLog>>` (`src/tel/engine.rs:306`), `SurfaceMerger.log: Arc<dyn EffectLog>` (`src/agent_sql/merge_engine.rs:326`), `AgentRuntime.log: Arc<dyn EffectLog>` (`src/agent_sql/runtime.rs:362`).

# 2. Public methods that read frames back and produce a merge result

Both mergers implement `pub trait Merger: Send + Sync` (`src/tel/merge.rs:214`):

```rust
fn merge(
    &self,
    lca: &BranchRecord,
    ours: &[TxnFrame],
    theirs: &[TxnFrame],
    policy: &dyn ColumnPolicyLookup,
    merged_state: &dyn GuardContext,
) -> Result<MergeOutcome, FerroError>;                      // src/tel/merge.rs:215-222

fn diff(&self, from: BranchId, to: BranchId) -> Result<Diff, FerroError>;   // src/tel/merge.rs:224
```

Impl sites: `ThreeWayMerger::merge` `src/tel/engine.rs:334`, `ThreeWayMerger::diff` `src/tel/engine.rs:457`; `SurfaceMerger::merge` `src/agent_sql/merge_engine.rs:336`, `SurfaceMerger::diff` `src/agent_sql/merge_engine.rs:427`.

**Which one actually reads the log.** `merge` does **not** read the log at all — it is handed `&[TxnFrame]` slices; the caller reads them back via `log.frames_for(branch, 0)`. Documented at `src/tel/engine.rs:302-303`. Only `diff` reads the log:
- `ThreeWayMerger::diff` (`src/tel/engine.rs:457-482`): reads `frames_for(from,0)` **and** `frames_for(to,0)`, subtracts the `TxnId`s present on `from`, sorts the novel ones by `(seq, txn_id.0)`, de-dupes by `TxnId`, and concatenates `ops` then `guards`. With no log it returns `Err(FerroError::Merge("this merger has no effect log, so it cannot compute a diff"))` (`src/tel/engine.rs:458-460`) — explicitly tested at `src/tel/engine.rs:1750`.
- `SurfaceMerger::diff` (`src/agent_sql/merge_engine.rs:427-437`): reads **only** `frames_for(to, 0)`, `dedup_frames`, concatenates. It ignores `from` except to stamp `Diff.from`, so it returns the target branch's whole history, not a changeset relative to `from`. This is a real behavioural difference from `ThreeWayMerger::diff`.

Outcome types, all in `src/tel/merge.rs`, all `#[derive(Debug, Clone, PartialEq)]` — directly `assert_eq!`-able:

| Type | file:line | derives |
|---|---|---|
| `MergeOutcome` (`Clean` / `Commuting{composed:Vec<Op>}` / `Conflict(Vec<ConflictReport>)` / `ResolvedWithLoss{applied:Vec<Op>, discarded:Vec<DiscardedWrite>}`) | `src/tel/merge.rs:132-142` | `Debug, Clone, PartialEq` + `Display` at `:171` |
| `Diff { from, to, ops: Vec<Op>, guards: Vec<Guard> }` | `src/tel/merge.rs:196-202` | `Debug, Clone, PartialEq` |
| `ConflictReport` | `src/tel/merge.rs:73-86` | `Debug, Clone, PartialEq` + `Display` `:112` |
| `DiscardedWrite` | `src/tel/merge.rs:119-125` | `Debug, Clone, PartialEq` |
| `ConflictKind` | `src/tel/merge.rs:57-70` | `Debug, Clone, PartialEq, Eq, Hash` |
| `Deduped { kept: Vec<TxnId>, dropped: Vec<TxnId> }` | `src/tel/engine.rs:221-224` | `Debug, Clone, PartialEq` |
| `Op` | `src/tel/op.rs:146-160` | `Debug, Clone, PartialEq`; fields all `pub` incl. `witness: Option<Value>` |
| `Guard` | `src/tel/guard.rs:317-322` | `Debug, Clone, PartialEq` |

Helpers on `MergeOutcome`: `is_conflict()` `:145`, `lost_a_write()` `:150`, `conflicts() -> &[ConflictReport]` `:154`, `name() -> &'static str` `:161`. Existing tests compare by `name()` (string) rather than full `PartialEq` in the differential tests, and by full `assert_eq!(out, MergeOutcome::Clean)` at `src/tel/engine.rs:1708`.

# 3. `dedup_by_txn`

```rust
pub fn dedup_by_txn<'a>(
    ours: &'a [TxnFrame],
    theirs: &'a [TxnFrame],
) -> (Vec<(&'a TxnFrame, Side)>, Deduped)
```
`src/tel/engine.rs:232-235`; re-exported `ferrodb::tel::dedup_by_txn` (`src/tel/mod.rs:26`). Body `src/tel/engine.rs:236-249`.

- **Keys on `TxnFrame.txn_id` alone** (`HashSet<TxnId>`, `seen.insert`) — not on branch, not on seq, not on op contents. So the same `TxnId` reaching a merge from both sides is dropped from `theirs`.
- Iteration order: `ours` first (tagged `Side::Ours`), then `theirs` (`Side::Theirs`); **within each side** frames are sorted by `(f.seq, f.txn_id.0)` before de-duping (`src/tel/engine.rs:239-240`). First occurrence wins.
- Returns `(kept_frames_with_side, Deduped { kept, dropped })` where `kept`/`dropped` are `Vec<TxnId>` in encounter order. `Deduped` is `PartialEq`, so the report is directly assertable — see `src/tel/engine.rs:1275-1283` (`report.dropped == vec![TxnId(7)]`) and `:1285-1294` (cross-side duplicate, `kept[0].1 == Side::Ours`).
- `Side` is `pub enum Side { Ours, Theirs }`, `#[derive(Debug, Clone, Copy, PartialEq, Eq)]`, `src/tel/engine.rs:54-58`.

The SQL-surface analogue is a **different function with a different signature**: `pub fn dedup_frames(frames: &[TxnFrame]) -> Vec<&TxnFrame>` at `src/agent_sql/merge_engine.rs:106`. It uses a linear `Vec<TxnId>` scan, takes one side at a time, and does **not** sort by `(seq, txn_id)`.

# 4. Compiling-shaped sketch: merge from log, restart log, recompute, compare

Fixtures needed and their constructors — no `Workspace`, no `AgentRuntime`, no page store:
- `MemEffectLog::new()` — `src/tel/log.rs:42`; `#[derive(Default)]` at `:35`. Public path `ferrodb::tel::MemEffectLog`.
- `PolicyTable::new()` — `src/agent_sql/merge_engine.rs:49`; `set(tbl, col, policy)` `:53`; `impl ColumnPolicyLookup` `:58` (default `MergePolicy::Reject`, `src/tel/merge.rs:28-29`).
- Base/merged state: `CellState::new()` — `src/agent_sql/merge_engine.rs:75`; `set(tbl,row,col,Value)` `:79`; `impl GuardContext` `:92`. (`GuardContext` is `src/tel/guard.rs:309`, one method `column`.) tel's own tests use a private `Base` struct instead (`src/tel/engine.rs:1160-1178`); `CellState` is the public equivalent and is what the cross-engine tests use (`tests/integration_merge_agreement.rs:52-57`).
- LCA: `BranchRecord::trunk(root_page_id: PageId, lease_deadline: LeaseDeadline)` — `src/branch/record.rs:55`. Existing tests pass `BranchRecord::trunk(1, LeaseDeadline(u64::MAX))`.
- Frames: `TxnFrame::new(TxnId, BranchId, CommitHash, seq: u64, schema_ver: SchemaVer /* u32 */)` — `src/tel/frame.rs:36`; `push_op` `:49`, `push_guard` `:53`, `push_claim` `:57`. `BranchId::new(id: u64, generation: u32)` — `src/branch/types.rs:32`; `CommitHash::ZERO` — `src/branch/types.rs:111`.
- Ops: `Op::new(tbl, row, col: Option<ColId>, kind: OpKind)` — `src/tel/op.rs:163`; `.with_witness(Value)` — `:167` (or set `op.witness` directly, it is `pub`).
- Guards: `Guard::holds(GuardExpr)` — `src/tel/guard.rs:334`; `.with_source(impl Into<String>)` — `:338`; `GuardExpr::cmp(..)`, `GuardExpr::col(tbl,row,col)`, `GuardExpr::Literal(Value)`.

```rust
use std::sync::Arc;
use ferrodb::agent_sql::merge_engine::{CellState, PolicyTable};
use ferrodb::branch::record::BranchRecord;
use ferrodb::branch::types::{BranchId, CommitHash, LeaseDeadline};
use ferrodb::catalog::column::Value;
use ferrodb::tel::ids::{ColId, RowId, TableId, TxnId};
use ferrodb::tel::merge::{MergePolicy, Merger};
use ferrodb::tel::op::{Delta, Op, OpKind};
use ferrodb::tel::{EffectLog, MemEffectLog, ThreeWayMerger, TxnFrame};

const TBL: TableId = TableId(1); const ROW: RowId = RowId(1); const QTY: ColId = ColId(2);
let mut f = TxnFrame::new(TxnId(1), BranchId::new(1, 0), CommitHash::ZERO, 0, 1);
f.push_op(Op::new(TBL, ROW, Some(QTY), OpKind::Add(Delta::Int(-5))).with_witness(Value::Integer(20)));

let log: Arc<dyn EffectLog> = Arc::new(MemEffectLog::new());
log.append(&f).unwrap();                          // and again, to prove the dedup: log.append(&f)
let ours = log.frames_for(BranchId::new(1, 0), 0).unwrap();

let mut policy = PolicyTable::new(); policy.set(TBL, QTY, MergePolicy::Additive);
let mut pre = CellState::new(); pre.set(TBL, ROW, QTY, Value::Integer(20)); // PRE-op state; see §5
let lca = BranchRecord::trunk(1, LeaseDeadline(u64::MAX));
let m = ThreeWayMerger::with_log(Arc::clone(&log));
let first = m.merge(&lca, &ours, &[], &policy, &pre).unwrap();   // MergeOutcome: Debug + PartialEq
let d1 = m.diff(BranchId::TRUNK, BranchId::new(1, 0)).unwrap();  // Diff: Debug + PartialEq

let log2: Arc<dyn EffectLog> = Arc::new(MemEffectLog::new());    // "restart": replay into a fresh log
for fr in &ours { log2.append(fr).unwrap(); }
let back = log2.frames_for(BranchId::new(1, 0), 0).unwrap();
let m2 = ThreeWayMerger::with_log(Arc::clone(&log2));
assert_eq!(back, ours);                                          // TxnFrame is PartialEq
assert_eq!(m2.merge(&lca, &back, &[], &policy, &pre).unwrap(), first);
assert_eq!(m2.diff(BranchId::TRUNK, BranchId::new(1, 0)).unwrap(), d1);
```

Nearest existing precedent, already in the tree: `merging_frames_read_back_from_the_log_reaches_the_same_answer`, `src/tel/engine.rs:1755-1782` — appends `ours`, `theirs`, and a replayed `theirs`, reads both back with `frames_for(..., 0)`, merges with `ThreeWayMerger::new()`, and asserts the composed delta is `-10` and that a base of 8 conflicts. Also `tests/integration_merge_agreement.rs:63-78` for the two-engine `both()` harness and `tests/agent_sql_surface.rs:688-695` for `SurfaceMerger::new(db.runtime.log().clone()).diff(...)`.

**Caveat on "restart the log": no durable `EffectLog` exists on this branch.** `grep -rn "impl EffectLog" src/` returns exactly `MemEffectLog` (`src/tel/log.rs:88`) plus the test `NoLog`. There is no encode/decode/serialize for `TxnFrame` anywhere under `src/tel/` (`grep -rn "fn encode\|fn decode\|serialize\|to_bytes\|from_bytes" src/tel/` → NOT FOUND). So a restart can only be simulated in-process by replaying frames into a fresh `MemEffectLog`, as above.

Two `MemEffectLog::append` behaviours a restart test must respect (`src/tel/log.rs:88-113`): an identical re-append is stored once; an append that is a strict *prefix-extension* of the stored frame replaces it; anything else — differing/dropped/reordered ops, changed `base` or `seq` — is a hard `FerroError::Merge` refusal (`extends()`, `src/tel/log.rs:76-85`). `frames_for` filters `f.branch == branch && f.seq >= from_seq` and sorts by `(seq, txn_id.0)` (`src/tel/log.rs:114-123`), so replay order into `log2` does not affect what comes back out.

# 5. Does any merge path need a live `AgentRuntime` / page store?

**No, for the log-based path.** `ThreeWayMerger::merge` needs only `&BranchRecord`, two frame slices, a `&dyn ColumnPolicyLookup` and a `&dyn GuardContext`; `src/tel/engine.rs:330-333` states the `BranchRecord` is not even consulted, only retained to force the caller to prove an LCA. `ThreeWayMerger::diff` needs only the log. `SurfaceMerger` is the same. Nothing in either touches a page store, catalog, buffer pool or `TxnManager`. The log + a policy lookup + a `GuardContext` is sufficient.

**Yes, for `AgentRuntime`'s own merge path** — and note it is a *third*, workspace-based implementation, not a call into either `Merger`:
- `pub fn merge(&self, ctx: &mut ExecCtx, branch: BranchId) -> Result<MergeReport, FerroError>` — `src/agent_sql/runtime.rs:2061`
- `pub fn evaluate_merge(&self, ctx: &mut ExecCtx, branch: BranchId, assertions: &[Assertion]) -> Result<MergeEvaluation, FerroError>` — `src/agent_sql/runtime.rs:2135-2140`
- `pub fn diff(&self, ctx: &mut ExecCtx, branch: BranchId) -> Result<ChangeSet, FerroError>` — `src/agent_sql/runtime.rs:1863`

These require `ExecCtx<'a> { pub catalog: &'a mut Catalog, pub bp: Arc<BufferPoolManager>, pub txn: Arc<TxnManager> }` (`src/agent_sql/runtime.rs:85-89`) and a live session — they read `state.workspaces.get(&branch.id)` and error `"no agent session on branch {}"` otherwise (`runtime.rs:1866-1868`, `2144-2146`). `struct Workspace` (`runtime.rs:145`) and `struct WorkspaceSnapshot` (`runtime.rs:3798`) are **private**, so a test cannot construct one; you must go through `begin_session` + `stage`. `evaluate_merge` also calls `scan_table(name, ctx)` (`runtime.rs:2270`) and `ctx.catalog.get_table(name)`.

**Correction to a stale in-tree claim.** The doc comment at `src/agent_sql/runtime.rs:637-638` says "`MERGE` and `DIFF` both read from here through the shared traits, so the log is on the live path rather than a side record." That is false in the current tree: `grep -n "self\.log" src/agent_sql/runtime.rs` yields exactly two hits — the accessor at `:640` and one `self.log.append(&frame)` at `:1831` (inside `stage`). `AgentRuntime::merge`, `evaluate_merge` and `diff` read `Workspace.rows` / `Workspace.base_rows` / `Workspace.frame`, never `frames_for`. The comment at `runtime.rs:1836-1838` confirms it: "The workspace map above is still what `DIFF` and `MERGE` read". Corroborated by `tests/integration_merge_agreement.rs:5-8`, which states only the SQL surface's merger is near the demo path and that the runtime "shares its core" rather than calling it.

So: the log is **write-only** on the runtime path. A merge computed from `EffectLog` frames is computed by `ThreeWayMerger` or `SurfaceMerger` standalone, and its answer is not the one `AgentRuntime::merge` returns.