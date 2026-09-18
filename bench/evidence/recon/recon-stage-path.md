1. **`EffectLog::append` call sites in `src/`**

Exactly ONE non-test call site in production code:

- `src/agent_sql/runtime.rs:1831` — `self.log.append(&frame)?;` inside `AgentRuntime::stage_all`. Passes a **clone of the workspace's long-lived accumulating frame** (`ws.frame.clone()`), i.e. a RE-APPENDED GROWING frame on every statement after the first.

Surrounding lines (`src/agent_sql/runtime.rs:1807-1832`):
```
1807  let mut mirrored: Vec<(RowId, RowState)> = Vec::with_capacity(items.len());
1808  let frame = {
1809      let mut state = self.state.lock().unwrap();
1810      let ws = state.workspaces.get_mut(&branch.id).ok_or_else(|| {
...
1814      for item in items {
1815          let key = Workspace::key(tbl, item.row);
1816          ws.base_rows.entry(key).or_insert(item.before);
1817          ws.rows.insert(key, item.after.clone());
1818          for op in item.ops {
1819              ws.frame.push_op(op);
1820          }
1821          if let Some(g) = item.guard {
1822              ws.frame.push_guard(g);
1823          }
1824          mirrored.push((item.row, item.after));
1825      }
1826      ws.frame.clone()
1827  };
1828  // Re-appending the task's frame replaces it rather than adding a second copy: `Add` is
1829  // not idempotent and two copies of one frame would double-count. Appended ONCE for the whole
1830  // statement, which is also why the frame is cloned after every row is folded in.
1831  self.log.append(&frame)?;
```

`self.log` is referenced in only two places in `runtime.rs`: the accessor at `src/agent_sql/runtime.rs:640` and the append at `:1831` (grep over `src/agent_sql/runtime.rs` for `self\.log`). No COMMIT / MERGE / ABANDON / DDL path appends; there is no delete/prune API on the trait (`src/tel/mod.rs:43-52`), so an abandoned task's frame stays in the log (`abandon` at `src/agent_sql/runtime.rs:3278`, `workspaces.remove` at `:3315`, touches no log).

Callers that reach `:1831`, one append each: `src/agent_sql/runtime.rs:1506` (branch_update, one call per statement with all matched rows), `:1555` (branch_insert, via `stage`), `:1612` (branch_delete), and `stage` itself at `:1632`. `stage` (`:1621-1633`) is a one-row wrapper over `stage_all` and does NOT append separately.

All other `append` hits are test-only frames (fresh frames, not growth): `src/tel/log.rs:157,158,159,172,173,180,181,188,189,206`; `src/tel/capture.rs:469,470`; `src/tel/engine.rs:1727,1730,1735,1764,1765,1767`. (`src/consensus/**` and `src/wal/**` `append`s are a different log type — raft `Entry` / WAL records, not `TxnFrame`.)

2. **Two-statement agent task under one `TxnId` — exact append sequence**

Frame identity is created once per agent session, not per statement: `src/agent_sql/runtime.rs:699-700` (`state.next_txn += 1; let txn = TxnId(state.next_txn)`) and `:783` (`frame: TxnFrame::new(txn, branch, CommitHash::ZERO, 0, 1)`) inside the `Workspace` built at `:771-790`.

Sequence for `UPDATE inventory SET qty = qty - 5 WHERE id = 1 AND qty >= 5;` then `UPDATE inventory SET qty = qty - 1 WHERE id = 2;` (the literal case in `tests/agent_sql_surface.rs:677-678`):

- append #1: frame `{txn_id: TxnId(n), branch: B, base: CommitHash::ZERO, seq: 0, schema_ver: 1, ops: [op1], guards: [g1], claims: []}` → no key match, pushed (`src/tel/log.rs:113`).
- append #2: frame `{... same txn_id, branch, base, seq, schema_ver ..., ops: [op1, op2], guards: [g1, g2], claims: []}` → key `(branch, txn_id)` matches (`src/tel/log.rs:91-94`), not equal, `extends()` holds, so `frames[i] = frame.clone()` REPLACES in place (`src/tel/log.rs:110`).

What grows: `ops`, `guards`, `claims` — append-only, prefix-preserving, never reordered or truncated (`src/tel/frame.rs:49-59`; ops pushed at `runtime.rs:1819`, guards at `:1822`).
What stays byte-identical for the life of the task: `txn_id`, `branch` (the log key, `src/tel/log.rs:93`), and `base`, `seq`, `schema_ver` — all three are frozen at `TxnFrame::new(txn, branch, CommitHash::ZERO, 0, 1)`, `runtime.rs:783`, and `extends()` requires exact equality on them (`src/tel/log.rs:80-82`). `grep -n schema_ver src/agent_sql/runtime.rs` → NO hits, so `schema_ver` is permanently 1 and `seq` permanently 0 on the SQL path. `claims` never grows from SQL: `push_claim` has no caller outside `src/tel/frame.rs:57` and two `src/tel/engine.rs` tests (`:1412`, `:1697`).

Growth per statement is per-row, not per-statement-unit: `branch_update` pushes one `Op` per assigned column per matched row and one `Guard` per matched row when a WHERE exists (`src/agent_sql/runtime.rs:1471-1492`), then appends ONCE (`:1506` → `:1831`). So a 3-row, 2-column UPDATE grows `ops` by 6 and `guards` by 3 in a single append. INSERT contributes one `RowCreate` op with `col: None` and no guard (`:1554-1555`).

Ordering inside the append: the log append at `:1831` happens BEFORE the page mirror at `:1843-1856`, so a `put_row`/`delete_row` failure leaves the grown frame already logged (stated at `:1790-1794`).

3. **`EffectLog::frames_for` call sites in `src/`**

- `src/tel/mod.rs:47` — trait declaration; doc: "Frames written on `branch` at or after `from_seq`, in sequence order."
- `src/tel/log.rs:117-126` — `MemEffectLog`'s implementation. Filters `f.branch == branch && f.seq >= from_seq`, then **`out.sort_by_key(|f| (f.seq, f.txn_id.0))`** (`:124`). Because the SQL path pins `seq = 0` for every frame, the returned order is effectively by `TxnId` alone.
- `src/tel/engine.rs:461-462` — `ThreeWayMerger::diff`: reads `from` and `to`, builds a `HashSet<TxnId>` of `from`, then **re-sorts the novel frames itself** (`:467`, `novel.sort_by_key(|f| (f.seq, f.txn_id.0))`) before concatenating `ops` and `guards` in that order (`:472-478`), de-duplicating by `TxnId` (`:469,473`). Does not depend on `frames_for`'s own ordering.
- `src/agent_sql/merge_engine.rs:428` — `SurfaceMerger::diff`: `let frames = self.log.frames_for(to, 0)?;` then `for f in dedup_frames(&frames)` and extends `ops`/`guards` in iteration order (`:431-434`). **No re-sort — this one depends on `frames_for`'s returned order**, both for op concatenation order and for which duplicate wins: `dedup_frames` is first-occurrence-wins by `TxnId` (`src/agent_sql/merge_engine.rs:106-116`).
- `src/agent_sql/merge_engine.rs:577` — a test stub `NoLog` returning nothing.
- Tests only: `src/tel/log.rs:161,163,164,207`; `src/tel/capture.rs:472,473`; `src/tel/engine.rs:1769,1770`.

`dedup_by_txn` (`src/tel/engine.rs:232-252`) also re-sorts each side by `(seq, txn_id.0)` at `:241`, so the merge path does not depend on log ordering either.

Note for the durable-log question: `AgentRuntime`'s own `diff` (`src/agent_sql/runtime.rs:1863`, reading `ws.frame.ops`/`ws.frame.guards` at `:1876-1894`) and `evaluate_merge` (`:2135`, reading `ws.frame.ops`/`guards` at `:2154-2155`) read the LIVE WORKSPACE, not the log. The docstring at `src/agent_sql/runtime.rs:637-638` ("`MERGE` and `DIFF` both read from here through the shared traits, so the log is on the live path rather than a side record") does not match those two code paths. The only callers of `runtime.log()` anywhere are `tests/agent_sql_surface.rs:682,689`.

4. **`tests/integration_effect_log.rs` — 3 tests** (file is 74 lines; helper `frame(n)` at `:16-27` builds `TxnId(1)`, `BranchId::new(1,0)`, `CommitHash::ZERO`, `seq 0`, `schema_ver 1`, with `n` `Add(Int(-5))` ops on rows `0..n`)

- `an_open_task_frame_may_grow_under_one_txn_id` (`:29-40`) — appends `frame(1)`, `frame(2)`, `frame(3)`; asserts `frames_for(...).len() == 1` ("a growing frame became several frames") and `back[0].ops.len() == 3` ("the frame did not grow to its final contents").
- `a_replayed_identical_frame_is_still_not_stored_twice` (`:42-51`) — appends the identical `frame(2)` twice; asserts `log.len() == 1` and the stored frame still has exactly 2 ops (no doubling of `Add`).
- `a_txn_id_collision_between_different_transactions_is_still_a_hard_error` (`:53-74`) — after `frame(3)`: a contradicting frame (different table/row/col, `Assign`) errors with a message containing "refusing" (`:67-68`); a TRUNCATING re-append `frame(2)` also errors with "refusing" (`:71-72`); the stored frame still has 3 ops (`:73`).

Not covered by this file: ordering / `from_seq` filtering, `guards`/`claims` growth (ops only), cross-branch same-`TxnId`, and anything durable/reopen.

5. **`MemEffectLog::all()` / `::len()` / `::is_empty()` / `::frame()` outside `src/tel/log.rs`**

- `all()` — NO callers anywhere (`src/tel/log.rs:47`). Repo-wide `.all()` hits are unrelated types: `src/pgwire/extended.rs:383` (`session_params`), `tests/provenance_scan_cascade.rs:99,104`, `tests/adv_f4_false_refusal.rs:94,99`, `tests/adv_f6_dropped_capture.rs:95`, `tests/provenance_retention_wiring.rs:105,110` (test row-store helpers).
- `len()` — ONE caller outside `log.rs`: `tests/integration_effect_log.rs:49`. Inside `log.rs`'s own tests: `:174`, `:182`, `:190`.
- `is_empty()` — NO callers (`src/tel/log.rs:55`). `src/consensus/node.rs:399` and `src/consensus/tests_log.rs:394,412` are the raft log.
- `frame(branch, txn)` — NO callers (`src/tel/log.rs:60`). The only `.frame(` hits repo-wide are `src/consensus/log.rs:559,564,757`, a different method on the raft log.

Everything else holds the log as `Arc<dyn EffectLog>`, which erases those inherent methods: `src/agent_sql/runtime.rs:362,391,395,440,477,639`; `src/agent_sql/merge_engine.rs:326,330`; `src/tel/engine.rs:306,314`; `src/cli/cli.rs:119,125`; and the many `Arc::new(MemEffectLog::new())` sites in `tests/` (e.g. `tests/integration_simulate.rs:76,1157`, `tests/integration_runtime_reattach.rs:63,72,156,221`, `tests/prop_merge_outcomes.rs:63,169`, `tests/adv_f5_probe.rs:63`, `tests/guard_precondition_probe.rs:218`, `tests/integration_branch_pages.rs:72`, `tests/integration_zero_copy_fork.rs:45`, `tests/integration_runtime_concurrency.rs:47`, `tests/integration_trunk_tree_authority.rs:80`, `tests/integration_merge_agreement.rs:69`).

Also relevant: `DEMO.md:231` documents "The effect log is `MemEffectLog`, which is a memory implementation rather than a durable one".