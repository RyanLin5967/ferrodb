# D101 — resume state

Written before the long measurement sweep, because this agent was already killed once by the
weekly rate limit mid-run and a fleet watchdog quarantine-committed the residue.

- **Worktree**: `/Users/idide/wt/ferrodb-D101-stub-runtime-guard`
- **Branch**: `D101-stub-runtime-guard`, based on `fe5df25`.
- **Main has moved** to `08e0ec2` (69+ commits). Exactly one of them touches files this branch
  also touches: `08e0ec2` edits `src/agent_sql/runtime.rs` (deletes the orphaned `visible_rows`
  wrapper and the `encode_row` import) and `examples/d68_merge_is_o_table.rs` (drops three
  orphaned constants, retains `reader_thread`). Expect a small conflict in d68 at merge time;
  nothing touches the guard's seams (`dispatch.rs`, `session.rs`, `pgwire/mod.rs`).
- **Do NOT merge or push.** Not authorised for this task.

## Done and committed

| commit | what |
|---|---|
| `ca61e38` | The guard: `agent_sql::designated`, `ServerContext::new` designates, `run_agent_stmt` refuses, `ServerContext::session()`. Plus `tests/d101_designated_runtime.rs` and `tests/d101_no_server_context.rs`. |
| `fc443c4` | The five harnesses run on the runtime they build (`ctx.session()`); d71 given the `cli.rs`/d90 wiring it never had. |
| `964ad16` | Eleven banked bench files annotated; `d68` line 3 corrected in place. |
| `84f40aa` | Arena + `checkpoint_to` armed in all five; runner gated on per-block `rc`. |
| `da3a01e` | Measure-lock heartbeat per block. |

## Verification already banked (re-quote, do not re-derive)

- Guard **fires**: mutation removing `designated::check(&runtime)?` from `dispatch.rs` makes
  `an_agent_statement_on_an_undesignated_runtime_is_refused` FAIL — `BEGIN AGENT SESSION`
  succeeds on the stub. Mutation making `check` always-`Err` makes both permitting tests FAIL.
- Guard **does not fire spuriously**: `cargo test --lib` = **1431 passed, 0 failed**;
  d54_as_of_in_session (1), d54_reader_writer_handshake (3), integration_cli_agent_isolation (6),
  integration_simulate (25), provenance_e2e (5) all green.
- The **unmodified pre-fix d55** binary, under the guard, is refused by name at
  `BEGIN AGENT SESSION` — direct evidence the banked run measured a stub.

## The single next action

Run `bench/d101_rerun.sh` (it acquires the measure lock itself) and then build the old-vs-new
tables. Output goes to `bench/d101_rerun.txt`; the previous, VOID sweep is preserved as
`bench/d101_rerun_VOID_disk_emergency.txt` — **do not quote a number from that file**, a disk
emergency deleted `target/` mid-run and eleven blocks exited 127.

Banked baselines to compare against (all now carry a D101 CORRECTION head block):

- d55 → `bench/d55_after_quick.txt`, AFTER column: 1T 2216, 2T 4704, 4T 8529, 8T 16017, 16T 27359 stmt/s.
- d56 → `bench/d56_plan_curve.txt`, FIX rows. agent medians 1000/4000/16000: r1 145017/141555/135054.
- d67 → `bench/d67_merge_contention.txt` used a BROKEN counter (credited quarantined merges).
  The corrected-counter baseline is `bench/d67_recount.txt`: disjoint 1T 70.9, 2T 43.0, 4T 60.9,
  8T 24.4, 16T 19.0 merges/sec. `bench/d67_recount_long.txt` is an ABORTED run with no numbers
  and says so itself — do not quote it.
- d68 → `bench/d68_merge_is_o_table.txt`: 1000 6.000ms, 2000 7.149, 4000 10.566, 8000 17.050,
  16000 32.003; ms/1000rows converging to 2.00.
- d71 → `bench/d71_point_update_curve.txt`: PLAIN 4.032/4.138/4.093/5.042/5.396 ms;
  STAGED 0.185/0.342/0.688/1.421/2.385 ms.

⚠ Durations on this box are **upper bounds** — it runs a 13-agent fleet at load 20-60. Counters
(fsyncs, bytes/fsync, applied merges) are not. Every block stamps load before and after.

## ⚠ THE CONFOUND — read before writing any old-vs-new table

**The banked runs and this re-run are not at the same commit, and the gap is ~60 commits.** The
banked files were produced at `991d6e0`, `3cd3034`, `a8fc8e6` and similar; this branch is
`fe5df25` + D101. Between them sits, among much else, **D71's predicate pushdown**, which
`bench/d71_pushdown_fix.txt` claims took the staged write path from O(table) to a descent.

So a raw old-vs-new delta conflates THREE causes and cannot be attributed to any one of them:
1. stub runtime → the runtime the harness builds (D101),
2. persistence off → `checkpoint_to` armed (D101),
3. ~60 commits of engine change, including a fix that deliberately changed one of these curves.

A partial d55 QUICK run (killed deliberately, not a result) read **1T 123,102 stmt/s against the
banked 2,216** — a 55x gap that no configuration fix produces. That is what surfaced this.

**The instrument that survives it** is each harness's own within-run control, because the control
arm does not touch the agent runtime and therefore absorbs engine and box drift equally:

| harness | control arm (runtime-independent) | treatment arm | quantity to compare |
|---|---|---|---|
| d71 | PLAIN (ordinary UPDATE) | STAGED | STAGED/PLAIN per size |
| d56 | `arm=plain` | `arm=agent` | agent/plain per size |
| d55 | PRIVATE (N ServerContexts) | SHARED (one) | shared/private per thread count |
| d68 | "CONTROL writes" column | "MERGE" column | MERGE/writes per size |
| d67 | **none — every arm is agent work** | — | shape only; say so |

Banked within-run ratios, precomputed:
- d71 STAGED/PLAIN: 0.046, 0.083, 0.168, 0.282, 0.442 over 1k→16k. Rises ~10x over 16x rows,
  which IS the O(table)-vs-flat conclusion stated drift-free.
- d56 agent/plain: 0.878, 0.903, 0.906 (r1); 0.923, 0.885, 0.909 (r2); 0.880, 0.882, 0.872 (r3).
  Flat across size — the agent read path tracks the plain one.

Report the raw old-vs-new side by side as asked, then the ratio, and say plainly which of the
three causes the evidence can and cannot separate.

⚠ Do **not** assert the five conclusions were wrong. D71's is a complexity class and plausibly
survives; D68's mechanism (`evaluate_merge` scanning the shared table) runs in both
configurations. Let the numbers decide and report honestly when a conclusion holds.
