# F11 — the server must actually reap

## Done
- `src/branch/lease_thread.rs` + `src/branch/lease_thread/tests.rs` (11 unit tests) — commit 8c2dfdb.
- `src/cli/cli.rs` + `examples/pgserver.rs` wired: reaper attached, lease scan started — 22cafc1.
- `tests/integration_server_reaps.rs` — 8 tests against the shipped binaries — 121ad74 + the merge row.
- Regression: integration_cli_agent_isolation, integration_server_stdout, integration_readme_commands,
  integration_pgwire, integration_system_views_wire all green after the wiring.

## Doing right now
- The mutant pass: break each rule on purpose, confirm the named test fails, restore.

## Single next action
- Run the 10 mutants listed in `scratchpad/F11-mutants.md`, record what each printed.

## DESIGN NOTES (do not re-derive)
- S2b is stale: `src/cli/cli.rs` and `examples/pgserver.rs` both build `ArenaPageStore`, and
  `pgwire::ServerContext` holds a storage-backed `AgentRuntime`. There IS something to reap.
- The "runtime lock" a merge holds is the pgwire **catalog mutex** (outermost, one statement,
  `pgwire/extended.rs:309` is the single execute site). The lease scan takes that same lock via the
  `RuntimeLock` trait — impl for `ServerContext`, and `CatalogLock` for the CLI. No new lock order.
- `resume_interrupted_reaps` is inherent on `TwoTierReaper`, not on the `Reaper` trait.
- `AgentRuntime::forget_reaped_branches` is called after every reap; it exists for this caller only.
- Clock: `LeaseDeadline::try_now_millis()`; a cluster member with no applied tick REFUSES.
- Durable measurement: `reserved_page_count` is exact whenever `<db>.arena` is read (it only changes
  at an extent event, which is when the map is written). `live_page_count` is exact after a clean
  CLI exit and after a reap's `free_arena` rewrite.
- `DbLock` is NOT released by SIGKILL — the pgserver tests delete `<db>.lock` after killing.
