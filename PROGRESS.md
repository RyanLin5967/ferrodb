# F9 — agent isolation on a cluster — COMPLETE

## Done
- `src/agent_sql/cluster.rs` (new, ~1250 lines) + one `pub mod` line in `src/agent_sql/mod.rs`.
  `runtime.rs` was **not** touched: everything F9 needed was already public.
- `tests/integration_cluster_agents.rs` — 35 tests.
- One allowlist entry in `tests/integration_server_stdout.rs`, with its reason, for the repo-wide
  guard against tests that bind their own socket. The guard was forced to fire with the entry in
  place and shown quiet again.
- 28 mutants, every one killed by the test named against its rule
  (`scratchpad/F9-mutants.py`, transcript `scratchpad/F9-mutants.md`).
- Whole suite, per target: `scratchpad/F9-suite.txt` — **1730 Rust tests across 86 targets, plus
  97 Go**. Baseline was 1695 Rust / 97 Go; this row adds exactly its own 35.

## Doing right now
- Nothing. The row is finished; the summary is `scratchpad/F9-agents-cluster.md`.

## Single next action
- None outstanding in this worktree. The branch is `F9-agents-cluster`, committed, never pushed.
