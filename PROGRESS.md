# F9 — agent isolation on a cluster

## Done
- Read DISTRIBUTED.md (§F9 + "What happens to an agent's in-flight branch when its node dies"),
  the frozen `src/consensus/mod.rs` contract, `src/cluster/mod.rs` (F4), `src/consensus/node.rs`
  (the driver + `Applier`), `AgentRuntime::{evaluate_merge, publish_evaluation, merge}`, the
  branch catalogs' id minting, and the existing test templates.

## Doing right now
- Writing `src/agent_sql/cluster.rs`.

## Single next action
- Write `src/agent_sql/cluster.rs`, then `pub mod cluster;` in `src/agent_sql/mod.rs`, then build.
