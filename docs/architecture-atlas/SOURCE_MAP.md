# Source map for the architecture atlas

Baseline: `fc9556a742f61884a3011ccc28e78f160a8d57fe`.

These are the exact linked anchors used by the diagram. The independent review notes trace additional supporting code.

## 01 — The system, in one view

- Transaction and agent session are separate: [src/execution/session.rs:6](../../src/execution/session.rs#L6)
- Statement routing: [src/execution/executor.rs:173](../../src/execution/executor.rs#L173)
- Heap + workspace read path: [src/agent_sql/runtime.rs:2152](../../src/agent_sql/runtime.rs#L2152)
- Exclusive statement gate: [src/pgwire/mod.rs:143](../../src/pgwire/mod.rs#L143)
- Trunk SQL rows live in heap: [tests/integration_trunk_tree_authority.rs:170](../../tests/integration_trunk_tree_authority.rs#L170)

## 02 — From SQL to a physical row

- Read snapshots: [src/execution/executor.rs:115](../../src/execution/executor.rs#L115)
- Optimization and lowering: [src/optimizer/optimizer.rs:7](../../src/optimizer/optimizer.rs#L7)
- Optimistic index descent: [src/storage/index.rs:347](../../src/storage/index.rs#L347)
- Ordinary index page layouts: [src/storage/index_page.rs:4](../../src/storage/index_page.rs#L4)
- Page fetch: [src/buffer/buffer_pool.rs:747](../../src/buffer/buffer_pool.rs#L747)
- Index rebuilding: [src/wal/recovery.rs:177](../../src/wal/recovery.rs#L177)

## 03 — MVCC: time belongs to the reader

- Snapshot representation: [src/wal/txn.rs:121](../../src/wal/txn.rs#L121)
- Visibility predicate: [src/wal/txn.rs:1113](../../src/wal/txn.rs#L1113)
- Following previous versions: [src/wal/visibility.rs:3](../../src/wal/visibility.rs#L3)
- Creating a new version: [src/execution/update.rs:73](../../src/execution/update.rs#L73)
- Ending a version: [src/execution/delete.rs:47](../../src/execution/delete.rs#L47)
- Version header: [src/storage/tuple.rs:11](../../src/storage/tuple.rs#L11)

## 04 — Fork and shadow paging, with real pointer roles

- Child inherits page root: [src/branch/record.rs:244](../../src/branch/record.rs#L244)
- Share workspace roots: [src/agent_sql/runtime.rs:1737](../../src/agent_sql/runtime.rs#L1737)
- Immutable map clone: [src/agent_sql/persistent_map.rs:190](../../src/agent_sql/persistent_map.rs#L190)
- COW decision: [src/branch/arena.rs:2008](../../src/branch/arena.rs#L2008)
- Relink ancestors: [src/cow/btree.rs:942](../../src/cow/btree.rs#L942)
- Record new root: [src/branch/table_catalog.rs:1124](../../src/branch/table_catalog.rs#L1124)

## 05 — The COW engine: lookup, ownership, edit, relink

- Tree lookup: [src/cow/btree.rs:214](../../src/cow/btree.rs#L214)
- Tree insertion: [src/cow/btree.rs:466](../../src/cow/btree.rs#L466)
- Ownership/privacy decision: [src/branch/arena.rs:2008](../../src/branch/arena.rs#L2008)
- Leaf boundary/split logic: [src/cow/btree.rs:787](../../src/cow/btree.rs#L787)
- Ancestor relinking: [src/cow/btree.rs:942](../../src/cow/btree.rs#L942)
- Operation journal: [src/cow/btree.rs:147](../../src/cow/btree.rs#L147)
- Publish changed root: [src/agent_sql/runtime.rs:1486](../../src/agent_sql/runtime.rs#L1486)

## 06 — Inside the pages: three layouts, three purposes

- COW header bytes: [src/cow/page_header.rs:9](../../src/cow/page_header.rs#L9)
- COW payload: [src/cow/node.rs:19](../../src/cow/node.rs#L19)
- Encoded row key: [src/agent_sql/paged_rows.rs:36](../../src/agent_sql/paged_rows.rs#L36)
- Ordinary index headers: [src/storage/index_page.rs:35](../../src/storage/index_page.rs#L35)
- Heap layout: [src/storage/heap_page.rs:19](../../src/storage/heap_page.rs#L19)
- Version prefix: [src/storage/tuple.rs:11](../../src/storage/tuple.rs#L11)
- Logical row identity: [src/agent_sql/runtime.rs:299](../../src/agent_sql/runtime.rs#L299)

## 07 — What an agent actually reads and writes

- Read overlay: [src/agent_sql/runtime.rs:2152](../../src/agent_sql/runtime.rs#L2152)
- Agent SELECT scope: [src/agent_sql/runtime.rs:2250](../../src/agent_sql/runtime.rs#L2250)
- Staging sequence: [src/agent_sql/runtime.rs:2990](../../src/agent_sql/runtime.rs#L2990)
- Operation capture: [src/agent_sql/runtime.rs:6856](../../src/agent_sql/runtime.rs#L6856)
- Frame extension: [src/tel/log.rs:1625](../../src/tel/log.rs#L1625)
- Live merge versus durable log: [tests/integration_durable_tel.rs:8](../../tests/integration_durable_tel.rs#L8)

## 08 — Merge means reconciliation, then publication

- Operation resolution: [src/agent_sql/merge_engine.rs:199](../../src/agent_sql/merge_engine.rs#L199)
- Evaluation: [src/agent_sql/runtime.rs:4269](../../src/agent_sql/runtime.rs#L4269)
- Guard pre-state: [src/agent_sql/runtime.rs:4562](../../src/agent_sql/runtime.rs#L4562)
- Read-premise finding: [src/agent_sql/gate.rs:278](../../src/agent_sql/gate.rs#L278)
- Publication stages: [src/agent_sql/runtime.rs:4894](../../src/agent_sql/runtime.rs#L4894)
- Atomic row transaction: [src/agent_sql/runtime.rs:5168](../../src/agent_sql/runtime.rs#L5168)
- Weak guard can allow negative result: [tests/guard_precondition_probe.rs:288](../../tests/guard_precondition_probe.rs#L288)

## 09 — WAL, commit, crash recovery

- Commit flush ordering: [src/wal/txn.rs:608](../../src/wal/txn.rs#L608)
- WAL-before-page gate: [src/buffer/buffer_pool.rs:1425](../../src/buffer/buffer_pool.rs#L1425)
- Redo and undo: [src/wal/recovery.rs:5](../../src/wal/recovery.rs#L5)
- Compensation-based abort: [src/wal/txn.rs:651](../../src/wal/txn.rs#L651)
- Checkpoint: [src/wal/txn.rs:802](../../src/wal/txn.rs#L802)
- Tree write journal: [src/cow/btree.rs:147](../../src/cow/btree.rs#L147)
- Process-crash test scope: [tests/integration_crash_safety.rs:12](../../tests/integration_crash_safety.rs#L12)

## 10 — What is on disk, and what reopening restores

- CLI construction: [src/cli/cli.rs:46](../../src/cli/cli.rs#L46)
- Server construction: [examples/pgserver.rs:61](../../examples/pgserver.rs#L61)
- Memory effect log wiring: [examples/pgserver.rs:112](../../examples/pgserver.rs#L112)
- Separate catalog pool: [src/branch/table_catalog.rs:176](../../src/branch/table_catalog.rs#L176)
- Allocator persistence: [src/branch/arena.rs:1466](../../src/branch/arena.rs#L1466)
- Reopen creates fresh runtime state: [src/agent_sql/runtime.rs:1381](../../src/agent_sql/runtime.rs#L1381)
- Provenance file contents: [src/provenance/durable.rs:38](../../src/provenance/durable.rs#L38)

## 11 — Who owns a page, and when it can be freed

- Reap stages: [src/branch/reaper.rs:619](../../src/branch/reaper.rs#L619)
- Preserve descendant links: [src/branch/reaper.rs:242](../../src/branch/reaper.rs#L242)
- Half-open lifetime interval: [src/branch/record.rs:1267](../../src/branch/record.rs#L1267)
- Pending versus immediate free: [src/branch/arena.rs:2132](../../src/branch/arena.rs#L2132)
- Arena growth: [src/branch/types.rs:271](../../src/branch/types.rs#L271)
- Background interval: [src/branch/lease_thread.rs:120](../../src/branch/lease_thread.rs#L120)
- Default lease: [src/agent_sql/runtime.rs:96](../../src/agent_sql/runtime.rs#L96)

## 12 — Extensions connect to the core at specific points

- Candidate workflow: [src/agent_sql/simulate.rs:367](../../src/agent_sql/simulate.rs#L367)
- Dependency-aware revert: [src/agent_sql/runtime.rs:5791](../../src/agent_sql/runtime.rs#L5791)
- Runtime page diff: [src/agent_sql/runtime.rs:2094](../../src/agent_sql/runtime.rs#L2094)
- Structural merge library: [src/cow/merge3.rs:303](../../src/cow/merge3.rs#L303)
- Replication and CDC components: [src/replication/mod.rs:1](../../src/replication/mod.rs#L1)
- Cluster atomicity boundary: [src/agent_sql/cluster.rs:67](../../src/agent_sql/cluster.rs#L67)
- Catalog replication boundary: [tests/integration_replication_e2e.rs:255](../../tests/integration_replication_e2e.rs#L255)

## Verification artifacts

- [Focused test command and results](review/core-tests.json)
- [Full focused-test output](review/core-tests.log)
- [Browser checks](review/browser-audit.json)
- [Final completion record](verification.json)
