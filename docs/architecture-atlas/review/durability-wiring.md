# Durability, boot, server concurrency, and replication evidence

Verified against `fc9556a742f61884a3011ccc28e78f160a8d57fe`. Source inspection only; no tests run by this reviewer. Paths below are relative to repository root. This is evidence for the architecture atlas, not a claim of exhaustive crash correctness.

## Diagram-ready boot and storage map

```text
CLI / pgserver
  │ acquire DbLock: exclude a second process writing the same database
  ├─ main DiskManager + BufferPoolManager ─────────────── <db>
  │    ├─ ordinary catalog, heap, version heap, indexes: below arena floor
  │    └─ ArenaPageStore COW pages: reserved region above arena floor
  ├─ WalManager + TxnManager ─────────────────────────── <db>.wal
  │    └─ recover heap records → open ordinary Catalog
  ├─ TableBranchCatalog ─ separate DiskManager + pool ── <db>.branchcat
  ├─ arena allocator image + append tail ─────────────── <db>.arena
  └─ AgentRuntime + TwoTierReaper + LeaseThread
       ├─ CLI: DurableEffectLog ───────────────────────── <db>.tel
       ├─ CLI: DurableProvenanceStore ─────────────────── <db>.provenance
       └─ pgserver example: effect log + provenance remain in memory
```

- CLI boot is `src/cli/cli.rs:46`: lock at 57; database pool at 61; WAL attached at 64; recovery at 65; catalog open/create at 66; rebuild indexes and checkpoint after nonempty recovery at 71.
- `examples/pgserver.rs:61` opens the same main pool/WAL; calls recovery at 66 and opens catalog at 67, but has no corresponding index-rebuild/checkpoint-on-recovery block.
- Arena comes AFTER ordinary catalog creation to avoid overlapping allocations (`src/cli/cli.rs:76`). Its persisted floor confines ordinary allocation below it and arena allocation above it. CLI chooses current high-water plus `arena_headroom()` at 113; pgserver example hardcodes high-water plus 32,736 pages at `examples/pgserver.rs:98`.
- CLI arms allocator persistence at `src/cli/cli.rs:120`. It opens durable TEL at 141 and durable provenance at 157. These are **not** pgserver defaults: `examples/pgserver.rs:114,121` pass `MemEffectLog`, and runtime constructors default to `MemProvenanceStore`.
- `TableBranchCatalog::open_sidecar` creates its own pool at `src/branch/table_catalog.rs:176`; normal branch metadata file is `.branchcat` at 213. Old `.branches` is a legacy migration input, not the current normal file.

## What each durable representation means

| Representation | Contents and writer | Durability/recovery boundary |
|---|---|---|
| `<db>` | Ordinary catalog, heap rows, old versions, indexes; also arena COW pages | Shared main pool caches both storage regions. Ordinary heap recovery uses WAL; do not infer equivalent WAL coverage for every page type. |
| `<db>.wal` | Physical heap insert/update/delete records, transaction control, compensation records, DDL/run declarations | Commit fsyncs through Commit; recovery redoes heap changes and compensations, then undoes losers. |
| `<db>.branchcat` | Branch records, roots, generations, child/deadline/arena indexes | Separate ordinary B+ tree/pool; mutation stages root metadata, then group flush + disk sync. Not part of the main heap transaction. |
| `<db>.arena` | Allocator floor, extent ownership/allocation state, pending frees; image plus checksummed tail | Extent claims/frees persist; full images use durable replacement. This stores allocator state, **not branch row payloads**. |
| `<db>.tel` (CLI) | Typed task frames: operations, guards, claims; frame extensions | Append + `sync_data` before accepting in-memory index change; reopen replays frames. It does not reconstruct runtime workspaces. |
| `<db>.provenance` (CLI) | Interned run identities, physical version stamps, logical row authors, forgotten tables | Separate append-only store replayed on open. No unified transaction with the heap commit. |
| Runtime memory | Workspaces, names, applied-operation history/index, merge records, escrow, row-version dependency map, read captures | `reopen_with_storage` initializes fresh `State::default()`; no replay restoring these structures. |

Evidence: `src/wal/txn.rs:608`; `src/wal/recovery.rs:77`; `src/branch/table_catalog.rs:347`; `src/branch/arena.rs:1466`; `src/tel/log.rs:1413,1580,1624`; `src/provenance/durable.rs:38`; `src/agent_sql/runtime.rs:814,1397`.

## Ordinary transaction durability flow

```text
Heap change
  → append WAL record, update cached page and page LSN
  → COMMIT: append Commit → WAL flush + sync_data → finish transaction
  → dirty heap pages may be written later

Any dirty page flush/eviction
  → recognized page LSN → flush WAL through that LSN → write page

Restart
  → scan valid WAL records
  → analyze transaction ends
  → redo heap records/CLRs (skip if page LSN already covers record)
  → undo unfinished transactions using compensation log records
  → repair heap page directories
  → CLI rebuilds indexes and checkpoints
```

- Commit is `src/wal/txn.rs:608–619`; WAL bytes reach `sync_data` at `src/wal/log.rs:831–835`. Table pages need not all be flushed at commit.
- WAL flush holds the log buffer lock through disk sync, preventing a later flush from falsely advancing the durable watermark over a missing earlier range (`src/wal/log.rs:803`). This also blocks appends during fsync.
- Buffer flush calls `wal_gate` before data write (`src/buffer/buffer_pool.rs:1221`). `page_lsn_of` recognizes selected ordinary page layouts at 1436; it is not a general journal for COW/allocator metadata.
- Recovery is `src/wal/recovery.rs:5`: redo at 77, undo at 95, directory repair at 110, page-LSN skip at 152. Ordinary transaction abort writes compensation records (`src/wal/txn.rs:651`).
- Checkpoint requires no active transactions (`src/wal/txn.rs:802`). It flushes WAL → pages → database sync → WAL truncation → schema/run redeclarations (`src/wal/txn.rs:851`). Automatic triggering defaults to 256 commits; it is suppressed for clustered mode pending a replicated checkpoint decision (`src/wal/txn.rs:642`).
- WAL pins retain history for consumers/snapshot handoff; a checkpoint is not a license to discard a subscriber's required records (`src/wal/log.rs:760`).

## Branch persistence is several coordinated mechanisms, not one atomic root switch

- Changing a branch root writes its branch record then stages and durably flushes the **branch catalog pool** (`src/branch/table_catalog.rs:1124`). That pool is distinct from the **main pool containing its COW pages**.
- Therefore “set root → the whole branch is crash-atomically durable” is not established. Group-committing branch metadata is not a transaction across branch data, allocator sidecar, TEL, and catalog.
- Fork's durability ticket can be awaited outside the caller's statement lock; completing it still happens before responding to the SQL statement (`src/pgwire/extended.rs:407–422`). A shared fsync may cover multiple staged forks (`src/branch/table_catalog.rs:373`).
- Arena allocator persistence avoids handing out already-owned extents on reopen. It is armed at `src/branch/arena.rs:1466`; new extent records at 2248 onward; frees at 2427. Per-page bump updates are not individually persisted; reopen conservatively clears allocation cursors and resolves unknown extent fill (`src/branch/arena.rs:273`).
- Full allocator checkpoint replacement uses temp write → fsync temp → rename → fsync directory (`src/storage/atomic_file.rs:9`; `src/branch/arena.rs:1827`). This specific guarantee is not a blanket guarantee for all database files; Windows directory-fsync limitation is explicitly documented at `src/storage/atomic_file.rs:50`.
- Bitmap allocation writes metadata directly (`src/storage/disk_manager.rs:433–434,468–482`). Recovery's redo dispatcher handles heap records and CLRs (`src/wal/recovery.rs:77–85`). Thus WAL's row-transaction guarantees must not be generalized into complete power-loss atomicity of allocation, schema, indexes, or branch metadata. This boundary follows the actual write and redo paths; it is not an independently demonstrated failure here.
- The merge process-crash test explicitly **does not simulate machine power loss** (`tests/integration_crash_safety.rs:12–20`).

## What restart can actually recover

1. Open/replay ordinary WAL and reopen persisted ordinary catalog.
2. Reopen branch catalog, restore allocator state, attach to the existing COW trunk root; unreadable/missing root is refused rather than replaced with an empty tree (`src/agent_sql/runtime.rs:1381`).
3. CLI replays TEL and provenance stores; pgserver example does not persist those stores.
4. Lease thread resumes interrupted reaps using shared catalog/store (`src/cli/cli.rs:173`; `examples/pgserver.rs:142`).
5. Runtime workspaces and related live-session state remain empty (`src/agent_sql/runtime.rs:1403`). Durable COW pages/frames **do not mean a previous unfinished SQL agent session can be resumed**.

Particularly important: live SQL `merge/evaluate_merge/diff` read `Workspace.frame`, not `EffectLog::frames_for`. The separate merger can compute from reopened TEL, but normal runtime SQL has not been rebuilt from it. This distinction is spelled out and exercised in `tests/integration_durable_tel.rs:8–19,139–152`. Its test name mentioning a restart must not be taken as full session recovery.

TEL appends cost an fsync per changing agent statement in the CLI (`src/tel/log.rs:1580`). Branches add workspace, log, COW, metadata, and admission costs; **there is no supported general claim they are cheaper than ordinary transactions**. They cheaply share existing branch state relative to copying all data.

## Actual SQL server concurrency

```text
One thread per connection → one shared ServerContext / AgentRuntime
  ├─ eligible read: per-connection catalog snapshot + busy slot → concurrent reads
  └─ other statement: catalog mutex + drain readers → execute → release
       └─ fork's deferred durability ticket: await after release
Lease scan → same exclusive catalog lock
```

- Threads are spawned at `src/pgwire/mod.rs:362`; shared runtime at 100; connection sessions receive that runtime at 436.
- Eligible reads use cached catalog metadata and a per-connection busy slot (`src/pgwire/extended.rs:355–382`). Catalog-cache refresh may itself lock; a writer already announced sends the statement through the exclusive path.
- Exclusive statements acquire one shared catalog mutex and drain active readers (`src/pgwire/mod.rs:143–184`); their execution runs at `src/pgwire/extended.rs:407–416`.
- Fork staging stays under that lock so root/workspace inheritance represent one state; only fsync moves outside it (`src/pgwire/extended.rs:388–405`).
- Therefore multiple agent sessions coexist and fork fsyncs may group, but the normal server does **not** execute ordinary write statements freely in parallel. MVCC supports visibility across transactions; it does not remove this entrypoint's statement serialization.

## Replication / CDC / consensus boundary

```text
Committed durable WAL
  ├─ physical log shipping → ReplicaApplier → heap redo on replica
  └─ LogicalDecoder → committed row events → publication filter → JSONL/CDC consumer
       ↑ optional initial MVCC snapshot + pinned WAL handoff

Separate consensus Node/driver
  → Raft-style ordered commands / durable round log / quorum
  → ClusterAgents coordinates metadata decisions and merge admission
  → accepted row WAL arrives as later WalBatch commands
```

- Physical log source stops at `flushed_lsn` (`src/replication/mod.rs:21`). Asynchronous log shipping itself provides no leader election/failover (`src/replication/mod.rs:9–17`); optional ack waiting is a separate component.
- A base backup is needed when log history is insufficient (`src/replication/mod.rs:32`). Bare physical redo is not a full schema replica: the existing TCP test asserts catalog pages are not replicated (`tests/integration_replication_e2e.rs:255`).
- CDC reads the WAL and emits committed logical row changes; it is not the TEL and does not itself merge branches (`src/replication/mod.rs:36`). Snapshot handoff combines an MVCC boundary and WAL retention/resume position (`src/replication/snapshot.rs:314`).
- Neither normal CLI nor `examples/pgserver.rs` constructs `ClusterAgents`/consensus `Node`. `examples/consensus_node.rs:28` is a separate entrypoint. Show consensus as a separate implemented subsystem, not an automatic path for normal SQL.
- `ClusterAgents` keeps speculative branch contents node-local; branch metadata decisions are replicated. Merge checks the base round, proposes a merge decision, waits for application, and then publishes locally (`src/agent_sql/cluster.rs:1130–1179`).
- **Known gap:** metadata merge and row `WalBatch` are different rounds. A crash between them can leave the cluster recording a sealed merge without its rows (`src/agent_sql/cluster.rs:67–80`). Private branch data is unavailable on a different node; no transparent unfinished-session failover.
- Do not repeat cluster comments' universal claim that no database can preserve uncommitted work. Keeping speculative state node-local is this implementation's choice.

## Existing checks worth linking (not newly executed here)

| Existing test | What it actually establishes |
|---|---|
| `tests/integration_crash_safety.rs:162` | Killing a process mid-row-publication leaves all-before or all-after rows after recovery; not simulated power failure. |
| `tests/d159_fork_sync_is_deferred.rs:58,94,224` | Fork ticket is deferred, multiple forks share fsync, later sync covers earlier staged data. |
| `tests/integration_durable_tel.rs:177` | Frame intent and separate-merger results survive TEL reopen; not restoration of SQL Workspace. |
| `src/tel/tests_durable_log.rs` | TEL format, retry/extension handling, fault injection, torn-tail recovery. |
| `src/branch/arena.rs:2677` | Reopening allocator state must not reissue pages referenced by a durable branch catalog. |
| `tests/integration_replication_e2e.rs:202,262` | TCP heap redo convergence and explicit absence of catalog replication. |
| `tests/integration_cdc_snapshot.rs:86` | Initial snapshot plus feed sees existing/subsequent rows in its fixture. |
| `tests/integration_server_reaps.rs` | Actual CLI/pgserver lease reaping entrypoints, rather than only reaper unit calls. |

## Atlas placement recommendation

- Main overview: common main pool/datafile; **separate branch-catalog pool**; WAL/TEL/provenance distinct; row merge lands in heap.
- Deep durability panel: WAL-before-page ordering and restart sequence; put branch-sidecar boundary beside the root-publication arrow.
- Persistence table: distinguish CLI from pgserver and persisted artifacts from resumed sessions.
- Small peripheral panel: CDC/physical shipping/consensus with dashed edges marking optional/separate wiring.
- Avoid assurances “all writes parallel,” “root update is atomic commit,” “every branch is fully restartable,” “durable TEL drives live SQL merge,” or “branches are cheaper than transactions.”
