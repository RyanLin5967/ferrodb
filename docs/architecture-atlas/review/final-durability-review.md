# Final independent review: durability and wiring

Reviewed `atlas.json` at source revision `fc9556a742f61884a3011ccc28e78f160a8d57fe`, principally sections `system`, `wal`, `persistence`, and `boundaries`; also read all other node/card text for conflicting guarantees. Cross-checked the source paths from `durability-wiring.md`. No tests run and no model edits made by this reviewer.

## Result

**Scoped pass, with one minor wording correction recommended.** No blocking architecture error or missing central durability connection found in the reviewed sections. This is a source-consistency review, not a proof of database correctness under every crash or concurrent interleaving.

## Recommended correction

- `boundaries.diagram.nodes[id="wal"].lines[0]` currently says **“Committed records and positions.”** The WAL contains uncommitted physical records as well as commits, aborts, and compensation records; physical replication can ship durable records before their transactions commit. Suggested label: **“Row records + transaction markers.”** Keep CDC's separate label **“Decode committed row changes”**: the decoder must interpret transaction boundaries. Evidence: `src/wal/recovery.rs:77–107` redoes logged work before undoing losers; `src/replication/mod.rs:21–24` limits shipping by the durable watermark, not a committed-transaction filter. The detailed WAL section is already accurate; this change avoids a contradictory shorthand in the extensions view.

## Optional clarity improvement

- `persistence.diagram.nodes[id="boot"]` says “Lock.” Its detail could name **DbLock as the process-exclusion lock**, preventing two independent processes from allocating/writing the same database files. This differs from the per-statement server catalog mutex and per-page latches. Evidence: `src/cli/cli.rs:53–57`; `examples/pgserver.rs:45`. Current wording is not false, but the three meanings of “lock” could be clearer.

## Verified connections and limits

- Main heap/index/COW pages share the main pool/file; branch catalog has its own pool and `.branchcat` file (`src/cli/cli.rs:61,100`; `src/branch/table_catalog.rs:176`). The root-publication arrow correctly addresses main-file pages while retaining the separate durability warning.
- `.arena` contains allocator state, not private row payloads. CLI and pgserver boot create/restore it after ordinary catalog setup (`src/cli/cli.rs:76–120`; `examples/pgserver.rs:85–101`).
- CLI uses durable `.tel` and `.provenance`; pgserver example uses memory stores (`src/cli/cli.rs:141,157`; `examples/pgserver.rs:114,121`; `src/agent_sql/runtime.rs:1400`).
- Reopen creates fresh `State::default()` and does not restore workspaces, escrow, read captures, or applied-operation history (`src/agent_sql/runtime.rs:1397–1405`). Replayed TEL is not the live SQL merge input (`tests/integration_durable_tel.rs:8–19`).
- Ordinary commit synchronizes WAL before success; buffer writeback honors recognized page LSNs (`src/wal/txn.rs:608–619`; `src/buffer/buffer_pool.rs:1221–1225,1425–1441`). Checkpoint and retention-pin wording is scoped correctly.
- Recovery redo, loser undo, and heap-directory repair are correctly ordered. CLI's index rebuild/checkpoint differs from pgserver's open path (`src/wal/recovery.rs:77–125`; `src/cli/cli.rs:65–74`; `examples/pgserver.rs:66–71`).
- The model avoids generalizing heap WAL guarantees to schema, allocation, branch metadata, and COW sidecars. Process kill is correctly distinguished from machine power loss (`tests/integration_crash_safety.rs:12–20`).
- Server writer serialization and concurrent eligible reads are correctly stated (`src/pgwire/extended.rs:355–424`; `src/pgwire/mod.rs:143–184`).
- CDC, physical shipping, and consensus are correctly separated. Normal CLI/pgserver do not construct the consensus coordinator; the cluster's metadata/row-WAL gap remains explicit (`src/agent_sql/cluster.rs:67–80`).
- Bare physical redo's catalog limitation is accurate and explicitly asserted by an existing test (`tests/integration_replication_e2e.rs:255–262`).

## Earlier-conversation claims corrected by the atlas

The model does **not** inherit earlier implications that all entrypoints persist TEL/provenance, that persisted frames automatically restore an unfinished SQL agent session, that SQL merge reads the on-disk TEL, that a branch root switch is a complete durable commit, or that many agents imply parallel SQL write execution. It also makes no blanket cheaper-than-transactions claim. Those corrections should remain visible in the final artifact.

## Acceptance addendum

Verified the requested replacements in `atlas.json`: the extensions WAL node now reads **“Row records + transaction markers”**; startup now names **DbLock** and explicitly explains exclusion of a second process writing the same database. Both review items are resolved. **Accepted within the review scope above; no outstanding corrections.**
