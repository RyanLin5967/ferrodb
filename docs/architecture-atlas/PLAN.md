# FerroDB architecture atlas — construction and verification plan

Source baseline: `fc9556a742f61884a3011ccc28e78f160a8d57fe`.

## Purpose

One offline, navigable artifact that explains the implemented core from public entry points down to page bytes, and connects isolation, merging, durability, and reclamation. The artifact is an explanation of this revision, not a claim of complete correctness or a performance certification.

## Reading order and visual contract

1. System map: entry points, ordinary SQL lane, agent lane, storage and metadata boundaries.
2. Ordinary SQL: statement routing, query execution, index/heap lookup, buffer and disk.
3. Transactions: row-version chains, snapshot visibility, write conflicts, commit and abort.
4. Branches: catalog records, shared persistent workspaces, COW page layouts, fork/edit example.
5. Agent data path: current heap + private overlay, staging order, operation intent and merge.
6. Merge admission/publication: conflict rules, preconditions, read checks, atomic row transaction and surrounding steps.
7. Persistence/recovery: actual files, WAL-before-data ordering, CLI/server differences, restart limits.
8. Lifetimes: epochs, generation fencing, leases, descendant pinning, arena reclamation.
9. Boundaries and glossary: optional features connected to their core dependency; replication/consensus status; vocabulary and evidence.

Use an overview plus focused diagrams, never one illegible graph. Every arrow must have a specific meaning. Use the same colors for the same subsystem. Distinguish RAM, database pages, sidecars, and asynchronous/background actions. Page identifiers are example numbers, not runtime addresses. The root is a role, not an additional node type. Keep source evidence available without placing source-code detail in every diagram node.

## Evidence assignments

- `review/sql-mvcc.md`: ordinary SQL, indexes, page layouts, MVCC, buffer and server concurrency.
- `review/branch-cow-gc.md`: fork, COW, persistent sharing, epochs, allocator and reclamation.
- `review/agent-merge.md`: agent visibility, effect capture, semantic merge, guards and publication.
- `review/durability-wiring.md`: entrypoint construction, durable files, recovery and distribution boundaries.

Each review records source locations, actual behavior, important qualifications, and existing tests. Reports are intermediate evidence, not instructions to the reader. Conflicting findings must be resolved against executable source before rendering the atlas.

## Required distinctions

- Ordinary SQL heap/index trees versus the separate COW branch tree.
- Branch SQL's fresh committed heap + workspace overlay versus a frozen full-database snapshot.
- MVCC snapshot visibility versus COW isolation versus WAL recovery versus TEL operation intent.
- Source read/statement concurrency versus independently staged tasks; the server's actual writer gate.
- In-memory workspace authority versus mirrored COW rows and persisted branch metadata.
- Semantic SQL merge versus separate structural tree merge and auxiliary TEL engine.
- Preconditions before effects versus assertions over proposed results.
- Atomic published rows versus separate DDL/history/lifecycle steps.
- CLI durable effect/provenance stores versus pgserver's configured memory stores.
- Durable artifacts versus automatic recovery of unfinished agent sessions.
- Branch reclamation versus removal of historical heap versions.
- Added branch costs versus unmeasured workload-level benefits; no cheaper-than-transactions claim.
- Physical/logical replication versus Raft components and actual server wiring.

## Completion checks

1. Reconcile the four source reviews into one model and preserve exact references.
2. Run focused existing tests for the represented invariants, including row-publication process-crash behavior. Rebuild any child example binary first. Record failures and limits, not only totals.
3. Render self-contained HTML/SVG with no external runtime dependencies, and export a readable PDF.
4. Check every source link and diagram endpoint, all text bounds and page breaks, keyboard interaction, and local offline loading.
5. Independently review the completed rendered model for missing paths and inaccurate promises. Correct and recheck any change.
6. Record the final revision, test commands/results, artifact validation, and remaining implementation limits in `verification.json` only after the checks are complete.

## Continuity

The source model, evidence reports, and build script are the durable working record. If conversation context changes, reload these artifacts and revalidate the relevant source paths before continuing; do not infer that unrecorded work was completed.
