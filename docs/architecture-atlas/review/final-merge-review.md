# Final agent / merge accuracy review

Reviewed `atlas.json` against source `fc9556a742f61884a3011ccc28e78f160a8d57fe` and `review/agent-merge.md`.
Scope: all node labels, expanded details, captions, cards and arrows in sections `system`, `agent`, `merge`, and `boundaries`; relevant fork/persistence descriptions and the glossary. Distributed/WAL internals outside my source-review scope are not independently certified here. No model edits or tests performed.

## Required correction

### M1 — provenance is written on both sides of row commit

- Identifier: `sections[id=merge].diagram.nodes[id=record].detail[0]`.
- Current sentence: “Applied-operation history and provenance recording happen after row commit.”
- Problem: this groups all provenance writes after commit. Physical version-location authorship is stamped from the insert/update operators during row application, before commit (`src/execution/insert.rs:147`, `src/execution/update.rs:97`, `src/agent_sql/runtime.rs:5202`). Logical row authorship is stamped after commit by `record_applied` (`runtime.rs:5428`). Neither sidecar action is part of the row WAL atomicity guarantee.
- Suggested replacement: “Applied-operation history and logical row authorship are recorded after row commit. Physical version-author stamps may already have been written during row application. Provenance writes have separate durability; sealing follows, and a later failure cannot retroactively undo committed rows.”
- Related card `sections[id=merge].cards[title="What is not atomic together"]` remains correct, but may say “provenance writes/history recording” to avoid implying one discrete provenance stage.

## Small clarity improvements

### M2 — target state mixes disk and RAM in its label

- Identifier: `sections[id=merge].diagram.nodes[id=target]`.
- Title says “Current target heap”; its lines also contain applied effects, runtime versions and table counters. Those do not all live in heap pages.
- Suggested title: “Current target state”. Existing detail can specify “Heap rows + runtime operation/version history + catalog counters”.
- Evidence: `runtime.rs:4495`, `runtime.rs:5308`, `runtime.rs:5351`.

### M3 — combined extension box has an overly narrow outgoing label

- Identifier: `sections[id=boundaries].diagram.edges[from=simulate,to=core].label`.
- The source box covers both SIMULATE and cherry-pick/selected effects, but arrow says “same evaluator”. SIMULATE uses `evaluate_merge`; cherry-pick goes through branch staging, not that evaluator.
- Suggested label: “reuse core paths”, or split the label into “simulate: evaluate / pick: stage”.
- Evidence: `src/agent_sql/simulate.rs:512`, `src/agent_sql/runtime.rs:3488`.

## Verified accurate within scope

- Branch SQL reads current committed heap + private overlay; no whole-database fork snapshot claim.
- The workspace/frame is live merge authority; TEL is appended separately and not automatically replayed into unfinished workspaces.
- Staging order is workspace → TEL → COW mirror, with an explicit cross-store failure boundary.
- SQL capture recognizes same-column plus/minus literal as Add; other expressions may become Assign.
- SQL MERGE reconciles effects and writes accepted rows to ordinary heap; no trunk-root swap claim.
- First-touch witnesses and fork sequence are distinguished; ordinary unrecorded changes are cautiously described as potentially opaque Assign.
- Guards use pre-state; supplied assertions use proposed post-state; ordinary MERGE supplies no explicit assertions.
- Predicate-only reads are not automatically quarantined; exact-version and ordinary-write coverage limits are explicit.
- Arithmetic example excludes a prior stale explicit SELECT and retains the possibility of admission refusal.
- Conflict retention is distinguished from production gate quarantine.
- Stale-evaluation fingerprints are distinguished from validating all lifetime reads.
- Only accepted row writes are shown inside the ordinary transaction boundary; DDL and sealing remain outside.
- Structural tree merge is distinguished from production SQL merge; revert is not root rewind or one atomic cascade.
- No general cheaper-than-transactions or complete distributed SQL guarantee is asserted.

Disposition: source-grounded and suitable after M1; M2/M3 improve precision of the diagram itself.

## Acceptance addendum

Verified the corrected `atlas.json`: M1 now distinguishes physical version-author stamps during row application from post-commit logical authorship/history and explicitly excludes those separate writes from row WAL atomicity. M2 is now “Current target state”; M3 is now “reuse core paths”. All three findings are resolved. Accepted within the review scope above; no broad review or tests were repeated.
