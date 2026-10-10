# Completed-atlas branch/COW/GC review

Reviewed `atlas.json` sections `system`, `fork`, `pages`, and `lifetime`, plus their related glossary, persistence, agent, and extension descriptions against source revision `fc9556a742f61884a3011ccc28e78f160a8d57fe` and `branch-cow-gc.md`.

## Required correction

**`lifetime.diagram.nodes[id=fence].lines`: "Bump generation; release slot" is unconditional, but ID-slot reuse is conditional.**

- `src/branch/table_catalog.rs:1312` only adds a reaped ID to the reusable-ID index if `has_live_children(id)` is false; it retains a slot protecting live descendants.
- The diagram correctly says "Retain needed ancestor links," but an unconditional "release slot" alongside that can imply another branch can reuse the same numeric identity while the old descendants still use it for ownership/pinning decisions.
- Suggested line: **"Bump generation; reuse only if safe"**. Detail: **"The ID slot is eligible for reuse only after the catalog finds no live descendants; an interior reaped branch can remain as a lineage pin."**

## Pedagogical improvements

1. **`fork`: graph topology is correct, but the parent visually reverses its left/right children after the edit.** Before: page 12 is left, page 19 is right. After: original page 19 is left of shared page 12. The original root's child IDs are still correctly labeled 12 and 19, so this is not a wrong-reference bug. Because the example says "right leaf" and shows a sorted B+ tree, keep page 12 visually left of page 19 in both states, or explicitly label the outgoing edges with their unchanged separator/key ranges. The intended adjacency is verified: parent→7→{12,19}; child→30→{12,31}.
2. **`pages`: distinguish physical containment from traversal explicitly.** Header→internal-payload means fields in one page; internal-payload→leaf means traversal to another page. Add a short note: **"Each internal or leaf node occupies its own page with its own header; internal and leaf payloads are alternatives."** Current edge labels are plausible but the vertical chain can look like all three boxes form a single page.
3. **`fork` privacy card: make the equation explicit.** Current description is accurate but vague about "meet the privacy barrier." Prefer **"Writer owns the arena AND birth_epoch ≥ max(own fork epoch, latest live child fork epoch)."** The latest-child term is omitted if no live child exists (`arena.rs:123`, `2008`). This is a useful exact rule with minimal prose.
4. **`lifetime` lease card: qualify local wall time when covering distributed components.** Standalone uses local wall clock; configured cluster members use an applied cluster time tick and refuse decisions before knowing cluster time (`cluster/mod.rs:319`). Suggested wording: **"Time deadline for abandoned work: local wall time in standalone mode, replicated cluster time in clustered components."** This is a boundary precision improvement, not an error for the normal single-node SQL path.

## Passed checks

- The fork example copies data leaf 19→31 and shared ancestor 7→30; it keeps leaf 12 shared and changes only the child branch's COW root. It expressly excludes splits and boundary repair.
- Workspace root sharing is separated from disk-page sharing. Parent private changes remain isolated without claiming that ordinary heap tables are frozen at fork.
- The overall system shows current committed heap reads plus private overlay, with staged state mirrored separately to COW pages. SQL MERGE publishes heap rows rather than replacing the trunk COW root.
- COW header offsets, child IDs in payload, root IDs in catalog records, row-key encoding, no COW sibling links, and separate ordinary-index formats match source.
- In-place mutation is not confused with always-copy shadow paging; private ownership and descendant sharing are named.
- Metadata/catalog root identity is separate from each branch COW root. Separate catalog file/pool and main-file arena payloads are represented.
- Epoch interval `[birth, free)` and example `3 ≤ 5 < 9` are correct. Descendant protection through reaped interior ancestors is preserved.
- Reaping state is persisted before freeing; pending reclamation, restart cleanup, generation fences, default lease/scan periods, and separation from MVCC vacuum are represented.
- Page diff, structural three-way merge, semantic SQL merge, fingerprints, unwired dedup, and page-delta utilities remain distinct.
- No cheaper-than-normal-transactions claim or universal branch durability claim is made.

Review result: **one required label correction; four precision/clarity improvements. No incorrect page-ID edge found.** No application/model files were edited by this reviewer and no new tests were run during this pass.

## Acceptance addendum — revised fork/lifetime and new COW-engine section

Reviewed only the revised `fork` and `lifetime` sections and new `cow-engine` section in the completed `atlas.json`; no renewed broad source audit was needed.

- **Required correction resolved.** The reaped-state node now says "Bump generation; reuse if safe" and explicitly retains slots needed by live descendants.
- **Fork visual order corrected.** Page 12 remains the left/shared leaf; original page 19 and modified page 31 remain right-side alternatives. The child-to-shared-page-12 edge routes below and lands at page 12. All before/after page references remain correct.
- **Exact COW decision represented.** The new engine section states arena ownership AND `birth_epoch >= max(branch fork epoch, latest live child fork epoch)`, and explains that the stored private flag is insufficient.
- **Engine flow accepted.** Root lookup, separator descent, cached/pinned pages, leaf search, private in-place writes, shared-page shadowing, split/boundary repair, ancestor relinking, conditional root update, and operation-local rollback are represented with appropriate arrows and qualifications.
- **Durability boundary preserved.** The new section does not turn the local rollback journal or catalog root publication into a cross-store durable transaction.
- **Lease qualification resolved.** Standalone wall time and replicated clustered-component time are distinguished.

**Accepted for these reviewed sections. No further blocking accuracy findings.** Earlier optional page-containment wording is outside this narrowly requested recheck. No model/application edits or additional tests were performed by this reviewer.
