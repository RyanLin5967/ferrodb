# FerroDB architecture atlas

Open **[index.html](index.html)** for the complete, offline guide. Select a diagram box for its explanation; expand source evidence to find the implementation. The numbered navigation follows the same change from SQL through storage, branches, merge, recovery and cleanup.

**[ferrodb-core-atlas.pdf](ferrodb-core-atlas.pdf)** is the compact printable diagram guide. The HTML also offers **Print / PDF → With explanations** when a printout of all node details is wanted.

## Reading path

| View | Question answered |
|---|---|
| 01 — System | What are the components, and which ones actually connect? |
| 02 — SQL | How does a statement find a physical row? |
| 03 — MVCC | How can readers see different versions of that row? |
| 04 — Fork | What is shared, copied, and repointed? |
| 05 — COW engine | How does ownership control the actual read/edit/relink algorithm? |
| 06 — Page layouts | What is in a header, payload, pointer, or slot? |
| 07 — Agent reads/writes | Which state is authoritative, and what is mirrored? |
| 08 — Merge | How are intent, conflicts, guards and publication connected? |
| 09 — WAL | What happens at commit and after a crash? |
| 10 — Persistence | Which files survive, and which runtime state does not? |
| 11 — Lifetimes | When can shared pages and branch IDs be reclaimed? |
| 12 — Boundaries | Where do simulation, provenance, CDC and consensus attach? |

The glossary defines the storage and transaction vocabulary. The diagrams deliberately distinguish ordinary SQL indexes, COW trees, immutable workspace maps, row-version chains and metadata catalogs.

## Revision and evidence

The implementation baseline is `fc9556a742f61884a3011ccc28e78f160a8d57fe`. Application source files were not changed to produce this guide.

- `atlas.json`: all diagram nodes, labeled edges, explanatory text and source citations.
- `review/*-review.md`: independent final accuracy reviews and acceptance addenda.
- `review/{sql-mvcc,branch-cow-gc,agent-merge,durability-wiring}.md`: deeper source-tracing evidence.
- `review/core-tests.json` and `.log`: exact focused test command and complete results.
- `review/model-audit.json`: source locations, source hashes and graph checks.
- `review/browser-audit.json`: visual/layout, offline and interaction checks.
- `verification.json`: final completion record; this is written after artifact verification.

The focused test run passed **66 tests across 16 targets**, including process-crash row publication, fork survival after SIGKILL, private-state sharing, merge rules, guards, read premises and snapshot concurrency. Those checks support specific mechanisms; this guide is not a proof of every possible failure or a claim that the full repository test suite was run.

Important current limits are shown where they matter: fresh-base agent reads, serialized SQL writes, partial read validation, separate merge side effects, CLI/server persistence differences, no automatic unfinished-session reconstruction, and separately wired consensus components.

## Rebuild

No runtime packages, network access or web server are needed to open the HTML.

```sh
python3 docs/architecture-atlas/make_model.py
python3 docs/architecture-atlas/build_atlas.py
```

The content generator executes the adjacent `model_*.py` fragments with shared diagram helpers. `build_atlas.py` uses only the Python standard library and rejects estimated text overflow. After rendering, `window.atlasCheckLayout()` checks actual browser glyph bounds, label/node collisions and arrow paths.

For a later source revision, recheck the implementation and source anchors before regenerating. Merely replacing the revision string does not update the model.
