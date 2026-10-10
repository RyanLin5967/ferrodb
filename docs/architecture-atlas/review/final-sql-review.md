# Final SQL/MVCC atlas accuracy review

Reviewed `atlas.json` sections `system`, `sql`, `mvcc`, `pages`; also checked related WAL/persistence/boundary node text and the glossary for contradictions.
Source revision: `fc9556a742f61884a3011ccc28e78f160a8d57fe`. No model edits or tests performed in this review.

## Verdict

The architecture distinctions are accurate and appropriately qualified. Two factual text corrections and the already identified primary-access edge should be completed before publication. No further blocking architectural error found in the reviewed scope.

## Required corrections

1. **`pages.index_header.lines` and `pages.heap_header.lines`: replace `CRC` with `checksum field`.**
   Ordinary heap/index structs contain a checksum field but their constructors initialize it to zero and serialization/deserialization merely copy it. These paths do not demonstrate COW-style CRC computation/validation.
   Add to either detail: “This ordinary format has a checksum field; the shown path does not compute/validate it like the COW header.”
   Evidence: `src/storage/heap_page.rs:48`, `:51`, `:74`; `src/storage/index_page.rs:38`, `:42`, `:62`, `:88`, `:92`.
   Keep CRC in the COW header: `src/cow/page_header.rs:14` explicitly defines CRC32.

2. **`mvcc.rule.detail`: replace `visible_tuple` with `resolve_visibility`, or use plain “the visibility resolver”.**
   The actual function is `src/wal/visibility.rs:3`. The described behavior is otherwise correct.

3. **`sql.diagram.edges`: include Execute → Primary B+ tree as an alternative chosen access path.**
   Root already identified/planned this correction. The existing secondary→primary chain is correct, but alone could imply that every index query starts at a secondary index.
   Evidence: `src/optimizer/optimizer.rs:56` handles the primary index directly when column is 0.

## Useful small clarifications (nonblocking)

- **System overview WAL relationship:** the only visible log edge is Merge → Logs (“row WAL”). Ordinary DML also generates WAL. Add an Ordinary SQL → Logs edge or one short caption stating that both publication and normal DML use ordinary row WAL. View 08 already explains the mechanism correctly.
- **Buffer-pool detail:** normal fetch pins a frame, but optimistic index descent copies validated shadow bytes without a pin. The existing detail mentions optimistic shadows; one extra phrase would prevent “every cache hit pins” from sounding universal. `src/buffer/buffer_pool.rs:714`; `src/storage/index.rs:299`.
- **Page diagrams:** the current COW/internal→leaf and ordinary/internal→leaf arrows are valid examples, but “child IDs eventually lead to” would acknowledge extra internal levels without another node.
- **MVCC example:** “Reader started before B” is correct assuming this reader already sees committed A. Add “sees A, excludes B” if space allows; the detail already states the intended premise.
- **Shared SQL heap label:** “Authoritative committed rows” means committed view of a heap that can also physically contain uncommitted versions. Its detail and MVCC view prevent a material misunderstanding; “Authoritative SQL rows; MVCC filters visibility” would be maximally precise.

## Verified accurate distinctions

- SELECT binding/pushdown/optimization/lowering versus dedicated DML operators, with shared optimization for UPDATE/DELETE row selection.
- Primary PK→RecordId; secondary `(value, PK)`→primary lookup→heap→visibility and visible-value recheck.
- Ordinary tree page layouts versus COW node layouts; ordinary leaf sibling links versus COW traversal stack.
- Physical RecordId `(page,slot)` versus logical agent row identity derived from primary-key values.
- Fixed 24-byte tuple version prefix and separate old-version heap; UPDATE before-image links and DELETE ending transaction.
- Transaction snapshot metadata, own writes, retained explicit-transaction snapshots, current autocommit views.
- MVCC visibility is not full serializability; agent overlay reads do not inherit ordinary BEGIN snapshots.
- Pins, page latches, buffer shadows, row versions and branch COW have distinct purposes.
- Ordinary indexes mutate in place; heap WAL recovery plus index rebuilding differs from per-index-operation redo.
- Current pgwire writes use the exclusive catalog gate and drain shared readers; the atlas does not promise concurrent SQL writers.
- Branch reclamation is not MVCC vacuum. Entry-point recovery differences are stated rather than hidden.
- Glossary correctly distinguishes persistent RAM maps, durable data, logical epochs, physical LSNs and consensus rounds.

## Scope limit

This review checks explanation-to-code agreement, not every SQL isolation/recovery corner case. The atlas's explicit scope qualification correctly avoids promoting the simple snapshot example into proof of all key-reuse/index/rollback combinations.

## Acceptance addendum — 2026-09-23

Re-read the current `atlas.json` after revisions. All required corrections are applied: ordinary checksum-field labels explicitly avoid a CRC-validation guarantee; the visibility helper is named `resolve_visibility`; direct execution→primary-index lookup is drawn. The system overview now states that both ordinary DML and accepted merge rows use WAL.
Normal buffer fetch pinning is acceptable as illustrated; the detail separately identifies optimistic read shadows as a cache/concurrency technique. No further required changes remain from this SQL/MVCC review. Accepted within the scope and limits above; no new tests were run for this addendum.
