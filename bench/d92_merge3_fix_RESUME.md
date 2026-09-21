# D92-merge3-fix — state

Branch `D92-merge3-fix`, worktree `/Users/idide/wt/ferrodb-D92-merge3-fix`, rebased on main
`1758b3b`. Earlier unreviewed residue is preserved on `D92-merge3-fix-quarantined` and is
superseded — do not merge it.

## Commits
- `499fe80` findings 3 and 4, plus a first cut at 1 and 2.
- `0c4956d` supersedes that first cut: merge3 owns no identity, no hasher and no memo.

## The four findings
1. **Memo keyed on a reused `PageId`.** Resolved by deletion: merge3 owns no memo. ⚠ **Not**
   resolved by consolidating onto `cow::diff` — `SubtreeHash` and `MemoIdentity` are keyed on
   `PageId` too. Reproduced and pinned by
   `merge3::tests::a_stale_subtree_hash_makes_merge3_drop_a_change_silently`.
   ⚠ **Nor is it resolved by re-keying on `(PageId, birth_epoch)`**, which catches one route of
   three: a recycled id yes, an in-place write no, an in-place write to a *descendant* no — and
   the last cannot be fixed by any per-page key. Measured in
   `birth_epoch_discriminates_a_recycled_page_but_not_an_in_place_write` and in
   `bench/d92_content_identity_trade.txt`'s addendum. `fix-diff-memo` has been told.
2. **Sole authority / duplicate hasher.** `H128` deleted. `NodeIdentity::proof()` added with no
   default; rides out on `MergeResult::identity_proof`.
3. **`read_node`'s `_ =>` arm.** Now `cid::shape_of`. Fire-checked: the pre-fix arm followed a
   zeroed `Heap` page's leftmost child to page 0.
4. **`cid.rs`'s "grep returned zero".** Corrected, and restated as a count rather than a command.

## Verification
- `cargo test --lib`: **1543 passed, 0 failed, 3 ignored**.
- Fire-checks: read_node guard fails against the restored catch-all; the hazard test fails when
  `SubtreeHash`'s memo is bypassed.

## Open, and NOT mine
- `examples/d92_merge3_curve.rs` **fails on main** at `1758b3b`, identically and with identical
  counters (rule 2 contested = 6, asserted 0). `cow::chunker`'s content-defined leaf boundaries
  changed which rules can fire in the "contested" arm; the example's own fire-check assertion
  predates that. Verified by running the example at `1758b3b` with none of this branch applied.
- `cow::diff`'s stamp staleness (finding 1's other half). `SubtreeHash` has **no refresh path** —
  `stamp_inner` returns early on a memo hit — so a caller cannot invalidate, only rebuild.
