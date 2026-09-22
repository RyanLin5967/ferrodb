# D92-merge3-fix — state

Branch `D92-merge3-fix`, worktree `/Users/idide/wt/ferrodb-D92-merge3-fix`.
**Rebased onto main past D105/D108–D112; eight commits, `main..HEAD`.** Earlier unreviewed residue
is on `D92-merge3-fix-quarantined` and is superseded — do not merge it.

⚠ **This branch is HYGIENE on a RETIRED module.** `cow::merge3` has zero production callers:
`MERGE` has a delta in hand and `DIFF` does not, and a `MERGE` reads zero branch-engine pages.
Land it as hygiene; do not expand it or treat it as groundwork for wiring merge3 up.

## `diff.rs` co-edit — resolved
The earlier "do not rebase until `fix-diff-memo` lands" block is **cleared**; `fddf13c` landed and
this branch is rebased past it. Both conflicts were resolved by taking **main's** side: my
"A STAMP GOES STALE" warning and my `MemoIdentity` note were superseded by better text that names
the real key and carries measured page-read counts.

What is left of this branch in `src/cow/diff.rs` is **39 insertions, 0 deletions** against current
main, and only two things: the `IdentityProof` enum, and `NodeIdentity::proof()` **with a
`Fingerprint` default** so it cannot break foreign impls. The explicit `Fingerprint` impls on
`SubtreeHash` and `MemoIdentity` were dropped as redundant with that default.

⚠ The shipped memo key is **`(birth_epoch, checksum)`** (`cow::diff::PageVersion`), NOT
`(PageId, birth_epoch)`. It catches routes A and B; route C stays open by construction. Anything
on this branch that names the old candidate is describing a rejected proposal, not live advice.

## The four findings
1. **Memo keyed on a reused `PageId`.** Resolved by deletion: merge3 owns no memo. ⚠ **Not**
   resolved by consolidating onto `cow::diff` — `SubtreeHash` and `MemoIdentity` are keyed on
   `PageId` too. Reproduced and pinned by
   `merge3::tests::a_stale_subtree_hash_makes_merge3_drop_a_change_silently`.
   ⚠ **`(PageId, birth_epoch)` would have caught one route of three** — a recycled id yes, an
   in-place write no, an in-place write to a *descendant* no. Measured in
   `birth_epoch_discriminates_a_recycled_page_but_not_an_in_place_write`.
   ✅ **The key `fix-diff-memo` actually landed is `(birth_epoch, checksum)`, which catches two of
   three**: crc32 covers the header too, so it moves on an in-place write where `birth_epoch` does
   not. Route C — an in-place write to a *descendant* — is unreachable by any per-page key and is
   now **asserted** as a limit on its side, not merely documented. See
   `bench/d92_content_identity_trade.txt`'s second addendum. The merge3 store-property test stays
   as it is: it measures `birth_epoch`, which genuinely does not discriminate B or C, and it is
   deliberately not duplicated into `diff.rs`.
2. **Sole authority / duplicate hasher.** `H128` deleted. `NodeIdentity::proof()` added with no
   default; rides out on `MergeResult::identity_proof`.
3. **`read_node`'s `_ =>` arm.** Now `cid::shape_of`. Fire-checked: the pre-fix arm followed a
   zeroed `Heap` page's leftmost child to page 0.
4. **`cid.rs`'s "grep returned zero".** Corrected, and restated as a count rather than a command.

## Verification
- Counts below are PENDING re-measurement after the rebase onto current main — the 1543 figure
  was taken at base `1758b3b` and does not name this tip. Re-run `cargo test --lib cow` and
  record the literal line here.
- Fire-checks, all taken pre-rebase and all still expected to hold: the `read_node` guard fails
  against the restored `_ =>` catch-all; the store-property test fails when route B's premise is
  flipped; `proof()` without a default reproduces `error[E0046]` in `tests/review_cow_adv.rs:368`.
- ⚠ `a_stale_subtree_hash_makes_merge3_drop_a_change_silently` was a PINNED HAZARD and its own doc
  says to delete it once `cow::diff` grew a staleness guard. `fddf13c` is that guard, so this test
  is **expected to fail now** and must be converted into a guard that the fix HOLDS, not deleted
  outright. Confirm by running it before changing it.

## Open, and NOT mine
- `examples/d92_merge3_curve.rs` was **red on main** at `1758b3b`, verified by running it there
  with none of this branch applied (rule 2 contested = 6, asserted 0; `cow::chunker`'s
  content-defined leaf boundaries changed which rules can fire). ⚠ Main has moved a long way
  since — **re-check before repeating the claim**, and route it to the chunker author.
- Route C of the memo staleness: an in-place write to a *descendant* leaves every ancestor
  byte-identical, so no per-page key reaches it. Asserted as a limit on `cow::diff`'s side.
