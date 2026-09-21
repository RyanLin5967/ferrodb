# D92-merge3-fix — resume state

Branch `D92-merge3-fix`, worktree `/Users/idide/wt/ferrodb-D92-merge3-fix`.
Quarantined earlier residue is on branch `D92-merge3-fix-quarantined` (unreviewed, superseded).

## Done
- `499fe80` (rebased onto main `1758b3b`): findings 3 and 4, plus a first cut at 1 and 2.
  - finding 3: `read_node`'s `_ =>` arm replaced with `cid::shape_of`. Fire-checked.
  - finding 4: `cid.rs`'s "grep returned zero" claim corrected.
  - finding 1: memo scoped to one `merge3` call via `begin_merge`. Fire-checked.
  - finding 2: `H128` deleted, `MerkleId` computes `cid::leaf_cid`/`cid::internal_cid`.

## Next action (single)
Consolidate merge3 onto `src/cow/diff.rs`'s identity machinery, per the lead's update:
delete merge3's `PageIdentity` trait, `ShadowId` and `MerkleId`; use `diff::NodeIdentity`,
`diff::PageIdentity`, `diff::SubtreeHash`.

## Corrections owed to the lead (verified, not yet reported)
1. "Consolidating resolves finding 1 outright" is **false**. `diff::SubtreeHash` and
   `diff::MemoIdentity` are *also* keyed on `PageId`, so the staleness defect rides along.
   Fix belongs at the single authority in `diff.rs`.
2. "ONE hasher in `src/cow/`" is no longer reachable by this change: `cid.rs` (`Hasher128`),
   `diff.rs` (`SubtreeHash`/`fold_bytes`) and `chunker.rs` each landed their own. What is
   reachable is **merge3 ships none**.
3. My own `cid.rs` correction text claims "exactly one 128-bit hash in `src/cow/`" — now false
   by the same count. Must fix before landing.

## Measurement that decides the SHA-256 question
merge3's own example already reports, at n=16000 convergent edit:
ShadowId `nodes_read=9`; MerkleId `nodes_read=0` but `pages_hashed=1022`.
Content identity costs ~113x the page reads it saves. Hash *choice* is second-order;
re-run across the size axis and record here before choosing.
