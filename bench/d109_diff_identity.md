# D109 — `src/cow/diff.rs` identity soundness

Branch `D109-diff-identity`, worktree `/Users/idide/wt/ferrodb-D109-diff-identity`,
rebased onto `main` at `d6be514`.

Six findings from an adversarial review that did not build or run anything. Every number
below was reproduced here before it was acted on. Two of the six were not what the review
said they were; both are recorded with the correction rather than quietly dropped.

Instrument for every page-read figure: `CountingStore`, the `PageStore` wrapper in
`cow::diff`'s test module that tallies `read_page` calls. Counted, not timed.

---

## 1 — HIGH. The two-store guard could not fire on the case that mattered

**Commit** `Bind MemoIdentity to one tree at construction, not to whichever warmed first`

`MemoIdentity::new(compute)` bound `compute` as a closure over one tree, while `store` was a
`OnceLock` filled from whichever tree called `warm` **first**. So `Arc::ptr_eq` was trivially
true on the first warm, and only a *second* warm from a different store was refused.

A memo whose digest closed over tree A, warmed **once** against tree B, was accepted. Its rows
were `(B's page version, A's digest)`. `id_of` then validated against the right store, found
the row fresh, and returned the wrong file's id — silently, with `misses()` reading zero.

**Reproduced, not read off the source.**
`bench/d109_two_store_guard_before.txt` holds the run. Two fresh stores hand out the same page
id 1 for different content; the memo answered with tree A's digest:

```
assertion `left != right` failed: the memo answered for tree A's page 1 while validating
against tree B's
  left:  [0, 90, 86, 206, 136, 147, 50, 222, 167, 196, 7, 89, 56, 96, 169, 215]
  right: [0, 90, 86, 206, 136, 147, 50, 222, 167, 196, 7, 89, 56, 96, 169, 215]
```

The `assert_eq!(ident.misses(), 0, ...)` above it passed, which is the point: nothing moved.

**Fix — unrepresentable, not checked harder.** The tree is fixed at construction and handed to
the digest on every call:

```rust
pub struct MemoIdentity<'t, F> { tree: &'t CowTree, compute: F, ... }
impl<'t, F: Fn(&CowTree, PageId) -> Result<[u8;16], FerroError>> MemoIdentity<'t, F> {
    pub fn new(tree: &'t CowTree, compute: F) -> Self
    pub fn warm(&self, root: PageId) -> Result<usize, FerroError>   // no tree argument
}
```

The `OnceLock<Arc<dyn PageStore>>` and the `Arc::ptr_eq` check are both gone. A row's version
and its digest come from one file by construction.

**Residual, stated in the type's docs rather than hidden:** a closure may ignore the `&CowTree`
it is handed and capture another. That is a wilful act and it is visible at the call site as an
ignored argument. It is the most a generic adapter over an arbitrary digest can do.

**On the test that was removed.** `warming_one_memo_from_two_stores_is_refused` tested a runtime
path that no longer exists — `warm` has no second tree to reject. It is replaced by
`a_memo_is_bound_to_one_tree_so_its_digest_and_its_version_share_a_file`, which uses the same
two-store fixture and asserts the invariant positively, plus a `compile_fail` doctest on the
two-argument `warm`. A single test cannot span both API shapes, so the before-state is carried
by the banked run above.

**The `compile_fail` was checked not to pass vacuously:** flipped to `no_run` with the correct
one-argument call, it compiles (`- compile ... ok`). So it is failing on the arity, not on
broken scaffolding.

**Call sites changed:** `src/cow/diff.rs` (doc example + 5 test sites),
`tests/integration_production_diff_wiring.rs:357,371`. No caller was getting a wrong id before
this — every one passed a closure over the same tree it warmed. This is an API change, not a
live defect.

---

## 2 — MEDIUM. `MemoIdentity`'s docs did not state the subtree blind spot

**Commit** `State MemoIdentity's subtree blind spot where a caller reads it`

`PageVersion`'s doc asserts that `SubtreeHash` and `MemoIdentity` "repeat it where a caller will
read it". `SubtreeHash` does. `MemoIdentity` did not: its section said only that "a row whose
page has been recycled or rewritten **misses**", which reads as a guarantee that a stale row
never answers.

It is not one. A `MemoIdentity` row holds a **subtree** digest — that is what `cid::subtree_cid`
returns and what this adapter exists to wrap — while being validated against one page's version.
`btree::insert` rewrites an already-private leaf where it lies and stops copying up, so every
ancestor keeps its id, its `birth`, its bytes and therefore its version.

**Reproduced.** `bench/d109_ancestor_row_blind_spot_before.txt`. Asserting the docs as written
(`assert_ne!(id_of(root), stamped_root)`) on a 12-page tree after an in-place leaf rewrite:

```
assertion `left != right` failed: docs as written: a rewritten page's row misses rather than
answering
  left:  [0, 63, 51, 122, 124, 234, 69, 165, 146, 23, 119, 17, 134, 114, 143, 94]
  right: [0, 63, 51, 122, 124, 234, 69, 165, 146, 23, 119, 17, 134, 114, 143, 94]
```

All three premises held before that line: the write did not copy up (`after == root`), the
root's own version was unchanged, and the control digest *had* moved.

**Fix.** The bullet is narrowed to the page *itself*, and the blind spot gets its own section
with the usage rule that actually covers it (warm after the last write; do not let a warmed
provider outlive a write to a branch it warmed).

`an_ancestors_warmed_row_survives_a_descendant_rewrite_and_goes_on_answering` pins it, including
the contrast that makes it a blind spot rather than plain staleness: of 12 pages, **exactly one**
— the rewritten leaf — goes stale, and the root is not it. `misses()` does not move.

**Not re-derived.** Which of the three staleness routes the `(birth, checksum)` key does and does
not discriminate was already measured on the unlanded `D105-diff-memo` by
`the_key_catches_the_recycle_and_the_in_place_write_and_provably_not_a_descendant`. The test here
cites it and pins a different thing: what route C costs a caller of *this adapter*, which is what
the docs got wrong. Neither branch had fixed the doc.

---

## 3 — MEDIUM. `warm`'s `walk_pages` was charged to `subtree_cid`

**Commit** `Charge warm's walk_pages to walk_pages, not to subtree_cid`

The table credited 4325 page reads to "subtree_cid itself" and derived "2.0x the control".
`warm` iterates `tree.walk_pages(root)`, and that walk reads every page once before the digest
is called on any of them — so 4325 was `walk_pages + subtree_cid`.

**Reproduced by direct measurement, not arithmetic on the quoted total.** The test now counts
`walk_pages` and `subtree_cid` separately through the same `CountingStore` and pins the
decomposition as an equality:

```
tree nodes                                 :     1085
diff + PageIdentity, 4 rows changed        :       18 page reads
CowTree::diff, the O(N) path being beaten  :     2188 page reads
SubtreeHash::stamp, bottom-up              :     1085 page reads
MemoIdentity(subtree_cid).warm             :     5410 page reads
  of which subtree_cid itself              :     3240 page reads
  of which warm's own walk_pages           :     1085 page reads
  of which the version check               :     1085 page reads
```

`assert_eq!(warm_reads, walk_reads + cid_reads + nodes)` holds: `5410 == 1085 + 3240 + 1085`.
Asserting the docs' 4325 in its place fails `left: 3240, right: 4325` —
`bench/d109_warm_split_before.txt`.

So the digest alone is **1.48x** the control, not 2.0x. The review's arithmetic was right.
The verdict is unchanged: 3240 is still half again the whole O(N) path, before anything is
walked or validated.

---

## 4 — LOW. `Differ::walk` asked the provider to compare a page with itself

**Commit** `Settle a page against itself without asking the identity provider`

`id_of` is a pure function of the page id by `NodeIdentity`'s contract, so `a == b` can only
compare equal — but the memoising providers were spending two page fetches and two header parses
to find that out. In a copy-on-write store the same-page-id pair is what every shared subtree
looks like, which is most of a small diff. `if a == b ||` in front is exactly equivalent.

`comparing_a_page_with_itself_never_consults_the_identity_provider` forces it **both ways**
through a call-counting `NodeIdentity`:

- two identical roots cost **zero** queries, while still reporting one skip covering all 1085
  nodes and reading nothing;
- a real one-row change still queries, or the short-circuit would be reporting every diff empty.

Its exact invariant is `id_of calls == visited`, which holds under `PageIdentity` because two
ids are equal there iff the pages are one page. Before the short-circuit each skipped pair also
spent two queries, so it ran `2 * skipped_subtrees` higher.

Without the short-circuit it fails at the first assertion, `left: 2, right: 0` —
`bench/d109_self_comparison_before.txt`.

**One behaviour change, documented** on `MemoIdentity::misses` and on `diff`: an identical pair
no longer counts as a miss, because no digest was consulted to decide it.

---

## 5 — The review was wrong. There is no "748 page reads" row on main

**Folded into the commit** `Compare both-root precompute against a control that walks both roots`

The review placed a "748 page reads" row at `diff.rs:786` and said it was measured with `stamp`
applied to `base` only. **No such row exists on main.** `grep -c 748 src/cow/diff.rs` returns 0.
It was added by `6b00698` and lives only on the unlanded `D105-diff-memo`, at that file's line
825. `diff.rs:786` on main is inside an assertion.

The *concern* behind it was real, and it lands on rows that are here. The table set
`MemoIdentity(...).warm` at 5410 against `CowTree::diff` at 2188 and called it "2.5x the
control" — but the control walks **both** roots while 5410 warms one. The file's own rule at
`SubtreeHash` ("a harness that wants an honest diff-time number stamps both roots") was not
being applied to the file's own table.

**Measured**, both roots through one memo, which is what a diff needs:

```
SubtreeHash::stamp, BOTH roots             :     1450 page reads   0.66x the control
MemoIdentity(subtree_cid).warm, BOTH roots :     9024 page reads   4.1x  the control
```

The second root is cheap for either provider only because the memo already holds the pages the
two roots share. The verdict is unchanged and gets **stronger**: warming through `subtree_cid`
costs four times the path it would replace, not two and a half, and `SubtreeHash` stays under
the control — which is the whole reason the docs tell you to prefer it. Both figures are now
asserted, so the table fails rather than ages.

---

## 6 — LOW. "Unconstructable" was false, so the collision is now constructed

**Commit** `Build the crc32 collision this file said was unconstructable`

Two places claimed a page that disagrees on `birth_epoch` while agreeing on `checksum` cannot be
built: `PageVersion`'s doc called it "unconstructable", and
`the_version_key_consults_the_birth_epoch_and_not_only_the_checksum` gave that as the reason no
store-level test exists. crc32 is affine, so it is constructible by algebra. The adjacent claim
— "exactness where crc32 is probabilistic" — was the correct one all along.

Rather than only deleting the overstatement,
`a_page_crafted_onto_a_colliding_checksum_is_still_a_different_version` does the construction:
read off what each of 64 free payload bits does to the crc (the difference is a property of the
flip alone, since `crc(a)^crc(b)^crc(c) == crc(a^b^c)` for equal-length messages), row-reduce to
a basis over GF(2), and solve for the flips that cancel the difference a new birth epoch made.

The result passes the store's own `verify_checksum` and carries the original page's checksum in
a later epoch. `PageVersion` still separates the two lives — which is exactly what the `birth`
half is for, now made to fire against an adversary rather than asserted about.

**Made to fire:** skipping the solved flips fails at *"the crafted page does not verify, so the
construction is wrong and proves nothing"*.

The surviving claim is narrower and true: the **store** never writes such a page, which is why
there is no store-level test — not that no test could build one.

---

## What the review got wrong

1. **Finding 5's row does not exist on main.** Verified by `grep -c`, and traced to `6b00698`
   on the unlanded `D105-diff-memo`. Nothing was invented to "fix" it; the underlying labelling
   concern was applied to the rows that are actually there, where it turned out to matter more.
2. **Finding 2's code-level fact was already pinned elsewhere.** `D105-diff-memo` had already
   measured the three staleness routes. Only the doc half was unfixed. Re-deriving the
   measurement would have been duplicated work.
3. Findings 1, 3, 4 and 6 reproduced exactly as described, including finding 3's arithmetic.

## What the coordinating brief got wrong (checked with git)

- `D92-merge3-fix`'s tip is `3413743`, not `204b40e`; the branch advanced by one commit,
  *"D92: default proof() to Fingerprint, and correct two claims fix-diff-memo falsified"*.
- `NodeIdentity::proof()` **has** a default (`IdentityProof::Fingerprint`) at that tip, and its
  own doc explains that the required-method version was deliberately backed out. The instruction
  to give any new impl a `proof()` "since there is no default" rests on a stale read. (Moot here:
  no new `NodeIdentity` impl was added to `src/`; `MemoIdentity<F>` became `MemoIdentity<'t, F>`,
  the same single impl. The test-only `CountingIdentity` inherits the default.)
- "`+70/-0`, purely additive" is additive against the **merge base** `1758b3b`, not against main.
  `git merge-tree --write-tree main D92-merge3-fix` exits 1 with `CONFLICT (content) in
  src/cow/diff.rs` — D92 already collides with main before this branch exists, because `fddf13c`
  rewrote the memo key underneath it. Its added `SubtreeHash` warning also still recommends
  `(PageId, birth_epoch)`, which `fddf13c` superseded with `(birth_epoch, checksum)`.

## Verification

All commands run from `/Users/idide/wt/ferrodb-D109-diff-identity`, bounded with `timeout`.

```
$ cargo test --lib cow::diff
test result: ok. 25 passed; 0 failed; 0 ignored; 0 measured; 1555 filtered out

$ cargo test --test integration_production_diff_wiring
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

$ cargo test --doc cow::
test src/cow/diff.rs - cow::diff::MemoIdentity (line 351) ... ok
test src/cow/diff.rs - cow::diff::MemoIdentity (line 391) - compile fail ... ok

$ cargo test --lib cow::
test result: ok. 134 passed; 0 failed; 1 ignored; 0 measured; 1445 filtered out

$ cargo build --all-targets
Finished `dev` profile [unoptimized + debuginfo] target(s)

$ cargo test --lib          # the whole library, nothing filtered
test result: ok. 1577 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 61.91s
EXIT=0
```

Baseline before any change on this branch was `22 passed; 0 failed`; the three new tests bring
it to 25.

### Banked raw artifacts

| File | What it is |
|---|---|
| `bench/d109_two_store_guard_before.txt` | Finding 1 reproduced against the old API, plus the test source |
| `bench/d109_ancestor_row_blind_spot_before.txt` | Finding 2: the docs-as-written assertion failing |
| `bench/d109_warm_split_before.txt` | Finding 3: `left: 3240, right: 4325` |
| `bench/d109_self_comparison_before.txt` | Finding 4: `left: 2, right: 0` without the short-circuit |
