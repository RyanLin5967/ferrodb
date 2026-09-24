# D208 pre-registration: root cells keyed by index identity

Written BEFORE any build or run. Nothing on this branch has been compiled or executed (quiet mode).
Every expectation below is INFERRED from source. Amendments are appended below the line at the end;
nothing above it is edited once committed.

## Commits (branch `d208-root-cell-per-index`)

| sha | what |
|---|---|
| `d7891d5` | base: lane `rollback-index-orphan`, which carries the finding's first red test |
| `799d5eb` | **RED phase.** Three behavioural tests. They compile against the old `(table, Option<column>)` API |
| `f7f38eb` | the fix, plus four cell-level tests that use `IndexTree` (they do not compile at `799d5eb`, by construction) |
| `c032bef` | the rebuild cell test gets a DROP below `t` and a moved-root premise. **GREEN phase and mutant base** |
| this commit | this file. `src/` and `tests/` are identical to `c032bef` |

## Tests, `tests/root_cell_is_per_index.rs` (plain `#[test]` fns, no macro)

| # | test | added at |
|---|---|---|
| T1 | `a_fulltext_index_beside_a_secondary_index_on_one_column_keeps_its_own_tree` | `d7891d5` (the finding's own) |
| T2 | `a_btree_index_beside_a_fulltext_index_on_one_column_keeps_its_own_tree` | `799d5eb` |
| T3 | `a_rebuild_leaves_a_btree_index_beside_a_fulltext_index_on_its_own_tree` | `799d5eb` |
| T4 | `an_index_on_a_reused_column_name_does_not_inherit_the_renamed_columns_tree` | `799d5eb` |
| T5 | `each_index_kind_on_one_column_holds_its_own_cell_in_either_order` | `f7f38eb` |
| T6 | `a_rebuild_stores_each_fresh_root_into_its_own_kinds_cell` | `f7f38eb`, strengthened `c032bef` |
| T7 | `a_btree_index_leaving_retires_its_cell_and_keeps_the_fulltext_one` | `f7f38eb` |
| T8 | `a_rename_carries_both_kinds_cells_to_the_new_name` | `f7f38eb` |

## RED at `799d5eb`: `cargo test --test root_cell_is_per_index` gives 4 run, 4 FAILED

- **T1** fails at the post-insert `SEARCH ... 'alpha'`. This is the finding lane's own pre-registration, carried over.
- **T2** fails at its FIRST lookup, before any write: "the B-tree index does not find a row it was built over", `[]` against `[7]`. The full-text index seeded the one cell, so `optimizer::lower` descends the posting tree. The EXPLAIN premise passes.
- **T3**: the premise lookup before the rebuild passes. It fails at "after a rebuild, the B-tree index lost a row", `[]` against `[7]`. The rebuild stores `S'` and then `F'` into the one cell.
- **T4**: both EXPLAIN premises pass. It fails at "the index on the new column `a` does not find a row it was built over", `[]` against `[7]`. The stale `(t, a)` cell names the tree the rename handed to `b`.

Falsifiers. Any one of these withdraws the row; it does not adjust a number.
- An EXPLAIN premise fails in T2, T3 or T4. The fixture then never reached the index, so it measured nothing. The 500-row sizing came from reading `cost_model.rs`: 17 against 26, and 17 against 40 for T4's wider row.
- T2, T3 or T4 PASSES here. Then the mechanism stated for it is wrong.
- A failure at any other assertion than the one named. The mechanism is then not the one claimed, even if the verdict agrees.

## GREEN at `c032bef` (or at this commit)

- `--test root_cell_is_per_index`: **8 passed**.
- `--test d53_shared_root_cell`: **3 passed**. `--test d53_private_root_allowlist`: **1 passed**. Both files are UNTOUCHED: `root_cell(t, None)` keeps its spelling and meaning, and no `open_shared` site was added or removed (the allowlist's floor is 6).
- `--lib -- catalog:: wal::recovery:: planner:: optimizer::`: all pass. D205's recovery tests call `root_cell(name, None)` only at `d7891d5`.
- Collateral: `integration_fulltext_retrieval`, `integration_alter_column`, `integration_secondary_index_debt`, `integration_index_debt`, `d178_dml_index_correctness`, `d179_secondary_strict_lower`, `d187_null_index_scan`, `d56_statsless_point_lookup` and `integration_crash_safety` all pass. The one exception is `integration_fulltext_retrieval::a_rebuild_over_a_re_used_primary_key_does_not_double_the_row`, D197's premise test (decision 3), which already fails at `d7891d5`.
- A T6 failure at a "premise failed: ... on its old page" message is a FIXTURE defect: the allocator did not move the rebuilt roots. It is not a fix defect. It withdraws T6's kill claims (K10 in particular) until the fixture is repaired and re-registered.

## Per-target suite (`VERIFY_MODE=per-target tools/verify-suite.sh d208`)

- Base figure: `d7891d5` = 2611 run, passed=2609 failed=2. This is quoted from `frontier/lane_rollback_index_orphan.md` §14 "Counts"; I did not re-derive its provenance.
- This branch adds exactly 7 runnable tests (T2 to T8), all in the one integration target above. There are no lib or doctest additions.
- **Prediction: 2618 run, passed=2617, failed=1.** The one failure is D197's premise test. T1 moves from failed to passed.
- If 2618 is wrong because the base figure is wrong, the delta still stands: +7 run, failed 2 → 1.

## Mutants (base `c032bef`; each run is `--test root_cell_is_per_index` only)

Every expression was fire-checked on the committed blobs without compute (`PATTERNS_ONLY=1`). Each matches exactly one site. The counter itself reports 0 and 2 on planted patterns. The edits are listed verbatim in `frontier/lane_d208_rootcell_run.sh`.

| mutant | edit | expected FAILED | expected passing |
|---|---|---|---|
| K1 | restores the shared key everywhere: lookup, `sync_root_cells`, `update_fulltext_root` and the rebuild all write FullText as Secondary | T1 T2 T3 T5 T6 T7 T8 | T4 |
| K2 | lookup only: `IndexTree::owned` maps FullText to Secondary | T1 T2 (at the full-text control) T3 T5 T6 T7 T8 | T4 |
| K3 | `open_table` opens a full-text index through the Secondary cell | T1 T2 (full-text control) T3 (record `assert_ne`) | T4 to T8 |
| K4 | `open_table` opens a B-tree index through the FullText cell | T2 T3 (both after the write) | T1 T4 to T8 |
| K5 | the optimizer's secondary scan opens through the FullText cell | T2 (first lookup) T3 (premise lookup) | T1 T4 to T8 |
| K6 | `update_index_root` stores into the FullText cell | T5 | the rest |
| K7 | `update_fulltext_root` stores into the Secondary cell | T5 | the rest |
| K8 | `sync_root_cells` seeds a full-text index under the Secondary key | T2 T5 T6 T7 T8 (the last four at `cell(FullText)`: no cell) | T1 T3 T4 |
| K9 | the rebuild stores the full-text root into the Secondary cell | T3 T6 | the rest |
| K10 | the rebuild stores the B-tree root into the FullText cell | T6 (at the B-tree cell == record check) | the rest. **T3 is UNDETERMINED**: without a free page below, the rebuilt B-tree may land on its freed pages and read correctly by coincidence |
| K11 | cells are retired by whole table again | T7 (at `is_none`) | the rest |
| K12 | the rename leaves the cells behind | T4 (lookup on `a`) T8 | the rest |
| K13 | the rename carries the B-tree cell only | T8 | the rest |
| K14 | the rename re-creates the cell instead of moving the `Arc` | T8 (at `ptr_eq`) | the rest |

K6, K7, K11, K13 and K14 are killed by cell-level tests only. No behavioural fixture here splits a root or drops an index, and there is no `DROP INDEX` statement at all. A behavioural twin for K6/K7 would need a fixture that splits a B-tree or posting root inside a write. None is claimed.

Any mutant surviving all eight tests is a surviving mutant. It is reported as one, and no test is changed to kill it after the fact.

## Owed decisions (⚖)

None. No D53 pinned assertion changed, and neither D53 test file is touched.

---

## Amendment 1 (appended before any run; nothing above is edited)

**A semantic merge conflict with lane #16's current head, and a resolution branch.** The finding's
lane moved on from `d7891d5` to `00f4c39` (D210/D211, UNBUILT). There, D205's
`wal::recovery::tests::a_crash_rebuild_points_every_shared_root_cell_at_its_new_tree` walks every
tree through `root_cell(name, column.as_deref())`, with `column: Option<String>` naming full-text
indexes by column alone. `git merge-tree --write-tree 344d309 00f4c39` is clean (rc=0), and the
merged tree does NOT compile against D208's `Option<IndexTree<&str>>`. That line is the only one:
checked by grepping the merged tree for every `root_cell(` call.

`d208-root-cell-per-index-on-00f4c39` = `88f09ee` is that merge with the call adapted (kind per
index, `column.as_ref().map(|i| i.borrowed())`). The assertion is unchanged. It is a D205 test, not
one of D53's. Built with plumbing, so this worktree did not move.

Expected at `88f09ee` (INFERRED):
- `--test root_cell_is_per_index` 8 passed.
- `--lib -- wal::recovery::` all pass, including the adapted test.
- Everything lane #16 pre-registered for `00f4c39` still holds. This branch touches none of its
  paths except that one test line and the files listed at the top.

If #16 lands at a head other than `00f4c39`, this branch is stale. The resolution is those two
edits in that test, re-applied.

---

## Amendment 2 (appended before any run; nothing above is edited): D214 and D215

The lead turned report §6's two findings into ledger rows D214 and D215 and assigned them to this
branch. Nothing has been built or run. Every expectation here is INFERRED.

| sha | what |
|---|---|
| `f83c082` | **RED phase for D214/D215**: two tests, both compiling against `f210779`'s API |
| `b5793ca` | the two fixes |
| this commit | this amendment. `src/` and `tests/` are identical to `b5793ca` |

New tests:
- **T9**, `root_cell_is_per_index::search_through_a_cached_snapshot_finds_a_row_after_a_posting_root_split` (D215). It clones the catalog as a reader's snapshot, splits the posting root through the live catalog, and runs SEARCH against the snapshot.
- **U1**, `catalog::alter::tests::finish_points_the_primary_cell_at_the_root_it_records` (D214). **A UNIT test.** It drives the private `finish` with a real second tree's root. No ALTER shape can move the primary root: `commit_rewrite` only `upsert`s keys the tree already holds, with a fixed-size `RecordId`, and `try_write_without_split` documents that a same-size replacement cannot split (READ).

The harness's `rows` and `ids` became free functions (`rows_on`, `ids_of`), so that one statement can run against a snapshot. The old tests' behaviour is unchanged.

### RED at `f83c082`
- `--test root_cell_is_per_index`: **9 run, 1 FAILED**. T1 to T8 pass (D208 is fixed at this sha). T9 fails at its last assertion, "a cached snapshot's SEARCH descended its recorded root ...", with `[]` against `[0]`. Its four premises pass: the root moves within 5000 postings; the epoch is unchanged; the snapshot's record is stale; the live search finds `[0]`.
- `--lib -- catalog::alter::tests`: **1 run, 1 FAILED** at "finish recorded the new primary root and left the shared cell on the pre-ALTER tree". Its premises pass.
- Falsifiers:
  - T9's premise "5000 postings never split" fails. The fixture is then wrong.
  - T9 PASSES here. Then the stated mechanism is wrong: the scan does not stop at the smaller token on the next leaf, or the snapshot does not share the live catalog's cell.
  - U1 PASSES here. Then something else already stores into the cell.

### GREEN at `b5793ca` (or at this commit)
- `--test root_cell_is_per_index` gives **9/9**, and `--lib -- catalog::alter::tests` gives **1/1**.
- Everything in the GREEN section above still holds.
- `d53_private_root_allowlist` 1/1, file untouched. D215 adds one `::open_shared(` site: 8 → 9 over comment-stripped `src/`, counted by a Python port (below); floor 6. That corrects this file's earlier "no `open_shared` site was added or removed", which was true of D208 alone.

### Per-target suite
- +2 run (T9 and U1). **Prediction: 2620 run, passed=2619, failed=1** (D197's premise test).

### Mutants (base `b5793ca`)

| mutant | edit | target | expected FAILED |
|---|---|---|---|
| K15 | `finish`'s `cell.store(primary_root_now, ..)` removed | `--lib -- catalog::alter::` | U1 |
| K16 | SEARCH looks up the Secondary cell, so it finds none and falls back to the record | `--test root_cell_is_per_index` | T9 |
| K17 | SEARCH's `Some(cell)` arm opens from the record anyway | `--test root_cell_is_per_index` | T9 |

K1 to K14 are unchanged, and T9 is expected to pass under each of them. None of them touches the SEARCH lookup or `finish`.

### ⚖ for Ryan (proposed, NOT committed)
The `d53_private_root_allowlist` change that lets it see a private open through a helper is a test edit. The proposed diff is in `frontier/lane_d208_rootcell.md` §8, not in this tree.

A Python PORT of its rule was RUN over committed trees; this is evidence about the algorithm, not about the Rust, which is uncompiled:
- at `f210779`: helpers `['open_posting_tree']`, and one offender, `src/execution/fulltext_search.rs: let tree = open_posting_tree(ft_root, bp.clone());`;
- at `b5793ca`: no offenders.

If adopted, the allowlist target gains 1 test (its planted fire-check). That test is not in the prediction above.

---

## Amendment 3 (appended before any run): correction to amendment 2's mutant sentence

Amendment 2 says "T9 is expected to pass under each of [K1 to K14]". **That is wrong for K2 and
K8.** I found this by re-deriving each mutant against T9's fixture, which has a full-text index and
no B-tree index on `body`:

- **K2** (the lookup maps FullText to Secondary). `sync_root_cells` still seeds `(t, FullText(body))` under its true key, but every `root_cell(.., FullText(..))` lookup now asks for `Secondary(body)`, which does not exist. SEARCH therefore falls back to the snapshot's stale record: **T9 FAILS**.
- **K8** (`sync_root_cells` seeds the full-text index under Secondary). No `FullText(body)` cell ever exists, so SEARCH falls back to the record: **T9 FAILS**.
- **K1** (the whole shared key restored) is NOT a kill for T9. The one full-text index then seeds `Secondary(body)`, and both the insert handle and SEARCH resolve to that same shared cell. T9 passes, as amendment 2 says.
- K3 to K7 and K9 to K14 leave T9 passing, as amendment 2 says. K3 and K7 were re-checked specifically: under each, the full-text cell still ends at the current root (K3 through `update_fulltext_root`'s store into the true key; K7 through the handle's own split into the shared cell).

Corrected expected kills: K2 fails T1 T2 T3 T5 T6 T7 T8 **T9**. K8 fails T2 T5 T6 T7 T8 **T9**. K16 and K17 fail T9. K15 fails U1.

---

## Amendment 4 (appended before any run; nothing above is edited): the fresh-context review

The review is `frontier/d208_review.md` @ artie-research `4608802`. Its verdict is SOUND-WITH-CAVEATS, with two blockers:
- **F1:** D214's store regresses a cell that is ahead of a lagging record.
- **F2:** T9 cannot go red.

Its other items:
- **F3** is pre-existing on main and is now ledger row D222. It is NOT this branch's: lane `d222-index-root-after-backfill` fixes it, and that branch has no commits yet (it still sits at `9aa6968`), so there is nothing to merge.
- **F4** is hardening.
- **F5** is re-cutting the #16 resolution branch.

Nothing has been built or run. Every expectation here is INFERRED.

| sha | what |
|---|---|
| `5f19277` | **RED**: U2 (F1, a UNIT test), T9 re-cut (F2), and the three F4 tests; all compile against `f612ba8` |
| `0214c64` | the fixes: `finish` moves the primary cell by `compare_exchange(planned, now)` (F1); `install_fresh_cell` in each CREATE (F4); D215's comment names the 64-hop right walk (F2) |
| this commit | this amendment. `src/` and `tests/` are identical to `0214c64` |

### Withdrawals (logged here, where the claims were logged)
- **Amendment 2's T9 RED at `f83c082` is WITHDRAWN.** T9 as written there stopped at the first split, one hop from the recorded page. D58's `read_leaf_for` walks right up to 64 hops (`index.rs` `RIGHT_WALK`) and repairs that, so T9 would have PASSED at `f83c082` (the review, INFERRED). Amendment 3's T9 kills of K2 and K8, and amendment 2's T9 kills of K16 and K17, fall with it. All four are re-registered below for the re-cut T9.
- **Amendment 2's D214 contract is replaced.** It said the cell and the record "must agree afterwards". The contract is now: the cell follows a root the rewrite moved, but only if the cell still names the root the rewrite started from. A cell that is ahead of a lagging record is left alone.

### New and changed tests
- **U2**, `catalog::alter::tests::finish_leaves_a_primary_cell_that_is_already_ahead_of_the_record`. A UNIT test: it sets the cell ahead of the record directly.
- **T9, re-cut.**
  - `body VARCHAR(2000)`; one `zulu` row; then rows of 200 ascending `aNNNNNNN` tokens each.
  - It grows the tree until `hops_from(recorded -> zulu's leaf) > 64`, with `hops_from` using `descend_optimistic`'s own stopping rule, and bounds that at 400 rows.
  - It asserts that as a premise, beside the earlier four.
  - Its red phase is a MUTANT run (K16, K17), because the fix predates the fixture.
- **F4a/F4b/F4c**: `a_create_index_` / `a_create_fulltext_index_` / `a_create_table_never_inherits_a_cell_left_under_its_key`. Each drops a record from `tables` without a sync, creates the same key again, and asserts the cell is not the dead `Arc` and names the new record.
- The count of runnable tests the branch adds over `d7891d5`: T2–T9 (8) + F4a–c (3) + U1 + U2 = **13** (`#[test]` counted per file, no macros; `root_cell_is_per_index` 12 against 1 at `d7891d5`; `alter.rs` 2 against 0).

### RED at `5f19277`
- `--test root_cell_is_per_index`: **12 run, 3 FAILED.** F4a, F4b and F4c each fail at "inherited the dead tree's cell". T1–T9 pass: D208 and D215 are fixed here, and T9's premises hold.
- `--lib -- catalog::alter::tests`: **2 run, 1 FAILED.** U2 fails at "finish regressed the shared primary cell to a stale record". U1 passes.
- Falsifiers: any F4 premise "landed on the dead tree's root page"; U2 passing here; T9's 400-row premise failing.

### T9's red, as mutants K16 and K17 at `0214c64`
T9 fails at its last assertion, `[]` against `[0]`, with every premise passing. The mechanism:
1. The drift is over 64 hops, so the optimistic descent returns `Ok(None)`.
2. After 16 restarts it falls back to the latched descent from the stale leaf, which does not walk right.
3. The scan starts past the recorded leaf's end and reads the next leaf.
4. That leaf's first token, `aNNNNNNN`, is not `zulu`, so the scan breaks.

If T9 PASSES under K16 or K17, that mechanism is wrong.

### GREEN at `0214c64` (or at this commit)
- `root_cell_is_per_index` **12/12**, and `--lib -- catalog::alter::tests` **2/2**.
- The rest of the GREEN section and amendment 2 still hold.

### Per-target suite
- **2624 run, passed=2623, failed=1** (D197's premise test). That is `d7891d5`'s 2611 (quoted from #16 §14) plus 13.

### Mutants: the table in force from here (base `0214c64`)

`install_fresh_cell` changes several kill sets. That is a property of the new code, not a correction of a derivation; each was re-derived against it (INFERRED). The effects:
- The index created LAST now owns a collided key, so under K1 and K2 T1 passes.
- Under K8 the FullText cell `install_fresh_cell` seeds is retired again by the same CREATE's sync.
- The fresh install independently stops the renamed tree being inherited, so T4 no longer kills K12. T8 still does.

| mutant | target | expected FAILED |
|---|---|---|
| K1 | integration | T2 T3 T5 T6 T7 T8 |
| K2 | integration | T2 T3 T5 T6 T7 T8 F4b |
| K3 | integration | T1 T2 T3 |
| K4 | integration | T2 T3 |
| K5 | integration | T2 T3 |
| K6 | integration | T5 |
| K7 | integration | T5 |
| K8 | integration | T5 T6 T7 T8 T9 F4b |
| K9 | integration | T3 T6 |
| K10 | integration | T6 (T3 undetermined) |
| K11 | integration | T7 |
| K12 | integration | T8 |
| K13 | integration | T8 |
| K14 | integration | T8 |
| K15 (re-cut: `finish`'s compare-exchange removed) | lib `catalog::alter::` | U1 |
| K16 | integration | T9 |
| K17 | integration | T9 |
| K18 (compare-exchange replaced by an unconditional store: the first D214) | lib `catalog::alter::` | U2 |
| K19 (`install_fresh_cell` inserts only if absent: inheritance restored) | integration | F4a F4b F4c |
| K20 (`create_table`'s install removed) | integration | F4c |
| K21 (`create_index`'s install removed) | integration | F4a |
| K22 (`create_fulltext_index`'s install removed) | integration | F4b |

Every mutant has at least one killing test. Any mutant that survives is reported as a survivor, and no test is changed afterwards to kill it.

### F5: the resolution branch on #16's head `115f0b7`
- `git merge-tree --write-tree 0214c64 115f0b7` gives rc=1, with a content conflict in `src/wal/recovery.rs` on the `use` line: `IndexTree` against `RetiredSlot` (RUN).
- The resolution takes both imports, plus amendment 1's two-line adaptation of D205's cells test (`root_cell(name, column.as_deref())`).
- A grep of the merged tree finds no other old-style `root_cell(` call (RUN).
- `run`, `sync_roots` and `sync_fulltext_roots` keep their signatures at `115f0b7` (RUN, grep).
- It is built with plumbing onto a new branch, `d208-root-cell-per-index-on-115f0b7`. Its sha is in the lane report, because a body cannot name its own commit.
- `d208-root-cell-per-index-on-00f4c39` (`1877e03`) is SUPERSEDED: it sits on `00f4c39`, which #16 has moved past.

---

## Amendment 5 (appended before any run; nothing above is edited): D214 in its principled form, and D222 merged

The lead's decision: take the review's PRINCIPLED F1 and REPLACE the compare-exchange. The compare-exchange reconciled two copies of the primary root; this removes the second copy. Nothing has been built or run. Every expectation is INFERRED.

| sha | what |
|---|---|
| `3bcdf5c` | **RED**: T10, through the real ALTER path. It compiles against `ad8adf3` |
| `459ff80` | the principled D214; U1 and U2 re-cut to `finish`'s new signature |
| `a7f5d1b` | merge of `d222-index-root-after-backfill` @ `bd023f6` (D222's own red `f7f9022` and fix `bd023f6`); merge-tree clean |
| this commit | this amendment. `src/` and `tests/` are identical to `a7f5d1b` |

### The design (READ at `459ff80`)
- `AlterPlan` carries the primary index's SHARED cell, `primary_cell: Arc<AtomicU32>`, taken from `root_cell(table, None)`. The only exception is a private cell seeded from the record for a table the catalog never gave a cell, the same reasoning as `open_table`'s fallback.
- `commit_rewrite` opens the tree with `open_shared(primary_cell)`, so a root move lands in the cell as it happens. It no longer returns a root.
- `finish` writes `entry.primary_index_root = primary_cell.load(Acquire)` and **never writes the cell**.
- U1 ("record == cell == moved, same `Arc`") and U2 ("a cell ahead is never regressed", now also "record == cell") therefore hold BY CONSTRUCTION. The record is copied from the cell, and nothing in the ALTER path stores into the cell.

### Changes to registered tests (logged here)
- **Amendment 4's F1 contract (the compare-exchange) is WITHDRAWN**, together with K15 and K18 as defined there. Their patterns no longer match anything.
- **U1 and U2 are re-cut.** `finish` no longer takes a root, so each now:
  1. moves the cell (a split through the shared handle, or an earlier INSERT's);
  2. calls `finish(.., &cell, ..)`;
  3. asserts record == cell and that the cell is untouched.

  No assertion was weakened, and U2 gained "record == cell". Their earlier reds (at `f83c082` and `5f19277`) were taken against the old signature and are not repeated. Their red is now the mutant runs K15 and K18c.
- **New: T10**, `root_cell_is_per_index::an_alter_after_a_lagging_record_keeps_the_cell_and_catches_the_record_up`.
  1. It grows `t` until the primary root really splits, bounded at 5000 rows.
  2. It sets the in-memory record back to the pre-split page. This is the ONE simulated step: the lag a failed INSERT leaves before `sync_roots`.
  3. It runs `ALTER TABLE t RENAME COLUMN v TO w`.
  4. It asserts the cell is the same `Arc` on the post-split root, and the record == the post-split root.
  5. It INSERTs a high key and looks up a right-half key, with an EXPLAIN premise for the primary index scan.

### RED at `3bcdf5c` (its code is `ad8adf3`'s)
- `--test root_cell_is_per_index`: **13 run, 1 FAILED**. T10 fails at "the ALTER left the record behind the cell it planned from": the compare-exchange leaves the cell alone and writes the stale planned root into the record. Its premises (a real split; the record caught up before the rollback) and its two cell assertions pass.
- Falsifiers:
  - T10 passes here, which would mean the compare-exchange version already recorded from the cell;
  - "5000 rows never split the primary root";
  - the EXPLAIN premise failing.

### GREEN at `a7f5d1b` (or at this commit)
- `root_cell_is_per_index` **13/13**; `catalog::alter::tests` **2/2**; `d222_index_root_after_backfill` **4/4**. That last is its own lane's expectation, and it is green here because this branch now carries its fix.
- The D53 files are untouched. `::open_shared(` sites over comment-stripped `src/`: 9 → **10** (`commit_rewrite`), floor 6. Counted by the Python port (RUN). The ⚖ port also finds 0 offenders at `a7f5d1b` (RUN).
- Everything in the earlier GREEN sections still holds.

### Per-target suite
- **2629 run, passed=2628, failed=1** (D197's premise test).
- That is `d7891d5`'s 2611 (quoted from #16 §14), plus this branch's 14 (T2–T10, F4a–c, U1, U2), plus D222's 4.

### Mutants: the F1 rows of the table in force (base `a7f5d1b`); every other row of amendment 4 stands

| mutant | edit | target | expected |
|---|---|---|---|
| K15 | `finish` does not write the record (`let _ = primary_cell;`) | lib `catalog::alter::` and integration | U1, U2 and T10 FAIL |
| K18a | the plan ignores the cell and seeds a private one from the record | integration | T10 FAILS (the record is caught up to a stale private cell, not the live one) |
| K18b | `commit_rewrite` opens the tree PRIVATELY again at the cell's value | both | **SURVIVES: an EQUIVALENT mutant.** No rewrite can split (a same-size replace of an existing key), so a private handle's root never diverges from the cell. No test can tell the two apart, and none is claimed to. `open_shared` is there so the structure has one copy, not because a test can see it |
| K18c | `finish` stores the stale record INTO the cell (the first D214's regression) | lib and integration | U1, U2 and T10 FAIL |

T10 is expected to pass under every other mutant: none of K1–K14 or K16–K22 touches the primary cell or ALTER.
