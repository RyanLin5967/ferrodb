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
