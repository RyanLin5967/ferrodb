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
