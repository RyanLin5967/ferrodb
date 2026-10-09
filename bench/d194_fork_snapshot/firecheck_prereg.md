# D194 fire-checks — PRE-REGISTERED before the first mutation is built

Committed before any mutant runs. Each mutation removes ONE mechanism; the targets run are
agent_sql_surface, integration_read_premise, integration_cherry_pick, integration_sibling_merge.
Script: the scratchpad `mutate.py` (anchors + replacements are copied into each result file's
name below; the mutation is reverted from HEAD after each run and `src/` checked clean).

"MUST FAIL" = the test that exists to catch this mechanism. Others may also fail; that is recorded,
not predicted. A mutant whose MUST-FAIL test passes means that test does not discriminate.

| id | mechanism removed | MUST FAIL |
|---|---|---|
| M01 | `BEGIN AGENT SESSION` pins at fork (dispatch uses the unpinned door; pin happens at first read) | `a_branch_is_pinned_at_its_fork_not_at_its_first_read` |
| M02 | a nested child inherits its parent's `fork_seq` (child takes a fresh `apply_seq`) | `a_nested_task_reads_its_parents_fork_and_composes_with_what_main_published_since` |
| M03 | `version_seen` names the version the pinned read saw (always returns main's latest) | `a_premise_read_through_the_fork_snapshot_after_main_moved_is_held` AND `..._names_the_older_published_version` |
| M04 | `version_seen` consults the history (returns "none" whenever latest > fork_seq) | `a_premise_read_through_the_fork_snapshot_names_the_older_published_version` (and NOT the `..._after_main_moved_is_held` one, whose right answer IS none) |
| M05 | `observed_at = fork_seq + 1` for a pinned read (back to `apply_seq + 1`) | `a_scan_through_the_pin_is_not_a_dependent_of_a_merge_it_could_not_see` |
| M06 | `cherry_pick` reads the target's images through its pin (reads main now) | `a_pick_reads_the_target_branch_as_of_its_fork` |
| M07 | `merge_into` looks up the target's own image when pins differ (always uses the source's base) | `a_row_only_the_source_touched_is_compared_against_the_targets_own_fork_image` |
| M08 | `sibling_op`'s fallback reads the image the target SEES (reads its staged rows only) | `a_row_only_the_source_touched_is_compared_against_the_targets_own_fork_image` |
| M09 | merge-time point lookups read main NOW (they read the branch's pin instead) — the control that merge-time must NOT be pinned | `two_branches_decrementing_the_same_row_compose_arithmetically` (pre-existing test) |
| M10 | branch reads go through the pin at all (`visible_rows_where` reads main now) | `a_branch_reads_main_as_of_its_fork_not_as_of_now` (the drafted test) |
