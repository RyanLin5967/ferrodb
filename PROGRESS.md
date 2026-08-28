# E82 — refused schema edit leaves partial state

**Done**
- Reproduced the finding from `I19-atk4-paths` (`0dee647`): copied
  `tests/integration_alter_refusal_paths.rs` onto HEAD (`d65cbcd`) and ran it —
  `4 passed; 5 failed`, exactly the five the commit message names.
- `src/catalog/alter.rs`: split `alter_table` into `plan_alters` (decides, writes
  nothing) + `apply_plan` (writes, refuses nothing), generalised to a SEQUENCE of
  actions on one table, executed as one heap pass. `alter_table` is now the
  one-action case of it. `rewrite_heap` became `prepare_rewrite`/`commit_rewrite`;
  the three inline row transforms became `conform_row`. Added
  `refuse_if_too_wide` for a row about to be published into an altered shape.
  `integration_alter_column` 29/29, `integration_alter_refusal_safety` 16/16.

**Doing now**
- `src/agent_sql/runtime.rs` `publish_evaluation_as`: plan every table's edit
  chain and measure every row to be published BEFORE anything is written, then
  apply the plans, then publish the rows conformed to the shape the edits produced.

**Next single action**
- Rewrite the tail of `publish_evaluation_as` (the publish transaction + the
  per-edit `alter_table` loop) into plan / precheck / apply / publish.
