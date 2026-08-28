# E82 — refused schema edit leaves partial state

**Done**
- Reproduced the I19-atk4 finding (`0dee647`) on HEAD `d65cbcd`: 4 passed, 5 failed.
- `src/catalog/alter.rs`: `alter_table` split into `plan_alters` (decides a SEQUENCE,
  writes nothing) + `apply_plan` (writes, refuses nothing).
- `src/agent_sql/runtime.rs`: the merge is plan → measure → apply → publish, schema first.
- Three-lens adversarial review in a fresh context; every finding closed or stated.
- 9 detectors in `tests/integration_merge_ddl_atomicity.rs`; 10 mutants, all fired
  (`scratchpad/E82/mutants-final.txt`).

**Doing now**
- Final full-suite run on the finished tree.

**Next single action**
- Write the summary to `/Users/idide/wt/artie-research/build-F/E82-ddl-atomicity.md`.
