# E79b — a prompt to hash

**Done (committed)**
- `BEGIN AGENT SESSION AS 'a' [RUN 'r'] [MODEL 'm/v'] [PROMPT 'text']`. `PROMPT` is a *soft* keyword
  matched by lexeme, so `prompt` stays a usable column/table name (the `ADMIT ALL` idiom).
- `AgentRuntime::begin_session_as(RunIdentity, parent)` is the single body; it hashes with
  `prompt_digest` into `RunEntity::prompt_hash`. No clause => `[0u8; 32]`, which is NOT
  `prompt_digest("")`. `begin_session` / `begin_session_with_model` delegate.
- Tests: 4 unit rules (scanner/parser/binder), `tests/integration_prompt_clause.rs` (8), the SQL-path
  feed test, and `ferro_runs_reports_the_digest_of_a_declared_prompt`.
- 8 mutants, all fired, tree clean after — `scratchpad/E79b-mutants.txt`.
- README/DEMO documented; corrected a README claim E79 had falsified.

**Doing now**
- Per-target suite run (`scratchpad/run_suite.py` -> `scratchpad/E79b-suite.txt`), 81 targets.

**Known instrument problem, NOT a regression**
- `integration_base_backup`, `integration_cdc_diff`, `integration_cdc_duckdb_sink` (and likely other
  cdc/repl targets) `Command::new(example_bin(...))` for `repl_primary` / `repl_replica` /
  `cdc_feed` / `table_dump`, plus external `duckdb` / `go` / `sqlite`. `cargo test --test X` does not
  build examples, so they fail for want of binaries. Re-run them after `cargo build --examples`.
- `integration_alter_column` produced NO result line; `integration_alter_refusal_safety` exited
  non-zero with 16 passed / 0 failed. Both still need a raw-output look and a base-commit comparison.

**Next action**
- After the suite: `cargo build --examples`, re-run every failing target, then compare the
  still-failing ones against base commit d65cbcd in a separate worktree before claiming anything.
