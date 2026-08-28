# F10 — a durable Typed Effect Log

## Done
- `src/tel/log.rs` rewritten: `DurableEffectLog` beside `MemEffectLog`, one shared `classify()` for
  the re-append rule, a self-describing codec for `TxnFrame` (Value tags are `index_page`'s), and
  the `Storage` seam so crashes can be aimed. Committed `6c3d45d`.
- `src/tel/tests_durable_log.rs`: 19 tests, all green. Includes a 60-point aimed crash sweep over a
  whole agent task × 3 write shapes × 2 durability models.
- Real defect found by widening that sweep and fixed (`52ef826`): `open` refused any file shorter
  than its 12-byte header, so a crash tearing the very first write bricked the log for ever.
- `scratchpad/guard-depth-measurement.md`: measured where `GuardExpr` recursion aborts the process
  (codec ok at 512, aborts at 768; the pre-existing clone/eval/Display/Drop abort near 1024). Cap
  set to 256 from the measurement, not a guess.
- `scratchpad/mutants-f10.py`: 16 mutants, one per rule.

## Doing right now
- Mutant run in flight (background, `scratchpad/mutants-f10.log`). **Do not write into src/ or
  tests/ while it runs** — it edits `src/tel/log.rs` in place and restores it.

## Single next action
- Read the mutant log; then add `pub use log::{DurableEffectLog, RecoveryReport};` to
  `src/tel/mod.rs` and create `tests/integration_durable_tel.rs` (drafted in the session
  scratchpad), then run `tools/verify-suite.sh` in per-target mode.

## OWNERSHIP CONSTRAINT (do not re-derive)
F9 holds `src/agent_sql/runtime.rs` and F11 holds `src/cli/cli.rs` + `examples/pgserver.rs`, both
live in this wave (`~/wt/artie-research/phase-f-wave-b.tsv`). So the last mile of the ledger's
"…and is the runtime's default" — swapping `Arc::new(MemEffectLog::new())` for
`DurableEffectLog::default_for_database(&db_path)?` at `src/cli/cli.rs:119` and `:125` and
`examples/pgserver.rs:95` and `:102` — is NOT mine to edit. `default_for_database` exists so that
edit carries no decision.
