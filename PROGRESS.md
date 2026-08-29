# F10 — a durable Typed Effect Log

## Status: DONE except one clause, which is a lane blocker (see below)

## Done
- `src/tel/log.rs` (212 → 1661): `DurableEffectLog` beside `MemEffectLog`, one shared `classify()`
  for the re-append rule, growth appended as a **delta** so a retry writes nothing and every op
  reaches the file exactly once, a self-describing `TxnFrame` codec (Value tags are
  `index_page`'s, strings go through `wal::log::write_str`), the `Storage` seam so crashes are aimed.
- `src/tel/tests_durable_log.rs` (1560): 24 tests. `tests/integration_durable_tel.rs` (337): 3.
- `src/tel/mod.rs`: one re-export line. `.gitignore`: `*.tel` and `*.provenance`.
- **Suite: 86 targets, 1722 passed, 0 failed. Go: 97 ok.** 1695 baseline + 27 new = 1722.
  Evidence in `scratchpad/suite-f10-*.tsv` and `/Users/idide/wt/artie-research/build-G/`.
- **Mutants: 23/23 killed** (`scratchpad/mutants-f10.py`, logs beside it).
- CI gate green: `RUSTFLAGS="-D duplicate_macro_attributes -D dead_code" cargo build --lib --tests
  --examples` exits 0.
- Three real defects in my own code found by attacking a pristine export in a fresh context, plus
  three more I verified from the panel's *refuted* list; all fixed. See the summary.

## The one clause not done, and it is not mine to do
"…**and is the runtime's default**". `DurableEffectLog::default_for_database(db_path)` exists so the
edit carries no decision, but the two files that own a database's name are held by live siblings in
this wave: `src/cli/cli.rs:119,125` and `examples/pgserver.rs:95,102` are **F11's**;
`src/agent_sql/runtime.rs` is **F9's** (its brief says so in as many words). Four arguments:

```rust
// src/cli/cli.rs:119 and :125   — fn run_cli(db_path: &str), uses `?`
-                Arc::new(MemEffectLog::new()),
+                DurableEffectLog::default_for_database(db_path)?,
// examples/pgserver.rs:95 and :102 — path variable is `db`, file uses `.expect`
-            Arc::new(MemEffectLog::new()),
+            DurableEffectLog::default_for_database(&db).expect("durable effect log"),
```
Apply `DEMO.md:231` with it — "The effect log is `MemEffectLog`, which is a memory implementation
rather than a durable one" becomes false at that moment.

## Single next action
Nothing. Summary written to `/Users/idide/wt/artie-research/build-G/F10-durable-tel.md`.
