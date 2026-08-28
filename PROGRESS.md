# F10 — a durable Typed Effect Log

**Done**
- Read phase-f-tasks.md, DISTRIBUTED.md, src/consensus/mod.rs, src/tel/*, src/provenance/durable.rs,
  src/wal/log.rs primitives, src/storage/{storage,sim}.rs, src/consensus/log.rs (framing pattern).
- Baseline `cargo build --lib` green at af77d8a.
- Established ownership constraint: F9 holds src/agent_sql/runtime.rs, F11 holds src/cli/cli.rs and
  examples/pgserver.rs, both live in this wave. So the entry-point wiring is NOT mine to edit.

**Doing now**
- Recon workflow (5 read-only agents) over: merge path, stage/re-append path, TxnFrame encoding
  surface, ledger conventions, crash-fault harness.

**Next action**
- Write `DurableEffectLog` into src/tel/log.rs over the `Storage` seam.
