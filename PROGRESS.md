# F3 — transport

**Done.** `src/consensus/transport.rs`: codec for every `Body`/`Command`/`BranchOp` variant over
`tag | u32 len | body`; `REPL_VERSION` 1→2 and `CONSENSUS_TAG = b'C'` added to
`src/replication/mod.rs` (the one shared file this row may touch); resumable `FrameReader`;
thread-per-peer `Transport` over `std::net`. 30 tests in `src/consensus/tests_transport.rs`, all
passing. Committed as `94b0e50`.

**Doing now.** The mutant sweep: 19 mutants, one per rule, each applied to the committed tree, its
named test run, then restored. Log at `~/wt/logs/F3-mutants.txt`, progress at
`~/wt/logs/F3-mutants-progress.txt`.

**Next action.** Read the mutant log; any mutant that SURVIVED means that rule has no detector and
needs a better test. Then `cargo build --examples` (the `REPL_VERSION` edit trips nine integration
files' freshness guards) and a per-target suite run.
