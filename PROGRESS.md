# F8 — deterministic simulator

**Done**
- `src/consensus/sim.rs`: the simulator. `Peer` trait, seeded fault model (asymmetric partitions,
  drops, reorder, duplication, crash/restart losing everything unfsynced), per-node durable `Store`,
  11 named detectors, `sweep()`, `Violation` that prints its replay seed.
- One line added to the frozen `mod.rs`: `pub mod sim;`.
- `src/consensus/tests_sim.rs`: `RefNode`, a reference Raft over the same contract with a
  const-generic defect mask, plus the harness/safety tests. 8 tests green; 400-seed chaos sweeps
  commit 11k+ rounds over 1643 crashes with zero violations.
- The simulator found THREE real defects in the reference machine while being built: a follower
  acking its own log length instead of the confirmed position; truncating the whole conflicting
  term instead of from `prev_round`; and advancing commit to `min(leaderCommit, own log length)`
  instead of `min(leaderCommit, last new entry)`. All three are data-loss bugs.

**Doing now**
- The mutants: one deliberate defect per rule, each required to fire its named detector and then to
  leave it quiet when switched off.

**Next action**
- Write the eight mutant tests, then the scenario tests (partitioned leader demotes itself, one-way
  partition, pre-vote does not depose a healthy leader, liveness after healing).
