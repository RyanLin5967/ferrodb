# F8 — deterministic simulator

**Done**
- `src/consensus/sim.rs`: the simulator — `Peer` trait, seeded fault model (asymmetric partitions,
  drops, reorder, duplication, crash/restart), per-node durable `Store`, 11 named detectors,
  `sweep()` over many seeds, `Violation` that prints its seed. Compiles.
- One line added to the frozen `mod.rs`: `pub mod sim;` (without it the file is not compiled at
  all; nothing outside mod.rs can declare a child of `consensus`). Reported in the summary.

**Doing now**
- `src/consensus/tests_sim.rs`: the reference state machine (`RefNode`) that lets the detectors be
  fired while F1/F2 are still `unimplemented!()`, plus the mutants and the scenario tests.

**Next action**
- Write `RefNode` (pre-vote, election restriction, lease, replication, §5.4.2 commit rule) and get a
  healthy 5-node run to elect and commit with zero violations.
