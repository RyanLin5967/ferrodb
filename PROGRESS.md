# F8 — deterministic simulator

**Done**
- `src/consensus/sim.rs` — the simulator (one added line in the frozen `mod.rs`: `pub mod sim;`).
- `src/consensus/tests_sim.rs` — `RefNode` reference Raft + 7 const-generic defects. 17 tests green.
- Every detector forced to fire and then shown quiet. Two mutants needed scripted scenarios rather
  than sweeps, and that is measured, not assumed: 400 seeds of chaos never produced figure 8.
- Contract findings so far: (1) `Consensus` has no log and no way to read one, so `Body::Append`
  cannot be populated; (2) `mod.rs` exempts every `PreVoteResp` from the later-term rule, and a
  *refused* one carries a real term — without acting on it a restarted node can deadlock.

**Doing now**
- The remaining coverage: the pre-vote disruption mutant, liveness (cold start, failover, and the
  anti-vacuity "a cluster cut in half elects nobody"), the restart-durability test, and the test
  that drives the real `Consensus` and starts asserting the moment F1/F2 land.

**Next action**
- Add those tests, clear every `-D dead_code` warning (CI denies it), run the 10k-seed sweep in
  release, then the full suite.
