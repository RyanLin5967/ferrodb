# F8 — deterministic simulator

**Done**
- `src/consensus/sim.rs` + `src/consensus/tests_sim.rs`. 24 tests, all green. One line added to the
  frozen `mod.rs`: `pub mod sim;`.
- Safety: 100 000 seeds per property in release — 346 982 elections, 3 013 990 committed rounds,
  410 028 crashes, zero violations. Default `cargo test` sweeps 2 000 seeds per property.
- Every detector fired by a deliberate defect and then shown quiet. Figure 8 and the lease are
  scripted, because 400 seeds of chaos measurably never produce them.
- Four real defects found, three in the reference machine and ONE IN THE SIMULATOR ITSELF: a node
  that crashed with a vote still queued behind an unfinished fsync had the vote delivered anyway,
  which is a lying fsync and reported two leaders in a term against a correct protocol. Seed
  1592682576 is pinned as a regression test.

**Doing now**
- Adversarial review in a fresh context, then the summary.

**Next action**
- Run the review workflow over sim.rs + tests_sim.rs; fix what it confirms; write
  `/Users/idide/wt/artie-research/build-F/F8-sim.md`.
