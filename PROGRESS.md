# F8 — deterministic simulator

**Done**
- `src/consensus/sim.rs` + `src/consensus/tests_sim.rs`. 24 tests. Full library suite 844 passed,
  0 failed. One line added to the frozen `mod.rs`: `pub mod sim;`.
- Safety at 100 000 seeds per property after every review fix: 354 906 elections, 3 039 981
  committed rounds, 410 974 crashes, zero violations. A 1M-seed run is in flight.
- Eight deliberate defects, each fired and then shown quiet. Figure 8 and the lease are scripted,
  because 400 seeds of chaos measurably never produce them.
- Adversarial review in five fresh contexts. Three reported before the fleet was paused; the other
  two (false positives, blind spots) are running now.

**Findings acted on**
- THREE lying-fsync defects in the simulator's own model, all found rather than reasoned about:
  a crashed node's queued message still delivered; a Flush carrying a bool so the first of two
  completions installed the second's value; a Flush crediting an fsync by LENGTH so a
  truncate-and-re-append made never-fsynced bytes durable.
- Partition counters inflated by cuts that blocked nothing (1230/892 -> 1093/702 on 60 seeds).
- No counter for a crash that forgot a vote — the stated breaking shape of the headline detector.
- RefNode answered a granted pre-vote at its own term, so grants from term-behind voters were
  discarded unread; and its AppendResp digest hashed a longer prefix than `matched` named.
- `Report.digest` hashed only message variant tags.

**Next action**
- Read the last two reviewers, act on what they confirm, then write
  `/Users/idide/wt/artie-research/build-F/F8-sim.md`.
