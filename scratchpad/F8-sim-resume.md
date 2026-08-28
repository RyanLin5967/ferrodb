# F8 — deterministic simulator: resume state

Branch: `F8-sim`  (worktree /Users/idide/wt/ferrodb-F8-sim)
Owns: `src/consensus/sim.rs`, `src/consensus/tests_sim.rs`.
Summary goes to `/Users/idide/wt/artie-research/build-F/F8-sim.md` — write EXACTLY ONCE, at the end.

## Established facts
- `mod.rs` is frozen but does NOT declare `pub mod sim;`. Without one line there the file is not
  compiled at all and nothing outside mod.rs can declare a child of `consensus`. One additive line
  added; reported loudly in the summary.
- `Consensus` has NO log field and no way to read a log. `Action::Persist{entries}` is outbound
  only; `Event::Persisted{term,round}` returns watermarks. So F2 cannot populate
  `Body::Append{entries}` under the frozen contract. Contract gap — report it.
- `Consensus` fields are `pub(crate)`, so sim.rs can reconstruct a node after a crash.
- `FerroError::NotLeader { leader: Option<String> }` already exists.
- F1/F2 handlers are `unimplemented!()`; sim is generic over a `Peer` trait so it drives both the
  real `Consensus` and a reference machine (`RefNode`, in tests_sim.rs).

## Next action
(kept current as work proceeds)
