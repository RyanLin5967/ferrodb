# F5 — membership changes

**Done:** read DISTRIBUTED.md §F5, mod.rs (frozen), config.rs, election.rs, F1's tests and mutant
driver. Design written to `scratchpad/F5-design.md` and committed.

**Now:** adversarial review of the design (the precondition's counting set is the whole row), then
implement `src/consensus/membership.rs`.

**Next action:** write `src/consensus/membership.rs` — `Change`, `plan_change`, `begin_membership`,
`note_config_in_log`, `note_config_ack`, `apply_committed_config`.
