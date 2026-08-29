# F11 — the server must actually reap

## Done — the row is complete
- `src/branch/lease_thread.rs` + `lease_thread/tests.rs` (12 tests): resume on startup, scan on an
  interval, never inside a merge, never on a clock this node cannot read, stoppable cleanly.
- `src/cli/cli.rs` and `examples/pgserver.rs`: both attach `TwoTierReaper` to the runtime AND start
  the lease thread. Both parse `FERRODB_LEASE_SCAN_MILLIS` before taking the `DbLock`.
- `tests/integration_server_reaps.rs` (8 tests): the thesis through both shipped binaries.
- `DEMO.md` items 4 and 6 corrected; `README.md` documents the knob.
- 11 mutants fired (`scratchpad/F11-mutants.md`); suite 1715 pass / 0 fail
  (`scratchpad/F11-suite.txt`, `F11-reruns.txt`, `F11-final.txt`); Go suite ok.
- Summary: `scratchpad/F11-server-reaps.md` and `~/wt/artie-research/build-G/F11-server-reaps.md`.

## Doing right now
- Nothing. The row is finished and committed.

## Single next action
- None for this row. If this worktree is resumed: the only thing deliberately left is a
  fresh-context adversarial review by a subagent, which this session was instructed not to spawn.
