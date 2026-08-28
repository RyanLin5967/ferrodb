# F4 — cluster-ifying node-local state

## Done
- `src/cluster/mod.rs` (new): `Authority`, `GrantError`, `GrantedCounter`, the process lease clock,
  `ClusterScope`. One consume path (`take`), two authorities; standalone self-grants, a member
  refuses. 23 rule tests in `src/cluster/tests.rs`, all green.
- `src/branch/arena.rs`: `next_extent_start` + `next_arena_id` -> `GrantedCounter`s.
  Checkpoint image format byte-identical (the issued watermark occupies the old u32 slots).
  `apply_arena_grant` added. Recycle stack epoch-stamped.
- `src/wal/txn.rs`: `next_txn_id: AtomicU64` -> `txn_ids: GrantedCounter`.
  `apply_txn_id_grant`, `raise_next_txn_id`, `next_txn_id()` accessor.
- `src/branch/types.rs`: `LeaseDeadline` reads cluster time; `try_now_millis`/`try_from_now`
  added; the callerless `is_expired()` replaced by fallible `is_expired_now()`.
- 3 one-line call-site fixes outside my files: `wal/recovery.rs`, `execution/executor.rs` (test),
  `replication/snapshot.rs` (test).

- `tests/integration_cluster_grants.rs`: 26 tests, all green. Single-node preservation, the three
  refusals, wrong-node/duplicate/standalone grant guards, two-node disjointness for pages and txn
  ids, the replicated clock, authority-change revocation, restart, and the byte-identical image.
- One real bug the tests caught: `GrantedCounter::remaining()` counted stale-epoch ranges the
  guard would refuse. Fixed to filter by epoch.

## Doing now
Mutation sweep: `scratchpad/f4_mutants.py` breaks each rule, runs its named test, restores.

## Next action
Run `python3 scratchpad/f4_mutants.py`, then the full suite, then the summary.
