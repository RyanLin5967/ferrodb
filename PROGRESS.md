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

- Two defects found by re-reading my own code, both fixed and pinned:
  (a) `accepted_through` was not reset on an authority change, so the old membership's high-water
      kept clamping the new leader's grants -> a node could refuse space it legitimately held.
      Reset floor is `issued`, never lower, because that is the record of what was handed out.
  (b) `reserve()`'s comment stated the stranding direction backwards. Fixed, and the invariant that
      makes the order safe (one grant carries `page_count` ids vs `page_count/extent_pages`
      extents) is now pinned by a test instead of assumed.
- FOURTH node-local decision found by sweeping `fetch_add` across src/, not named in the brief:
  `txn.rs` `commits_since_checkpoint` fires `wal.truncate` on a node-local counter, which is exactly
  the hole `Command::Checkpoint`'s own doc names. Guarded (automatic trigger withheld on a member,
  deferred not dropped) with `apply_checkpoint()` as the replicated entry point.
- 25 rule tests + 31 integration tests green.
- Mutation run 1 stopped early (fleet CPU starvation, ~2.5 min/mutant); 5/5 verdicts KILLED, banked
  in scratchpad/f4-mutants-partial-run1.txt. The kill DID leave a mutated file, caught and restored.

## Doing now
Re-running the mutation sweep against final code, grouped by mutation to halve the rebuilds.

## Next action
Run the grouped sweep, then tools/verify-suite.sh, then read the adversary, then the summary.
