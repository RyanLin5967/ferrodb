# F6 — snapshot transfer. Resume state.

Branch `F6-snapshot`, worktree `/Users/idide/wt/ferrodb-F6-snapshot`.
Summary goes to `/Users/idide/wt/artie-research/build-G/F6-snapshot.md` — ONCE, at the end.

## Findings already banked (do not re-derive)

1. **`src/replication/snapshot.rs` and `backup.rs` are NOT two copies of one path.** `snapshot.rs`
   is E12, a *logical* CDC backfill (rows -> JSONL, LSN handoff); `backup.rs` is E8, the *physical*
   page-image base backup. Zero duplicated functions; neither references the other. The one F6
   streams is `backup.rs`. Nothing to consolidate; the brief's premise is false and that is the
   answer to "check which is the better copy".
2. `backup.rs:38-40` states the backup "is transferred out of band ... Streaming it over the
   replication socket is a separate concern and is not implemented." **F6 is that transfer.**
3. **The base digest is the sharp edge.** `LogTail.base_digest` feeds every later rolling digest.
   `restore()` hardcodes 0 with the comment "no snapshot is installed (F6)". If an installed
   snapshot leaves it 0, every digest above the floor disagrees with the leader's and the
   divergence detector latches a *healthy* follower out of the quorum, permanently. So the payload
   carries the sender's `digest_at(last_round)`, and it must survive a restart.
4. **`unjoined` must NOT be cleared by the install itself.** `meta.last_round` is the sender's
   compaction floor, `<= leader.commit`, normally strictly below it — so "installed" proves only
   "I hold the snapshot's prefix", never "I hold what a quorum holds". The install sets
   `durable = meta.last_round` (the round watermark, which is what DISTRIBUTED.md §F6 actually
   says), and the *next* `Append` clears the flag through F1's single rule
   `observe_quorum_watermark(leader_commit)`. Pinned by tests_membership.rs:901 and
   tests_election.rs:605/626/650.
5. The config goes in through `membership.rs::note_config_in_log`, whose doc already names
   "a snapshot was installed, carrying `SnapshotMeta::config`" as a caller. It refuses an empty
   voter set and a `(version, term)` collision by latching the node out of office — propagate the
   `Err`, do not swallow it.
6. A peer mid-transfer answers no `Append`, so `Progress::silent` grows and eats the leader's
   lease (`election.rs:112`). `InstallSnapshotResp` must zero `silent`.
7. `on_append_resp:1114` clears `needs_snapshot` on every success. `needs_snapshot` stays the
   single authority; the in-flight cursor is subordinate and ignored/dropped when it is false.
8. `init_leader_progress` resets Progress field-by-field, so any new field needs an explicit line.
9. `ensure_log` asserts `tail.base == snapshot_round && tail.last_round() == last_round`. An
   install must move all three in one step, via a rebase seam in replicate.rs (`LogTail::rebased`
   is private there).
10. `SnapshotMeta`'s field set is a WIRE format (transport.rs:410/776) owned by F3. Adding a field
    is a compile error there. The snapshot *content* therefore rides the streamed payload, not the
    meta. `SnapshotMeta` keeps `{last_round, last_term, config, total_bytes}`.
11. Branch-catalog install: `LogBranchCatalog::put` does NOT advance epoch/next_id/free_ids and
    never truncates, so replaying records one by one into a live catalog is wrong. Install by
    writing the file (`u32 len | BranchRecord::serialize()` repeated) and reopening.
12. Arena install: `state_bytes()` / `load_state()`, precondition `base_page` must match; recipe is
    `reopen_from_checkpoint`. Catalog image rides the page image (catalog lives in data pages,
    root `first_catalog_page_id`).
13. Tests that MUST be rewritten because they assert the stub: tests_replicate.rs:1142
    `an_install_snapshot_is_answered_rather_than_dropped_or_fatal`, and the "zero sends" half of
    tests_replicate.rs:783.

## Done
- `src/consensus/snapshot.rs` — the payload format, both halves of the protocol, `SnapshotStore`,
  and `PageStoreSnapshots` over `replication::backup`.
- `src/consensus/tests_snapshot.rs` — 36 rule tests.
- `src/consensus/node.rs` — the driver: retention, the spool, the install, the durable floor-digest
  record. `replicate.rs` — two Progress fields, the tail seams, the dispatch, `restore`'s digest.
- `tests/integration_cluster_snapshot.rs` — exit criterion 3, with its anti-vacuity half.
- `BufferPoolManager::invalidate_all` and `LogBranchCatalog::reload_from`: an install replaces the
  live objects, not only the files.
- Mutants: 41 fired, 41 killed, tree clean (`scratchpad/F6-mutants.txt`). Seven survived the first
  pass and every one was a test that did not test the rule it named; all seven are now killed.
- Suite: `VERIFY_MODE=per-target tools/verify-suite.sh` -> **1737 Rust passed / 0 failed / 0 build
  errors**, **97 Go passed / 0 failed**, rc=0, head 57638f9. Baseline at the branch point was
  1695 / 97.

## Doing now
- Fresh-context adversarial review of the whole diff.

## Single next action
- Act on whatever the review confirms, then write
  `/Users/idide/wt/artie-research/build-G/F6-snapshot.md` (ONCE, at the end).
