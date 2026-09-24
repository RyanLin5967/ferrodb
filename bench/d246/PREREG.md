# D246 pre-registration: amendments

The original pre-registration lives in two places, both committed before any run:
- the header of `tests/d246_pgserver_provenance_survives_restart.rs`, at `b6960d5`;
- lane report §2 (`artie-research/frontier/lane_d246_pgserver_provenance.md`).

**This file only APPENDS to it.** An amendment never rewrites an earlier expectation. It quotes the old one, states the new one, and gives the reason.

## A1 (2026-09-24T10:16Z): C2 keeps "the decode is Ok" and drops the both-actors premise

**Governs:** claim C2 of `pgserver_provenance_survives_a_restart_and_its_log_decodes_cold` in `tests/d246_pgserver_provenance_survives_restart.rs`. It is this lane's own test and has never been on `main`.

**Decided by:** the lead, 2026-09-24 ("§5 is NOT Ryan's"). My ⚖ (lane report §5) proposed it.

**The expectation being replaced, quoted from `eca45fb`:** in the `Ok` branch of the cold decode of `[base_lsn, next_lsn)`,
> `premise: the decoded range must declare agent-a and agent-b under two slots, or it proves nothing about slot reuse; it declared {:?}`

That premise failed the test unless the decoded runs mapped `agent-a` and `agent-b` to two different slots.

**The new expectation:**
- C2 is "the cold decode of `[base_lsn, next_lsn)` is `Ok`". The `Ok` branch asserts nothing more.
- C1, C3 and C4 are unchanged. C3 is the slot-reuse claim: `agent-b`'s slot in the second process differs from `agent-a`'s in the first, read from each process's own `ferro_row_authors`. It holds on both trees.
- The header's pre-registered table is left as written; a note under it points here.

**The reason.** The lead's 09:44Z D227 run-half re-scope removed d216's re-declaration of old runs. So on the D246 × `d216-clean-restart` tree, process 2's open checkpoints and truncates `agent-a`'s declaration. The decoded range then declares only `agent-b`, and the premise fails even with the fix. The premise only ever held because the log kept the first process's declaration, and that is exactly what d216 changes. C3 makes the slot-reuse claim without depending on what the log retains.

**Applied on this branch, before the merge,** so the merge needs no test edit. On the D219 line the premise still held, so dropping it here removes a check but changes no verdict: C3 fails whenever the premise would have caught reuse.

**Pre-registered outcomes, with the runner's steps renamed to the new tip:**
- **GREEN** at the fix: 1 passed.
- **M1** (`examples/pgserver.rs` restored to `fbfe038`, no durable store) on THIS branch: FAILED, and it must still list C1, C3 and C4. C2 fails too, because at this base the decode is refused ("declares provenance slot 1 twice with different actors"). So four failures, as before. On the d216 merge, C2 may pass under M1, but C1, C3 and C4 must still fail.
- **M2** (the store on another file): FAILED, listing exactly one failure (C4), as before.
- **RED** is unchanged: step (a) runs the committed red commit `b6960d5`, whose test still carries the premise.

## A2 (2026-09-24T10:16Z): a fork's NEW run is made durable after the catalog guard, not under it

**Governs:** new tests. None existed before this entry.

**Decided by:** the lead, 2026-09-24: "§6.1 must be fixed, not recorded".

**The defect (lane report §6.1).** With D246, pgserver pays `DurableProvenanceStore::intern`'s fsync once per NEW run at `BEGIN AGENT SESSION`. It happens inside `begin_session_as_staged`, under `state` (`runtime.rs:1684`) and inside pgwire's `ServerContext::catalog()` guard (`:1701` at `eca45fb`). So a fork that brings a new run serialises every statement behind one fsync. That is the S5 shape D159 removed for the fork's own sync.

**The change being tested:**
- **Staging.** `begin_session_as_staged` interns through a new `ProvenanceStore::intern_pending`, which queues the run record (D219's `pending`) and writes and syncs nothing.
- **Completing.** `ForkDurability` gains the run. `complete()` awaits the branch ticket, as now, and then `ProvenanceStore::await_run(prov)`.
- **What `await_run` does.** It writes the pending run records, in order, and awaits ONE group-committed sync. The group is `branch/group_commit.rs`'s `CommitGroup`, reused rather than copied. The sync is issued OUTSIDE the store's file lock, so a fork staged meanwhile does not wait on it.
- **Drop.** `ForkDurability`'s `Drop` also awaits the run, so a forgotten `complete()` stays durable.

**Tests.** The integration file is `tests/d246_fork_run_is_durable_after_the_guard.rs`. It uses only API that exists at `eca45fb`, so it compiles at the base.

Instruments:
- `provenance().sync_counts()` (`total()` and `runs`), read while a stand-in for pgwire's guard is held and again after `complete()`;
- `TableBranchCatalog::syncs_issued()` for the fork's own sync;
- a SECOND `DurableProvenanceStore::open` on the same file, which proves the record reached the file (the counter alone would pass a sync that wrote nothing).

| test | claim | at `eca45fb` (red) | with the fix |
|---|---|---|---|
| T1 `a_new_runs_record_is_synced_after_the_guard_not_under_it` | staged under the guard: provenance syncs +0, branch syncs +0 | **FAILS**: provenance +1 | holds |
| | after `complete()`: provenance +1 (booked `runs`), branch +1 | **FAILS**: provenance +0 (already paid) | holds |
| | the reopened file holds the run | holds | holds |
| T2 `a_known_runs_fork_issues_no_provenance_sync` | premise: after the first fork of run `r` completes, the reopened file holds `r`; then a second fork of `r`: staged +0, complete +0 provenance syncs | holds | holds (non-discriminating on purpose: a repeat must stay free) |
| T3 `new_runs_staged_before_either_completes_share_one_provenance_sync` | two new runs staged: +0; both completed: +1 in total; the reopened file holds both | **FAILS**: +2 staged, +2 total | holds |
| T4 `a_staged_fork_dropped_without_completing_still_makes_its_run_durable` | the reopened file holds the run after the `ForkDurability` is dropped | holds | holds |

T1 and T3 collect their claims before asserting, so a red run lists every failed claim. The first failing claim at `eca45fb` is T1's staged +1.

Unit tests in `src/provenance/durable.rs`, added with the fix because they name its new API. They can only go red under a mutant:
- **U1** `a_runs_group_sync_does_not_hold_the_lock_every_staged_write_needs`: while `await_run`'s sync is held in flight by a test-only gate, `intern_pending` and `stamp_pending` from another thread return within 10 s.
- **U2** `a_second_await_of_a_run_already_written_waits_for_its_sync`: while that sync is in flight, a second `await_run` of the same run (a repeat run's fork) has NOT returned 2 s later, and does return once the sync is released.
- **U3** `a_run_a_synchronous_append_already_carried_costs_its_await_nothing`: a pending run carried by a synchronous `stamp_row` sync costs `await_run` no further sync.
- **U4** `interning_a_run_whose_record_is_pending_makes_it_durable_first`: `intern` keeps its contract (durable on return) for a repeat whose record is still pending.

**Mutants** (`bench/d246/mutants.py`; each must match exactly one site at the fix tip, checked by `--check`):

| mutant | edit | must fail | must pass |
|---|---|---|---|
| M3 `intern_under_the_guard` | `begin_session_as_staged` calls `intern` again: the sync inside the guard | T1, T3 (exactly as at red) | T2, T4 |
| M4 `complete_skips_the_run` | `complete()` does not await the run | T1 (complete +0; the file lacks the run), T2 (its premise), T3 | T4 |
| M5 `drop_skips_the_run` | `Drop` does not await the run | T4 | T1, T2, T3 |
| M7 `run_sync_under_the_lock` | `await_run` keeps the file lock across the sync | U1 | U2, U3, U4 |
| M8 `written_run_is_not_awaited` | `await_run` returns at once when the record is already written | U2 | U1, U3, U4 |
| M9 `synchronous_sync_covers_nothing` | a synchronous append no longer marks the group durable | U3 | U1, U2, U4 |
| M10 `intern_repeat_does_not_await` | `intern` returns a pending repeat without awaiting it | U4 | U1, U2, U3 |

(M1 and M2 are the pgserver mutants of the lane's first pre-registration. M6 is A3's.)

**Per-fork fsync count, pre-registered.** For a fork of a NEW run: 1 branch sync and 1 provenance sync, both after the guard, both group-committed. Concurrent forks share each one. For a known run: 1 branch sync and 0 provenance syncs. Before the fix, a new run's provenance sync was inside the guard.

## A3 (2026-09-24T10:16Z): a publish never declares to the log a run its provenance file lacks

**Why A2 needs it.** Before A2, a run's record was synced at `intern`, before any statement could name the run. So the WAL's `RunIdentity` record for a slot (written by `bind_run` at publish) could never be durable ahead of the provenance file's `Run` record for that slot.

After A2 that window exists. A `MERGE` of a branch whose `BEGIN` has not completed (another connection naming `b_N` between pgwire's guard release and the fork's `complete()`) would:
1. declare the slot to the WAL;
2. commit, with a WAL fsync;
3. only then write the run record, in the provenance sync that follows the commit.

A crash between the WAL fsync and that sync leaves the log declaring a slot the provenance file never issued. The reopened store then reissues that slot to another actor. That is exactly D246's defect, reached by another door.

**The change being tested:** `publish_evaluation_as` calls `provenance().await_run(snapshot.prov)` before `bind_run`. When the run's record is already durable, which is every ordinary case, that is a map lookup under the file lock with no I/O. The call waits on disk only in the window above, where the wait is required.

**Tests**, in `src/agent_sql/runtime.rs`'s `mod tests`. They compile at `eca45fb`: they use `fail_next_append`, the `prov_store` field and `executor::run_staged`, all of which exist there.
- **R1** `a_publish_never_declares_to_the_log_a_run_its_provenance_file_lacks`. The test stages `BEGIN AGENT SESSION` through `run_staged` and holds its `ForkDurability` uncompleted, inserts a row, arms `fail_next_append`, and runs `MERGE`. The claim: every slot the cold decode of the WAL declares is in a second `DurableProvenanceStore::open` of the file, under the same actor.
  - At `eca45fb`: **holds** (the run was synced at staging).
  - With the fix: **holds** (the `MERGE` is refused at the pre-bind await, before the log hears of the run).
  - Under **M6** `publish_before_the_run_is_durable` (the pre-bind await deleted): **FAILS**. The `MERGE` commits slot 1 to the log, then its row-author sync takes the injected failure, so the file holds no run.
- **R2** `a_publish_of_an_uncompleted_fork_declares_its_run_and_the_file_holds_it`. This is R1's control, with no injection. The claim: the `MERGE` is `Ok`, and the decode declares slot 1 for `('a', 'r1')`, which is R1's premise that the instrument sees this path. The file holds the run. It holds at `eca45fb`, with the fix, and under M6.

## A4 (2026-09-24T11:05Z): the fresh review's findings on `eca45fb..efd3541`, fixed

A fresh-context review of A1–A3 found no BLOCKER and five DEFECTs. It also found nine NITs. Its predictions for T1–T4, R1–R2, U1–U4 and M3–M10 matched A2/A3 by reading. Each finding below was re-read against the source before this entry was written. **A2 and A3's expectations stand; this entry only adds.**

**D-1 — "a failed fsync is not retried" held only sequentially.**
- `sync_handle` is `try_clone()` of the append descriptor. Both are one open file description, so on Linux an fsync error reported to one is not reported to the other.
- `sync_runs` checked the poison flag BEFORE its fsync and outside the file lock. So a leader could:
  1. pass the check;
  2. lose the EIO to an in-lock writer whose own fsync failed first and poisoned the store;
  3. have its own fsync return 0;
  4. acknowledge the run.
- **Fix:** `sync_runs` checks the flag AFTER its fsync, under the file lock, and the check before it is removed. This is sound because every in-lock writer holds the lock from its fsync to its `poisoned.store(true)`, so an error it consumed is visible by the time this leader can take the lock.
- **Test U5** `a_run_sync_overlapping_a_failed_append_is_not_acknowledged`:
  - hold the group sync in flight with the gate;
  - poison the store through an injected `stamp_row` failure;
  - release: `await_run` must return `Err`.
  - **RED at `efd3541`** (it returns `Ok`).

**D-2 — A3's await sat after the schema apply**, so a refusal there returned `Err` with the target's schema installed and logged. That breaks E82's rule, "everything this merge can refuse, it refuses before the first byte is written".
- **Fix:** hoist it into the plan phase, before `ProvenanceFlush::new` and the plan loop.
- **Test R3** `a_publish_refused_for_its_runs_record_leaves_the_schema_unchanged`: the uncompleted-fork rig with a staged `ADD COLUMN`, and the next append injected to fail. The `MERGE` is refused and `t` keeps 2 columns. **RED at `efd3541`** (3 columns).

**D-3 — D246's own orderings had no test and no named survivor.**
- New tests:
  - **U6** `a_stamp_queued_behind_a_run_is_never_left_written_but_unsynced` (only run records are left written-but-unsynced; `flush` must sync a stamp queued behind a run whose group sync is in flight);
  - **U7** `a_poisoned_store_refuses_a_pending_intern_and_leaves_the_index_alone`;
  - **U8** `a_poisoned_store_refuses_await_run_and_writes_nothing`. U8 also covers N-9.
- U6 and U7 hold at `efd3541`, so they are red only under a mutant. U8 is **RED at `efd3541`** on its N-9 half: `await_run` of a run absent from `run_seqs` returns `Ok` on a poisoned store.
- **Pre-registered SURVIVORS**, which no test here can kill, as D219 did for its M4. Each needs a concurrent leader or a failing fsync at an exact instant, and nothing here fault-injects the fsync itself:
  - **M16** `sync_before_write`;
  - **M17** `ticket_before_write` (`await_run` takes its tickets before its write);
  - **M18** `covered_before_sync` (a synchronous append marks the group durable before its fsync).

**D-4 — D219's M4 and M12 match 0 sites on any tree containing D246**, because A2 split `append_all_locked` into `write_locked` + `append_all_locked`.
- D219's runner pins `50e175b` and is unaffected.
- On trees containing D246, their replacements are:
  - **M15** `pending_written_last`: the same edit in `write_locked`. It must fail `provenance::deferred::tests::stamps_through_the_stamper_ride_the_next_sync_and_write_the_same_file`, D219's M12 target.
  - **M16**: D219's M4 at its new site, still a survivor.

**D-5 — a failed `complete()` left the connection inside the session it had just refused.**
- `dispatch` sets `session.agent` before the three completion sites (`executor::run`, `dispatch::run_agent_stmt`, pgwire extended) call `complete()?`.
- D246 routes provenance failures into that path. D159's branch-sync failures were already there.
- **Fix:** `ForkDurability::complete_for(&mut session.agent)` clears the session when the sync fails, and all three sites use it. The branch stays in memory until its lease reaps it, exactly as for a client that disconnects.
- **Test R4** `a_failed_completion_closes_the_session_it_opened` calls `complete_for` directly with an injected failure. It names the new method, so it is red only under **M24** `failed_complete_keeps_the_session`.
- Why not through `executor::run`: a lib test that runs an agent statement through dispatch can be refused by `designated::tests`, the one lib test that designates a runtime process-wide (review N-1). So R1–R4 now drive the runtime API directly: `begin_session_as_staged`, `write`, `stage_schema_edit`, `merge`. The rig changes; R1's and R2's claims do not.

**N-1:** R1 now also asserts that the refusal IS the injected failure ("injected provenance append failure").

**N-2:** **M11** `repeat_run_syncs` makes `await_run` sync even for a run absent from `run_seqs`. It must fail T2 (complete +1) and only T2 among T1–T4.

**N-3, declined.** The review suggests C2 keep "the range declares agent-b". Whether agent-b's declaration survives depends on whether a checkpoint fires between process 2's `MERGE` and its kill, and on d216's re-declaration rule. The test cannot assert that on both trees. C2 is not vacuous without it: at M1 on this base the decode is refused.

**N-4, N-7:** stale comments corrected (`runtime.rs` merge-sync count, `SyncCounts::total`, `append_locked`, `ForkDurability::drop`).

**N-5 — a plain `ALTER` refused mid-restamp left the stamps queued before the refusal pending**, with no flush guard. That is a D219 residue, and it also made `await_run`'s "SQL shapes never leave a stamp ahead of a run" false.
- **Fix:** `apply_plan` flushes what it queued and then returns the refusal.
- **Test** `catalog::alter::tests::a_rewrite_refused_mid_restamp_still_writes_the_stamps_before_it` uses a store that refuses the 6th `stamp_pending`. The flush after the ALTER must issue 0 syncs, because nothing is left pending. **RED at `efd3541`** (1).
- **M25** `refused_restamp_leaves_stamps_pending` restores the old loop.

**N-6:** A2's per-fork count covers `BEGIN AGENT SESSION`, the staged path pgwire completes after its guard. SIMULATE's and cluster's forks call `begin_session_as` inside their own statement and pay both syncs there, as before D246. Unchanged by this lane.

**N-8:** the runner refuses any `cargo test` step that collected zero tests (rc 97, recorded in the step file). The green phase adds `d154_length_prefix_refusal`, `agent_sql_surface`, `integration_prompt_clause` and `integration_run_identity_feed`, which all reach the changed await path.

**Mutants added** (each must match exactly one site at the new fix, by `--check`):

| mutant | must fail | must pass |
|---|---|---|
| M11 `repeat_run_syncs` | T2 | T1, T3, T4 |
| M15 `pending_written_last` | D219's `stamps_through_the_stamper_ride_the_next_sync_and_write_the_same_file` | — |
| M16 `sync_before_write` | — (SURVIVOR) | all |
| M17 `ticket_before_write` | — (SURVIVOR) | all |
| M18 `covered_before_sync` | — (SURVIVOR) | all |
| M19 `no_poison_check_after_the_run_sync` | U5 | U1–U4, U6–U8 |
| M20 `await_writes_every_pending_record` | U6 | U1–U5, U7, U8 |
| M21 `intern_pending_ignores_the_poison` | U7 | U1–U6, U8 |
| M22 `await_run_ignores_the_poison` | U8 | U1–U7 |
| M23 `await_after_the_schema_apply` | R3 | R1, R2, R4 |
| M24 `failed_complete_keeps_the_session` | R4 | R1–R3 |
| M25 `refused_restamp_leaves_stamps_pending` | the N-5 test | the other `catalog::alter::tests` |

### A4a (2026-09-24T11:20Z): the N-5 test's refusal point, corrected before the test was written

A4 says the N-5 test's store "refuses the 6th `stamp_pending`". The packed fixture guarantees only that SOME row moves, not how many, so a fixed 6th could miss the loop entirely. **Replaced by:** a twin ALTER counts the restamps `m`, with the premise `m >= 2`. The test's store then allows `m / 2` and refuses the next. The expectations are unchanged: refused by the test's store; the later `flush` issues 0 syncs; **RED at `efd3541`** (1 sync).
