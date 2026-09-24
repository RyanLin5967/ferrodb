# PRE-REGISTRATION — lease grace (F1 + adjacent point + F2). Written BEFORE any run. Amendments append-only.

- worktree: `/Users/idide/wt/ferrodb-lease-grace.noindex`, branch `lease-grace`
- base: `9aa6968` (main, "Merge D187")
- RED commit: `ea60cc4` — tests only, 551 added lines, 0 deleted (`git diff --numstat 9aa6968 ea60cc4`)
- FIX commit: the next `UNBUILT` commit after this file. Its sha is recorded in
  `artie-research/frontier/lane_lease_grace.md`, not here: a file cannot quote the sha of the
  commit that follows it.
- Nothing below has been run. Quiet mode: no cargo, no build, no test in this lane. Every
  prediction is from reading source.

## Owed before any count means anything

1. `cargo build --examples` first. `src/branch/lease_thread/tests.rs` changed at the RED commit,
   and `walk_newest` in `tests/integration_server_reaps.rs` excludes only `tests_*.rs` names —
   `tests.rs` is counted. Without the rebuild, every example-spawning test in that file fails on
   the staleness guard, which is not a result.
2. Per-target mode, and against the lead's certified count at `9aa6968` (this lane does not hold
   it). All deltas below are per-target.

## Tests added, by name, and the count they add

At RED (`ea60cc4`), +6:

| # | target | test |
|---|---|---|
| A  | `integration_server_reaps` | `a_lease_that_lapsed_only_while_the_server_was_down_survives_the_first_scan_after_restart` |
| B  | `integration_server_reaps` | `a_lease_that_had_already_expired_before_the_server_went_down_is_still_reaped_after_restart` |
| A' | lib `branch::lease_thread::tests` | `f1_a_lease_that_lapsed_while_nothing_was_scanning_survives_the_scans_after_a_restart` |
| C1 | `lease_expired_is_refused` (NEW target) | `forking_from_a_branch_whose_lease_expired_but_is_not_yet_reaped_is_refused` |
| C2 | `lease_expired_is_refused` | `writing_to_a_branch_whose_lease_expired_but_is_not_yet_reaped_is_refused` |
| C3 | `lease_expired_is_refused` | `altering_a_table_on_a_branch_whose_lease_expired_is_refused` |

With the FIX, +14, all lib (they use API the fix adds, so they have no red phase — their red is
the mutant table below):

| target module | tests |
|---|---|
| `cluster::tests` (+2) | `f2_the_standalone_lease_clock_is_its_anchor_plus_monotonic_elapsed_time`, `f2_the_process_lease_clock_never_reads_backwards` |
| `branch::tree_keys::tests` (+3) | `deadlines_after_a_mark_are_exactly_those_not_expired_at_it`, `a_deadline_key_decodes_to_the_deadline_and_id_it_was_built_from`, `the_alive_key_is_its_own_group_and_no_other_span_reaches_it` |
| `branch::table_catalog::f1_lease_grace` (+7) | `a_first_start_records_a_mark_and_extends_nothing`, `a_resume_moves_exactly_the_leases_running_at_the_mark_by_exactly_the_downtime`, `resuming_twice_at_the_same_reading_extends_the_outage_once`, `a_clock_behind_the_mark_moves_nothing_and_the_mark_follows_the_clock`, `the_mark_and_the_extension_survive_a_close_and_reopen_from_the_file_alone`, `a_mark_of_the_wrong_width_is_refused_rather_than_read_as_absent`, `enforced_lease_is_the_reapers_predicate_and_refuses_a_reaped_branch` |
| `branch::lease_thread::tests` (+2) | `f1_every_scan_records_the_mark_so_a_crash_does_not_lose_the_time_since_start`, `f1_a_reaper_that_never_resumed_the_clock_writes_no_mark` |

None is inside `reaper_suite!` or any other macro, so each `#[test]` is one test (checked by
reading; the D139 doubling does not apply).

**Totals, per-target:** RED = base + 6. FIX = base + 20 (6 + 14). A new target
(`lease_expired_is_refused`) must appear in the target list with `running 3 tests`; base + 17 at FIX
would mean it was never swept.

## Predictions at RED (`ea60cc4`, src unchanged)

The RED commit must COMPILE: every new test uses API that exists at `9aa6968` (`renew_lease`,
`LeaseThread::start/stop/stats`, `Harness::new_with`, `begin_session`, SQL through `executor::run`).
A compile failure here is a defect in the tests, not a red.

| # | predicted | failing assertion (first one reached) | why, from source |
|---|---|---|---|
| A  | **FAIL** | "a lease that lapsed only while the server was down was reaped on the first scan after the restart — F1" | `LeaseThread::start` spawns a loop whose first act is `scan_once`; the deadline is ~1 s in the past and nothing extends it. ALTERNATIVE red on a loaded box: if the first scan has not run within the 1 s window, it fails one assertion later ("did not report extending the fixture's one live lease") — still red, different line. |
| B  | PASS | — | deadline an hour before shutdown; reaped on first scan, as `:540` is. |
| A' | **FAIL** | "a lease that lapsed only while nothing was scanning was reaped by the first scans after the restart" (`reaped` = 1) | same mechanism, lib-level, with the scan count. |
| C1 | **FAIL** | panic "forked b3@g0 from b1@g0, whose lease has expired" | `begin_session_as_staged` calls `fork_staged`, which checks only `check_readable`; an expired `Live` parent is readable. Ids: parent `b1`, control child `b2`, refused child would be `b3` (fresh catalog, `next_id` from 1). |
| C2 | **FAIL** | panic "`INSERT INTO inventory VALUES (3, 7);` was accepted on b1@g0" | `stage_all` checks the session and the envelope, never the lease. |
| C3 | **FAIL** | panic "a schema edit was staged on b1@g0" | `stage_schema_edit` never reads the branch record at all. |
| every pre-existing test | unchanged | — | RED adds tests only. |

## Predictions at FIX

- A, B, A', C1, C2, C3: **PASS**.
- All 14 fix-commit tests: **PASS**.
- Every pre-existing test: **PASS**, with these named as the ones most likely to move, and why they
  should not:
  - `integration_server_reaps` (all 9 pre-existing). Startup now prints one more stderr line
    (`lease_resume_report`). None of its four texts contains `lease: reaped`, `lease: NOT reaping`,
    `lease: scan failed` or `error:` — the substrings those tests and `cli_run` assert absent.
    `:540` (deadline 0) and the CLI twin: deadline 0 < the mark, so not extended, reaped.
    `a_node_that_does_not_know_the_clusters_time_...`: log catalog and a joined process →
    `LeaseResume::Clustered`, start succeeds, heartbeat never armed.
  - `integration_capability_envelope::the_envelope_reads_one_funnel_while_three_reach_branch_state`:
    its three whitespace-stripped needle counts in `src/agent_sql/runtime.rs` are 1/1/3 at both
    `ea60cc4` and the fix — MEASURED with the test's own normalisation in python on both revisions
    (HEAD reproduces the expected 1/1/3, so the instrument is the test's). No `State`/`Workspace`
    field added.
  - `integration_simulate::the_losers_are_reaped_on_lease_expiry_...` and
    `::state_for_branches_the_reaper_took_...` (`lease_millis(1_000)`): the fix MOVES the candidate
    lease renewal from right after fork to right after the bodies run (`simulate.rs`). Without the
    move, the new write refusal would make any candidate body slower than 1 s error out. With it,
    the window in which "an unexpired lease is left alone" must hold only shrinks.
  - `branch::lease_thread::tests` (all pre-existing): log catalog → `LeaseResume::NoMark`,
    reaper never armed, no heartbeat, one extra stderr line at start. Gate acquisition counts are
    unchanged: the lease-clock resume runs inside the SAME `with_lock` as the reap resume.
  - `integration_cluster_grants::a_node_with_no_cluster_configured_reads_its_own_wall_clock_for_leases`:
    the anchored clock is non-decreasing and on the unix-epoch scale, which is all it asserts.
- `cargo clippy` and `cargo doc` were not run either; both are owed with the build.

## Mutants (fire-checks) — each must turn the named tests RED

| mutant | must fail |
|---|---|
| M1 `resume_leases`: skip `extend_leases_after` (extended = 0) | A, A', `a_resume_moves_exactly…`, `resuming_twice…`, `the_mark_and_the_extension_survive…` |
| M2 extend by `downtime + 1` | A (exact-deadline assertion), `a_resume_moves_exactly…`, `resuming_twice…` |
| M3 `deadlines_after(mark)` inclusive of `mark` | `deadlines_after_a_mark_are_exactly…`, `a_resume_moves_exactly…` (`at_mark` moved) |
| M4 heartbeat removed from `scan_once` | `f1_every_scan_records_the_mark…` |
| M5 reaper armed unconditionally (ignore `NoMark`/never-resumed) | `f1_a_reaper_that_never_resumed_the_clock_writes_no_mark` |
| M6 remove `refuse_if_lease_expired` from `begin_session_as_staged` / `stage_all` / `stage_schema_edit` | C1 / C2 / C3 respectively |
| M7 `enforced_lease` returns `Some` for `Quarantined` | `enforced_lease_is_the_reapers_predicate…` |
| M8 grant a fresh window instead: set every Live deadline to `max(D, now + 15 min)` at resume | B |

## Stated blind spots (no test here can see these)

- **F2 wiring.** Reverting `LeaseSource::LocalWall` to read `local_wall_millis()` directly passes
  every test: `f2_…anchor…` pins the pure arithmetic, and `f2_…never_reads_backwards` passes on a
  wall clock that does not step during the run. No in-process test can step the wall clock.
- **Atomicity of extension + new mark under a crash.** `resuming_twice…` shows one call does not
  extend twice; it cannot show that a crash between the two writes is impossible, because they
  are one stage/durable by construction and no test injects a crash inside `durable`.
- **Cost at scale.** `resume_leases` is O(live branches) writes in one fsync at every start.
  UNMEASURED; no arm at 10⁶ exists.

## Amendment 1 — before the fix commit

"`integration_server_reaps` (all 9 pre-existing)" is wrong: it has **8** at `9aa6968` and 10 at
`ea60cc4` (`git show 9aa6968:tests/integration_server_reaps.rs | grep -c '^#\[test\]'` → 8; the same
on the working file → 10). No total above used the 9; the +2 for that target stands.
