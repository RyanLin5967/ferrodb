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

## Amendment 2 — the lead's O(1) redesign, written before its fix commit

The lead (review of `0dcbe93`) rejected the per-branch extension. `extend_leases_after` rewrites
every live deadline and its DEADLINE key at every restart: O(live branches) writes at open, a
branch-count wall added by a correctness fix. The replacement is a **virtual lease clock**,
Chubby's stopped timer done as arithmetic:

- `TableBranchCatalog` keeps ONE durable cumulative downtime offset `D`, in the same key as the
  last-alive mark (`[0x08]` → `mark ‖ D`, 16 bytes).
- Deadlines are stored as `v = lease − D`, and read back as `v + D`, saturating both ways.
- `expired_before(now)` compares the stored `v` against `now − D`.
- A resume does `D += downtime`, written in the same stage/durable as the mark. No record is
  rewritten.
- Existing catalogs have no mark, so `D = 0` and every stored deadline keeps its meaning exactly.

**Red for it, committed first: `957e113`.** It adds the instrument
`TableBranchCatalog::key_rewrites` and the test
`table_catalog::f1_lease_grace::a_resume_writes_the_same_number_of_keys_whatever_the_number_of_live_branches`.

| at | predicted | why |
|---|---|---|
| `957e113` (O(N) design) | **FAIL**: 31 keys for 10 branches vs 301 for 100 | INFERRED: 3 per branch (record, old DEADLINE key removed, new one written) + 1 mark |
| the O(1) fix | PASS: 1 and 1 | only the `[0x08]` key is written |

**What the fix commit changes in tests, and why each change is not a test being bent to pass:**

| test | change | reason |
|---|---|---|
| A (`integration_server_reaps`, from `ea60cc4`) | the assertion `early.contains("1 live lease(s) extended")` is REMOVED | It pinned a count of rewritten leases. The O(1) design has no such count, and producing one would be an O(N) read at open. Its stated purpose ("the survival could be a scan that never ran") is still carried by `printed_downtime`, which panics without the resume line, and by the exact-deadline assertion `lease_deadline == deadline + downtime`, which is UNCHANGED and must still hold. Every other assertion in A is unchanged. |
| `tree_keys` `deadlines_after_…`, `a_deadline_key_decodes_…` | DELETED, with the functions they test | Both functions existed only for the per-branch extension; nothing calls them now. |
| `f1_lease_grace::a_resume_moves_exactly_…` | REPLACED by `a_resume_shifts_every_deadline_by_exactly_the_downtime_and_rewrites_no_record` | The old test asserted that leases at or before the mark, and quarantined ones, keep their value. Under the stopped timer EVERY stored deadline reads `downtime` later. Those that had expired stay expired (`D + v < now` iff `v < mark`), and that is asserted through `expired_before` at the same instants as before (4000, 4499, 4500), with the same answers. |
| `f1_lease_grace::resuming_twice…`, `…clock_behind…` | the `Resumed` literal's `extended` field becomes `offset_millis` | The variant's field changed; the asserted downtimes (0, 0) are unchanged. |
| `f1_lease_grace::…wrong_width…` | "8 bytes" → "16 bytes" | The value is now `mark ‖ D`. |
| `f1_lease_grace::…survive_a_close_and_reopen…` | reads `alive_state()` = `(4000, 3000)` instead of `alive_mark()` = `4000` | Same value, plus the offset, which must also survive. |

**New tests (+2):**
- `a_lease_written_after_a_resume_reads_back_exactly_as_it_was_given`: `fork` and `renew_lease` both
  store `lease − D`.
- `an_offline_deadline_of_zero_still_reads_as_expired_after_downtime`: `:540`'s `expire_lease`
  writes 0; stored as 0 (saturating), it reads back as `D`, which is expired at any real `now`.

**Counts, per-target:** RED `ea60cc4` = base + 6. `957e113` = base + 21. FIX (O(1)) = base + 21
(−2 tree_keys, +2 f1). New lib tests at FIX: `f1_lease_grace` 10, `lease_thread::tests::f1_*` 3,
`cluster::tests::f2_*` 2, `tree_keys` 1.

**Mutants, revised.** M1–M3 are replaced; M4–M8 are unchanged; M9–M13 are new.

| mutant | must fail |
|---|---|
| M1 resume does not add the downtime to `D` | A, A', `a_resume_shifts…`, `…survive_a_close_and_reopen…`, the O(1) test (its premise `lease == 4500`) |
| M2 `D += downtime + 1` | A (exact deadline), `a_resume_shifts…` |
| M3 `expired_before` compares stored `v` against `now`, not `now − D` | A, A', `a_resume_shifts…` |
| M9 `fork_staged` stores the caller's lease without subtracting `D` | `a_lease_written_after_a_resume…` |
| M10 `renew_lease` stores without subtracting `D` | `a_lease_written_after_a_resume…`, `an_offline_deadline_of_zero…` |
| M11 `get_raw` returns the stored `v` untranslated | A' (`moved > deadline`, and the reaper's re-read then expires it early), `a_resume_shifts…` |
| M12 `D` not loaded at `open` | `…survive_a_close_and_reopen…` |
| M13 reintroduce any per-branch write in `resume_leases` | the O(1) test |

**Blind spot, new and the most important one:** any read path that returns a stored deadline
without adding `D` compares a virtual deadline against a real clock, and that expires leases EARLY —
the destructive direction. The fix translates at every `TableBranchCatalog` method that hands a
record or a deadline out: `get`, `get_raw`, `scan`, `scan_ids`, `in_state`, `expired_before`,
`enforced_lease`, `reparent`, and the child `fork_staged` returns. `a_resume_shifts…` asserts
through `get`, `get_raw`, `scan`, `scan_ids`, `in_state`, `expired_before` and `enforced_lease`.
A method added later is not covered by anything but review.

## Amendment 3 — two test names differ from Amendment 2, and one test is stronger (before the fix commit)

- `…survive_a_close_and_reopen…` is named
  `the_mark_and_the_offset_survive_a_close_and_reopen_from_the_file_alone`.
- `…wrong_width…` is named `a_record_of_the_wrong_width_is_refused_at_resume_and_at_open`. It now
  also reopens the file and requires `open` itself to refuse. The reason: `open` loads `D`, and
  reading a damaged record as `D = 0` would read every stored deadline early. New mutant **M14**:
  `open` ignores an unreadable `[0x08]` record (loads `D = 0`) → that test fails.
- Counts are unchanged: `f1_lease_grace` has 10 tests (MEASURED: `grep -c '#\[test\]'` over the
  module in the working file → 10).

## Amendment 4 — the lead's two addenda: an F2 guard, and D206. Written before the D206 fix commit.

### F2 guard (`4dc1bd8`, test + a cfg(test) hook; no fix, because F2's fix is `447269e`)

- `cluster::tests::f2_a_wall_clock_step_mid_process_neither_expires_nor_revives_a_lease` steps
  THIS thread's wall clock ±1 h through `cluster::wall_step`, a thread-local offset on
  `local_wall_millis`, the process's one `SystemTime::now()` reader. It asserts through
  `LeaseDeadline::is_expired_now`, i.e. the decision path.
- **Predicted PASS at every commit from `447269e` on.** Its red is **M15**: put
  `local_wall_millis()` back at the `LeaseSource::LocalWall` arm of `lease_now_millis`, the exact line
  `447269e` replaced. Under M15 the first stepped assertion fails: "a FORWARD wall-clock step of one
  hour expired a ten-minute lease".
- Blind spot, stated at the hook: an inline `SystemTime::now()` at that arm bypasses
  `local_wall_millis` and passes the test.

### D206 red (`c3f62ab`, tests only), each predicted FAIL at `c3f62ab` and PASS after the fix

| test | first failing assertion at `c3f62ab` |
|---|---|
| `integration_cluster_grants::an_over_long_lease_cannot_forge_the_never_expires_sentinel` (ported verbatim from `31364b3`) | "an over-long lease forged the trunk sentinel" (`try_from_now(u64::MAX)` = `u64::MAX`) |
| lib `branch::types::tests::an_over_long_lease_from_now_does_not_forge_the_never_expires_sentinel` | same text, via `from_now` |
| lib `table_catalog::f1_lease_grace::a_shifted_deadline_never_forges_the_never_expires_sentinel` | "the downtime shift saturated a real deadline onto the never-expires sentinel" (`(u64::MAX − 1) + 3000` saturates to `u64::MAX`) |

All three are also red against `9aa6968` for the two `from_now`/`try_from_now` tests. The catalog
test's API does not exist there.

### What the D206 fix will do

- Every deadline COMPUTATION stops at `u64::MAX − 1`, through one function,
  `LeaseDeadline::saturating_deadline` (from `31364b3`). The computations are:
  - `from_now`
  - `try_from_now`
  - `to_lease_clock`'s `v + D`
  - the resume's `D += downtime`
- `to_stored` subtracts, so it cannot exceed its input.
- `u64::MAX` itself, when a caller passes it explicitly (`TRUNK_LEASE`, and a dozen benches and tests
  that fork with `LeaseDeadline(u64::MAX)`), is a **fixed point of both translations**. It is the
  caller's "never", not a computed value. `integration_system_views.rs:218` asserts that trunk reads
  `u64::MAX`.
- The cluster ledger stores `lease_millis` and computes no deadline (READ:
  `agent_sql/cluster.rs:285`, `:526`).

**One assertion of my own changes in the D206 fix commit.** In
`a_resume_shifts_every_deadline_by_exactly_the_downtime_and_rewrites_no_record` (`6fc317b`),
`lease(&c, never) == u64::MAX` becomes `u64::MAX − 1`. That branch was forked with `u64::MAX − 5`,
and the old assertion pinned exactly the forging D206 forbids.

### Counts, per-target, at the D206 fix

- **base + 25**: the +21 of amendment 2, the F2 guard (+1), and the three D206 tests (+3).
- Lib filter:
  `cargo test --lib -- f1_lease_grace lease_thread::tests::f1_ cluster::tests::f2_ the_alive_key_is_its_own_group an_over_long_lease_from_now`
  → **19** (11 + 3 + 3 + 1 + 1).

### Mutants, new

| mutant | must fail |
|---|---|
| M15 `LeaseSource::LocalWall => Ok(local_wall_millis())` | `f2_a_wall_clock_step_mid_process…` |
| M16 `saturating_deadline` = plain `saturating_add` (no `.min`) | the ported cluster test, `an_over_long_lease_from_now…`, `a_shifted_deadline_never_forges…` |
| M17 `to_lease_clock` uses plain `saturating_add` | `a_shifted_deadline_never_forges…` (`latest`) |
| M18 the resume's `D += downtime` uses plain `saturating_add` | `a_shifted_deadline_never_forges…` (the `offset` assertion) |
| M19 `to_stored` loses the `u64::MAX` fixed point | `a_shifted_deadline_never_forges…` (`small`, an explicit never written after the restart) |

### Blind spot (D206)

A record that ALREADY holds a forged `u64::MAX` — written by a build before this fix, with an
over-long lease — cannot be told from an explicit "never", and stays un-reapable, exactly as it is
today. Nothing rewrites it.

## Amendment 5 — the lead's review of `6fc317b`: D206 (already landed) and the stored deadline as a type. Written before the type commit.

**Item 1, D206, is already in the tree.** The lead's message crossed with the addendum-2 work:
- `c3f62ab` red: `31364b3`'s test ported, plus two more tests.
- `c8a7c39`: amendment 4.
- `a34b92c` fix: `from_now`, `try_from_now`, `v + D`, the downtime offset, and renewals (a
  renewal's deadline comes from `from_now` or is the caller's explicit value; `u64::MAX` is a
  fixed point).
- Mutants M16–M19.

Nothing further is committed for it here.

**Item 2: the stored deadline becomes a type.** All changes are in `src/branch/table_catalog.rs`
(plus one doc line in `record.rs`). A new private module `stored` holds:
- `StoredDeadline(u64)`;
- `StoredCore(CoreRecord)`;
- `StoredRecord(BranchRecord)`.

Their fields are private to that module, not to the file. There is no `Deref` and no raw accessor.
The only conversions are `outward(D)` (→ `LeaseDeadline` / `CoreRecord` / `BranchRecord`) and
`inward(D)`. What the rest of the catalog sees:
- `core()` returns `StoredCore` and `hydrate()` returns `StoredRecord`.
- `write_record` / `write_record_new` take a `Writable`: a `StoredRecord` borrowed as is, or a
  `BranchRecord` translated inward. The second is safe because a `BranchRecord` in this file can
  only ever be on the lease clock.
- The DEADLINE-index key and the expiry span are built inside `stored`, so the raw value never
  leaves it.

The measured width, from the count before the change: `self.core(` 20, `self.hydrate(` 9,
`self.write_record(` 6, `write_record_new` 1, `deserialize_core(` 3, `.lease_deadline` 8. That is
47 production sites, all in this one file.

**No pre-existing test is edited.** `d10_guard`'s `cat.write_record_new(&rec)` and
`serial_section_profile`'s `cat.write_record(&child, None)` still pass a `BranchRecord`; `Writable`
accepts it.

**One of my own assertions changes** (`a_resume_shifts_every_deadline…`, from `6fc317b`):
- Before: `c.core(..).lease_deadline() == LeaseDeadline(1_500)`.
- After: `c.core(..).deadline() == c.to_stored(LeaseDeadline(4_500))`.
- A `StoredCore` has no `lease_deadline()`, which is the point of the change. The new form asserts
  the same stored value (1500 at `D = 3000`), and the byte-identity assertion above it is unchanged.

**New test (+1):** `f1_lease_grace::reparent_hands_its_record_out_on_the_lease_clock`. `reparent` was
the one outward path translated but not asserted.
- Predicted PASS at the type commit, and it would also pass at `a34b92c`: it is coverage, not a
  red.
- Its discriminating power is against a wrong translation, e.g. `outward` applied twice. An omitted
  one no longer compiles.

**Counts, per-target:** base + 26. The lib filter
`cargo test --lib -- f1_lease_grace lease_thread::tests::f1_ cluster::tests::f2_ the_alive_key_is_its_own_group an_over_long_lease_from_now`
→ **20** (12 + 3 + 3 + 1 + 1).

**Mutants, new — compile-level, each must FAIL TO COMPILE (`cargo check --lib --tests`) with E0308:**

| mutant | expected error |
|---|---|
| M20 `get_raw` returns `self.hydrate(rec)` without `self.outward(..)` | expected `BranchRecord`, found `StoredRecord` |
| M21 `enforced_lease` returns `.then_some(rec.deadline())` | expected `LeaseDeadline`, found `StoredDeadline` |
| M22 `renew_lease` does `rec.set_deadline(lease)` without `self.to_stored` | expected `StoredDeadline`, found `LeaseDeadline` |
| M23 `expired_before` pushes `rec` instead of `self.outward_core(rec)` | expected `CoreRecord`, found `StoredCore` |

A mutant that COMPILES means the type is not doing its job, and that is a failure of this
amendment's claim.

**What the type does NOT stop** (stated, not solved):
- Code inside `mod stored` can do anything with `.0`. It is about 200 lines, reviewed as a unit.
- `Writable for BranchRecord` means someone could build a `BranchRecord` by hand with a virtual
  deadline and write it. No path in the file does; the rule is at the trait.
- `put` (cfg(test)) is one such path by design, and translates.

## Amendment 6 — the D198 adversary's caveats C1–C4 (`artie-research/frontier/d198_adversary.md`). Written before their fix.

**Red, at `7cd26ab` (tests only, compiled against `27b127d`'s API):**

| test | at `7cd26ab` | first failing assertion |
|---|---|---|
| `f1_lease_grace::a_catalog_with_a_last_alive_mark_refuses_expiry_questions_until_it_resumes` (C2) | **FAIL** | panic "expired_before answered [..] on a catalog whose downtime since its last mark has not been credited in this process (C2)" |
| `f1_lease_grace::a_heartbeat_carries_the_offset_on_disk_not_a_stale_copy_in_memory` (C3) | **FAIL** | left `Some((5000, 0))`, right `Some((5000, 3000))` |
| `lease_thread::tests::f1_a_clean_stop_records_the_moment_it_stopped_as_the_last_alive_mark` (C4) | PASS; its red is **M26** | — |

At the fix, all three PASS.

**The fix will do:**

- **C1.**
  - `impl Writable for BranchRecord` becomes `#[cfg(test)]`. The adversary READ, and the lead
    confirmed, that every production writer call passes a `StoredRecord`.
  - **M25 is closed by a TYPE, not by a source-text test.** The offset becomes
    `stored::LeaseOffset`. Its field is private to `stored`, and no public constructor exists. It
    is produced only by:
    - the catalog's in-memory cell (`OffsetCell::load`), which starts at 0 and is published only by
      a resume;
    - decoding the durable `[0x08]` record;
    - `credit`, applied to an existing offset.

    `inward` / `outward` take a `LeaseOffset`, so `StoredDeadline::inward(lease, 0)` is E0308 in
    every build.
  - **Why a type, not a text test.** A text test guards wording: `inward(lease, zero)` with a
    `let zero = 0;` walks around it. The type guards what the call receives. The same reasoning
    made the deadline a type.
  - `CoreRecord::with_lease_deadline` goes from `pub` to `pub(crate)`.
- **C3.** `record_lease_alive` reads the `[0x08]` record under `logical` and writes
  `(now, that record's D)`, never the in-memory copy. With no record it writes `D = 0`, which is what
  the disk says.
- **C2, the narrow form.** A `TableBranchCatalog` that holds a last-alive record, and has NOT run
  `resume_leases` on this instance, refuses `expired_before` and `enforced_lease` with "…has not
  resumed its lease clock…".
  - A catalog with no record answers as before.
  - Reads (`get`, `get_raw`, `scan`, …) still answer.
- **C4.**
  - Test A gains an independent two-sided bound on the server's printed downtime:
    `t_spawn − mark − 1000 ≤ downtime ≤ t_seen − mark + 1000`.
    - `mark` is read offline, before the server starts, through a new read-only accessor
      `TableBranchCatalog::last_alive_mark()`. It is an observing instrument.
    - `t_spawn` and `t_seen` are the test's own wall-clock readings.
    - The 1000 ms is cross-process clock slack.
  - The exact-deadline assertion is unchanged.

**C2 — the enumeration the lead required, and the ⚖ for Ryan.** From `git grep` in `src/` and `tests/`:

- Callers of `expired_before`:
  - production: `reaper.rs:844` (`expired_candidates`, reached from `scan_once` and
    `Reaper::reap_expired`);
  - test decorators: `lease_thread/tests.rs:841`, `reaper.rs:1700`, `:2134`, `tests/d124…:135`,
    `tests/d41…:174`, `tests/w4_sweep…:121`;
  - tests: `catalog.rs:1269–1353` (log catalog), `table_catalog.rs:2405`, `:2415`, `:2900–2902`, and
    mine.
- Callers of `enforced_lease`: production `runtime.rs:3028` (every agent write and non-trunk fork);
  my tests.
- Callers of `reap_expired`:
  - `reaper.rs` suite (`:1040`–`:2281`, which runs against BOTH catalogs via `reaper_suite!`);
  - `lease_thread/tests.rs:1150`;
  - `tests/adv_f5_probe.rs:275`, `d15…:64`, `d19…:52`, `integration_cluster_grants.rs:162`, `:165`,
    `:424`, `:432`, `:931`, `integration_simulate.rs:773`, `:778`, `:842`, `:1135`,
    `integration_zero_copy_fork.rs:221`.

**The adversary's strict form** refuses every unresumed catalog. That would turn every one of the
above red that uses a `TableBranchCatalog` without a resume, plus every agent write through a runtime
over one (`enforced_lease`): the `reaper_suite!` table half, `d15`, `d19`, `zero_copy_fork`,
`integration_simulate`, `integration_cluster_grants`' `store`, and more. Per the lead's rule those
tests are not edited. **⚖ Ryan: whether to take the strict form**, which means making every such test
and embedder resume first. It is NOT done here.

**The narrow form turns NONE of them red** (INFERRED from the same enumeration):

- A last-alive record is written only by `resume_leases` and `record_lease_alive`. Those run only in:
  - the two binaries, which resume before serving;
  - the lease-thread and `f1` tests on this branch, each of which resumes before asking.
- Every test that reopens a binary-marked catalog does so offline. These are
  `integration_server_reaps`' `arena_state`, `expire_lease`, `set_lease` and `interrupt_reap`: they
  read records and renew or set state, and ask no expiry question (READ).
- `tests/integration_fork_kill9.rs:110` reopens a catalog written by `examples/fork_kill9`, which runs
  no `LeaseThread`, so there is no mark (READ: `LeaseThread` appears in `examples/` only in
  `pgserver` and `outer_runtime_lock`).
- It closes the adversary's schedule, because that schedule requires a prior `pgserver` run and
  therefore a mark.
- **Residual:** an embedder that resumes and then never heartbeats. The next start credits its uptime
  as downtime. That is the extension direction only; it is recorded.

**One of my own tests changes, and the change is an ADDED line:**
`the_mark_and_the_offset_survive_a_close_and_reopen_from_the_file_alone` asked `expired_before` of a
reopened, marked, unresumed catalog, which is exactly what C2 now refuses. It gains
`re.resume_leases(4_000)` before those two assertions: downtime 0 against the mark of 4000, so `D`
stays 3000 and both assertions are unchanged. The durability of `D` is still asserted before the
resume, through `get_raw` (4500).

**Mutants, new:**

| mutant | gate | must |
|---|---|---|
| M24 in `set_root`, decode raw bytes (`BranchRecord::deserialize_core(..)?.into_hydrated(..)`) and hand the `BranchRecord` to `write_record` — C1's named path | `cargo check --lib` (non-test) | FAIL: E0277, `BranchRecord: Writable` not satisfied. It COMPILES under `--tests` (the impl is `cfg(test)`), which is why the gate is non-test |
| M24r in `scan`, return a raw-decoded `BranchRecord` without `outward` | `cargo test --lib a_resume_shifts_every_deadline` | compiles everywhere. **Registered compile-survivor.** Killed behaviourally: `scan` must read 4500, and the raw value reads 1500 |
| M25 `renew_lease`: `rec.set_deadline(StoredDeadline::inward(lease, 0))` | `cargo check --lib --tests` | FAIL: E0308, expected `LeaseOffset`, found integer |
| M26 delete `record_lease_alive` in `LeaseThread::shutdown` | `cargo test --lib f1_a_clean_stop` | FAIL |
| M27 delete the C2 refusal in `expired_before` / `enforced_lease` | `cargo test --lib a_catalog_with_a_last_alive_mark` | FAIL |
| M28 C3 reverted: heartbeat writes the in-memory `D` | `cargo test --lib a_heartbeat_carries_the_offset` | FAIL |
| M29 resume inflates the measured downtime by 5000 ms in both the print and the shift | `cargo test --test integration_server_reaps a_lease_that_lapsed` | FAIL at the new upper bound; the exact-equality assertion alone passes it |

**Correction to amendment 5's M23 text** (the adversary, Q2): M23 is still E0308. But
`let mut out = Vec::new()` infers `Vec<StoredCore>`, so the error lands at `Ok(out)` as "expected
`Vec<CoreRecord>`, found `Vec<StoredCore>`", not at the push. A verifier should match that text.

**Counts, per-target:** RED `7cd26ab` = base + 29 (+2 `f1_lease_grace`, +1 `lease_thread`). FIX =
base + 29. Lib filter
`cargo test --lib -- f1_lease_grace lease_thread::tests::f1_ cluster::tests::f2_ the_alive_key_is_its_own_group an_over_long_lease_from_now`
→ **23** (14 + 4 + 3 + 1 + 1).

## Amendment 7 — one more survivor, found while writing the M25 type (before the fix commit)

M25b: `rec.set_deadline(StoredDeadline::inward(lease, OffsetCell::new().load()))` COMPILES.

- A new catalog must be able to start at `D = 0`, so `OffsetCell::new()` is reachable from the rest
  of `table_catalog.rs`, and a fresh cell yields a zero offset.
- The type does stop the literal (M25's `0`) and any `u64`. It does not stop someone who builds a
  new cell on purpose.
- M25b is **a registered compile-survivor**, killed behaviourally by M10's test,
  `a_lease_written_after_a_resume_reads_back_exactly_as_it_was_given`, which reads 13000 where
  10000 is due.
- Of the same deliberate class: `AliveState::decode(&[0; 16])?.offset()`.
