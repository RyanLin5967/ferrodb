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

## Amendment 8 — C2b, the reap re-check (lead). Written before its fix.

**Red at `fb9bc43`** (test only, against `d87a8fc`'s API):
`lease_thread::tests::f1_an_unresumed_marked_catalog_is_not_reaped_by_the_recheck_either` →
**FAIL**. The panic is "the re-check answered Reaped on a catalog that refuses expiry questions".
`reap_if_still_expired` decides from `get_raw`, which C2 does not refuse.

**The fix will do:** `reap_if_still_expired` asks `BranchCatalog::enforced_lease`, which is the
refused predicate and the reaper's own, and no longer reads `get_raw(..).lease_deadline`.
- `Err(Branch)` → `ReapOutcome::Refused`. This is counted, and it carries the catalog's reason, so
  D127's semantics are unchanged. The C2 refusal itself arrives through this arm.
- `Ok(None)` → `NotExpired`. That covers a branch no longer enforced, e.g. one quarantined between the
  candidate query and the re-check. The old read would have reaped it on its long-expired deadline.
  This is a behaviour change, in the direction of keeping.
- `Ok(Some(d))` → `NotExpired` if `d` is not expired; otherwise `reap` runs, as before.

**The existing `reaper.rs` test at ~2385–2477** (`a_refused_reap_is_not_the_same_answer_as_a_lease_that_moved`,
instantiated for BOTH catalogs by `reaper_suite!`), predicted per arm (INFERRED):

| arm | today | after the fix |
|---|---|---|
| 1 | Reaped; `refused` unchanged | the same: Live, deadline 0 |
| 2 | NotExpired; `refused` unchanged | the same: `u64::MAX − 1` |
| 3 (never-minted id) | `get_raw` → `NotFound` → Refused, +1 | `enforced_lease` → `core` `NotFound` (table) / `get` `NotFound` (log) → `Branch` → Refused, +1 |
| 4 (stale generation) | `get_raw` is generation-blind, so `reap(stale)` refuses → Refused, +1, branch Live | `check_readable` refuses `Reaped` first → Refused, +1, branch Live |

**None turns red.** No ⚖.

**Other re-check exercisers, checked:**
- `lease_thread::tests` d98 (renewed under the sweep → NotExpired) and d127 (`RefusesLiveChildren`:
  the refusal comes from `reap`, after a re-check that passes). Their decorators do not override
  `enforced_lease`; the default reads through their forwarding `get`.
- `tests/d124_owner_record_refusal.rs` hides records from `get_raw` for the `free_page` /
  `drain_pending` sites only, which the re-check no longer touches.

**Mutant M30:** restore the `get_raw(..).lease_deadline.is_expired_at(now)` re-check → the C2b test
FAILS.

**Counts, per-target:** base + 30. Lib filter
`cargo test --lib -- f1_lease_grace lease_thread::tests::f1_ cluster::tests::f2_ the_alive_key_is_its_own_group an_over_long_lease_from_now`
→ **24** (14 + 5 + 3 + 1 + 1).

## Amendment 9 — the D198 re-review (`frontier/d198_rereview.md` @ `f05b325`) and the lead's FirstStart policy. Written before their fixes.

### Committed so far, with predictions

| commit | test | at that commit | after the fixes | its red |
|---|---|---|---|---|
| `b5bf3a3` | `f1_lease_grace::a_pre_d198_catalog_credits_the_downtime_since_its_file_was_last_written` | **FAIL**: "a lease that lapsed only inside the outage before the first D198 start was charged that outage" | PASS | this commit |
| `b5bf3a3` | `f1_lease_grace::a_migrated_legacy_log_credits_the_downtime_since_the_source_was_last_written` | **FAIL**: "a migrated lease that lapsed only inside the outage was charged it" | PASS | this commit |
| `b5bf3a3` | `f1_lease_grace::a_pre_d198_catalog_with_no_live_branch_does_not_move_the_offset` | PASS (control) | PASS | M32 |
| `b5bf3a3` | `f1_lease_grace::set_root_and_set_state_after_a_resume_leave_the_lease_where_it_was` | PASS (pin) | PASS | M24b, M24c |
| `802d379` | `f1_lease_grace::expired_before_examines_only_the_rows_it_answers_even_when_the_offset_exceeds_every_lease` (with the instrument `expiry_rows_examined`) | PASS | PASS | **M3**: 25 examined vs 5 answered. M3 is now killable (re-review R1). |

### The fixes that follow

- **(5) FirstStart policy (lead).** At a `FirstStart` of a catalog whose DEADLINE index is non-empty
  — that is, it holds a live non-trunk lease; checked in O(log N) by the group's first key — `D` is
  credited `now − mtime` once, in the same key write as the first mark. The outcome is reported as a
  new `LeaseResume::FirstStartFromFileTime { now_millis, file_mtime, credited_millis }`.
  - `mtime` is the catalog file's own, read by `open_sidecar` BEFORE the open touches the file. For a
    migration it is the SOURCE `.branches` log's, read by `default_for_database` before it migrates.
  - Unknown in three cases, and then the result is a plain `FirstStart`, today's behaviour:
    - a fresh file;
    - a catalog built with `create` / `open` over a caller's pool;
    - an unreadable mtime.
- **(3) C1 residual.** `StoredRecord::inward` (the general `BranchRecord` → `StoredRecord`) becomes
  `#[cfg(test)]`, beside the `Writable` impl that uses it. The catalog helper `inward` is removed.
  Production's only door is `StoredRecord::inward_at_zero(rec, offset) -> Result`, which REFUSES
  unless `D = 0`, the one case where a lease and its stored value coincide. Its callers are `create`
  (trunk) and `migrate_from` (a brand-new catalog). A new test,
  `f1_lease_grace::the_production_inward_door_refuses_once_the_offset_is_not_zero`, forces the guard
  to fire (+1). The overclaiming doc on `TableBranchCatalog::lease_offset` is corrected.
- **(4)** The C2 refusal text names `LeaseThread::start` and says that a bare `resume_leases` without
  heartbeats is the credited-uptime residual. The asserted phrase "has not resumed its lease clock" is
  kept.
- **(6)** `reaper.rs`'s "adds no second opinion" paragraph is rewritten for C2b. ARM 4's comment gains
  a sentence recording that the stale handle is now refused by the re-check. This is comment-only; no
  assertion changes.
- **(8)** A cluster member over a marked `TableBranchCatalog` refuses for ever. RECORDED only.

### (7) Corrections to earlier amendments (re-review R6)

- **M10**'s killers: only `a_lease_written_after_a_resume_reads_back_exactly_as_it_was_given`.
  `an_offline_deadline_of_zero…` cannot kill it, because `renew(0)` stores 0 with or without the
  subtraction.
- **M11**'s mechanism: A' fails at `moved > deadline`. Since C2b the reaper's re-read is
  `enforced_lease`, which is translated, so the "re-read then expires it early" clause is withdrawn.
- **M25b**'s numbers: the mutant is on `renew_lease`'s line, so the failing read is **23000 where 20000
  is due** (the renew half of `a_lease_written_after_a_resume…`). 13000 was M9's number.
- **M5**'s kill line: M5 is killed at the premise `counters.snapshot().scans == 3` of
  `f1_a_reaper_that_never_resumed_the_clock_writes_no_mark`, not at its named assertion. Under M5
  the first scan's heartbeat marks the catalog, and C2 then refuses the later candidate queries.
- **M29**'s kill has a timing premise: `t_seen − t_resume < 4000` ms. A server frozen after its
  resume would let M29 survive, and quiet mode's fan guard SIGSTOPs `target/debug` binaries. That is a
  false negative, never a false failure.

### New mutants

| mutant | must fail |
|---|---|
| M31 FirstStart credit ignores the mtime (`D = 0`) | both FirstStart tests |
| M32 credit applied with no live lease | `…with_no_live_branch_does_not_move_the_offset` |
| M33 migration uses the NEW file's mtime, not the source's | the migration test (the credit ≈ 0 and the lease is expired) |
| M34 `inward_at_zero` accepts a non-zero offset | `the_production_inward_door_refuses…` |
| M24b `set_root` writes `StoredRecord::inward_at_zero(raw_decoded, self.offset())?` | the `set_root`/`set_state` pin: `set_root` REFUSES at `D = 3000` |
| M24c `set_root` does `rec.set_deadline(self.to_stored(raw_decoded.lease_deadline))` | the `set_root`/`set_state` pin: reads 1500 where 4500 is due |

M24c compiles; it is a registered compile-survivor, killed by the pin.

### Counts, per-target

- **base + 36**: +4 at `b5bf3a3`, +1 at `802d379`, +1 for the inward-door test.
- `f1_lease_grace` → **20**.
- The lib filter
  `cargo test --lib -- f1_lease_grace lease_thread::tests::f1_ cluster::tests::f2_ the_alive_key_is_its_own_group an_over_long_lease_from_now`
  → **30** (20 + 5 + 3 + 1 + 1).

## Amendment 10 — review 3 (`frontier/d198_review3.md` @ `38ee1f7`) and SCALE-DESIGN "D198 addendum 2". Written before their fixes.

Nothing here has been run (quiet mode). Every prediction is from reading source.

### Committed so far, with predictions

Tests only, compiling against `1ec2deb`'s API. All in `table_catalog::f1_lease_grace`.

| commit | test | at that commit | after the fixes | kills |
|---|---|---|---|---|
| `f8d8225` | `a_first_start_credits_the_files_wall_clock_age_even_when_the_lease_clock_lags_it` (C1) | **FAIL**: credit `now − mtime` = 1 h, lease `t + 2h <= now = t + 3h` ("…short by the 7200000 ms the lease clock lags…") | PASS: credit is the wall age, 3 h + ms | M35 |
| `f8d8225` | `a_catalog_whose_offset_leaves_zero_is_refused_by_the_old_binary_magic_check` (C2) | **FAIL**: the magic on disk is still `0xFE44_0B01` | PASS | M42 |
| `f8d8225` | `a_catalog_that_never_leaves_offset_zero_keeps_the_magic_main_opens` (C2 control) | PASS | PASS | M38 |
| `f8d8225` | `an_offset_on_disk_beside_the_old_magic_is_switched_at_open` (C2, torn flush) | **FAIL**: magic unchanged after the reopen | PASS | M40 |
| `f8d8225` | `a_migration_whose_source_time_cannot_be_read_credits_nothing` (C5) | **FAIL**: `FirstStartFromFileTime` crediting ~0 ms from the new file's own mtime | PASS: `FirstStart` | M47 |
| `f8d8225` | `an_interrupted_switchover_credits_from_the_earlier_of_the_source_and_the_catalog` (C6, with a key) | **FAIL**: the log's 1 h is credited; the lease lapsed 1.5 h ago | PASS: recorded ~1 h + the catalog's age 2 h | M45 |
| `f8d8225` | `a_catalog_migrated_and_never_resumed_is_credited_from_the_retired_log` (C4) | **FAIL**: 5 min credited; the lease lapsed 30 min ago | PASS: recorded ~1 h + 5 min | M45, M46 |
| `f8d8225` | `the_first_start_evidence_survives_a_start_that_wrote_and_failed_before_resuming` (C4) | **FAIL**: ~0 ms credited from the failed start's own write | PASS: recorded ~1 h | M43, M45 |
| `ba77cbd` | `a_backward_wall_step_does_not_shrink_the_first_start_credit_below_the_lease_clocks` | PASS (`1ec2deb`'s credit IS the lease age) | PASS | M36 |
| `ba77cbd` | `a_crash_after_the_magic_switch_and_before_the_offset_leaves_a_catalog_this_build_opens` (C2) | **FAIL**: the open refuses `0xFE44_0B02` (`expect` panics) | PASS | M41 |
| `ba77cbd` | `the_new_magic_survives_a_root_split_after_the_offset_leaves_zero` (C2) | **FAIL**: the magic was never switched | PASS | M39 |
| `ba77cbd` | `the_downtime_before_an_unresumed_writer_is_kept_and_its_own_run_is_not_credited` (C4) | **FAIL**: credit `L(now) − (t + 2h)` saturates to 0; the lease reads `lapsed`, below `lo` | PASS: 4 h recorded + 1 h since, within `[lo, hi]` | M41b, M43, M44, M45 |
| `ba77cbd` | `a_switchover_with_no_recorded_evidence_credits_from_the_earlier_file` (C6, no key) | **FAIL**: the log's 1 h is credited | PASS: the catalog's 2 h | M48 |

With the fix, one test that cannot be red first (it names the new key constructor):
`tree_keys::tests::the_first_start_key_is_its_own_group_and_no_other_span_reaches_it`, a copy of the
`ALIVE` group test for tag `0x09`.

The new magic, pre-registered as a literal: **`0xFE44_0B02`**. `a_crash_after_the_magic_switch_…`
writes it by value; it is not read from the subject.

### The fixes that follow

**C1 — the wall age, never below the lease age.** The credit's time term is
`age(m) = max(W(now) − m, L(now) − m)`, both saturating at 0, where `W` is read through a new
`pub(crate) fn cluster::wall_millis_since(stamp) -> u64` (a duration, through `local_wall_millis`,
the one `SystemTime::now()` reader, so `wall_step` moves it) and `L(now)` is the `now_millis` the
resume is handed. The lead's rule is the first term. The second is added because the rule's premise
("the lease clock never advances faster than the wall clock") is false after a BACKWARD wall step
since this process anchored, which F2 exists to survive; then `W(now) − m` is short by the step and
`L(now) − m` is not. `max` is never below `1ec2deb`'s credit.

**C1 residual — a correction of the lead's inequality (INFERRED, derivation).** Write
`lag_p = W − L` for process `p`'s lease clock; it grows by host sleep. A lease is owed
`owed = L_new(now) − L_old(last_alive)`, because its deadline is on `L_old`. With
`m <= W(last_alive)`:

    W(now) − m − owed  >=  lag_new − lag_old
    L_new(now) − m − owed  =  (W(last_alive) − m) − lag_old

So the wall age over-credits iff `lag_new >= lag_old`, and otherwise falls short by at most
`lag_old − lag_new`; `1ec2deb`'s lease age falls short by up to `lag_old`. The lead's
`W(now) − W(last) >= L(now) − L(last)` holds on ONE process's lease clock. Across processes `L`
re-anchors to the wall clock at each start, jumping forward by `lag_old`, so the inequality fails by
`lag_old − lag_new`. Consequences:
- an upgrade from `main` has `lag_old = 0` (`main`'s lease clock is `SystemTime::now()`), so it
  over-credits only;
- **review 3's schedule E1 is NOT fixed by the wall age.** An F2-era unmarked writer (an embedder with
  no `LeaseThread`) whose host slept 2 h leaves a lease with 840 s on ITS lease clock; the file says
  nothing about its lag, the next start credits ~10 s, and the lease is reaped. No file time can
  carry `lag_old`. What would: a lease-clock stamp in `[0x09]` on every commit of an unmarked catalog
  (a soft mark). NOT TAKEN: it writes `[0x09]` into every catalog a D198 build creates, including the
  "pre-D198" catalogs `b5bf3a3`'s fixture builds with D198 code, making those two tests' stated
  premise ("No `[0x08]` record: every catalog written before D198 looks like this") false. Changing
  that fixture is a test edit: **⚖ for Ryan**, recorded, not done.

**C2 — the downgrade magic (lead, addendum 2).**
- `HEADER_PAGE_MAGIC` = `0xFE44_0B01` stays the magic of a catalog at `D = 0`.
  `HEADER_PAGE_MAGIC_OFFSET` = `0xFE44_0B02` marks a catalog that may hold `D > 0`.
- The catalog carries its current magic (`header_magic`), and `publish_root` writes it; it no longer
  writes the constant.
- `resume_leases`: when the record it is about to write has `D > 0` and the header still carries the
  old magic, it writes the header page with the new magic, stages, RELEASES `logical`, waits
  `durable`, then retakes the lock and recomputes. Only then does it write the record. One extra
  fsync, once per catalog lifetime. So the new magic is durable before the first record with
  `D > 0`, and therefore before any deadline stored against it.
- `open_from_header` accepts either magic and refuses anything else, with the same text. If the
  record already says `D > 0` beside the old magic, it switches and makes the switch durable before
  returning. That covers a catalog written by `1ec2deb` or earlier on this branch, or a damaged page.
- **Stated:** `main` refuses the new magic with "…is not a branch-catalog header (magic 0xfe440b02,
  expected 0xfe440b01)…". That is loud but misleading: the page IS a header, of a newer format. `main`
  cannot be changed.
- **Stated:** a headerless catalog (`create`/`open` over a caller's pool) has no page to carry a
  magic and no gate. Production opens only sidecars, through `default_for_database`
  (`cli.rs:91`, `examples/pgserver.rs:91`; READ).
- **Stated:** at `D = 0` a downgrade is still allowed. `main` ignores `[0x08]` and `[0x09]`, because
  every `range_scan` in `main:src/branch/table_catalog.rs` is bounded by a group span (READ, grep).

**C3.** Rewrite the narrow-C2 docs (`refuse_unresumed`, and the doc comment on
`a_catalog_with_a_last_alive_mark_refuses_expiry_questions_until_it_resumes`, a comment, not an
assertion). An unmarked catalog holding a live lease DOES have a measurable, uncredited outage before
its resume, and answers at `D = 0` until then.

**C4–C6 — the evidence, made durable, as an ACCRUAL.** A new key, `[0x09]` (`tree_keys::tag::FIRST_START`,
8 bytes, BE `u64` ms): **downtime recorded before a process that wrote this unmarked catalog and never
resumed it**.
- Written by `default_for_database`'s migration, into the tmp catalog before its flush and rename:
  the age of the source's mtime, if that is readable and the catalog holds a live lease.
- Written on the first commit of a process that opened an existing unmarked catalog, riding
  `stage` (no extra fsync): `recorded + age at open` of its file time, if it held a live lease at open;
  `0` if it held none and a record exists (the downtime was owed to nobody).
- Superseded, and no longer written, once `resume_leases` or `record_lease_alive` writes the mark.

Evidence at an open of an unmarked, non-fresh sidecar:
- An ordinary open: `(recorded or 0, own mtime)`.
- `has(cat)` with a legacy log beside it: with a key, `(recorded, own)`, because the key already
  measured the source. With no key, `(0, min(own, legacy))` (C6).
- The migrating process, whose file it wrote itself: `(0, source mtime)`, or NO evidence if the source
  mtime is unreadable (C5). Its own mtime is never evidence.

The credit is `recorded + age(since)`.

**Why accrue, not freeze.** A frozen stamp (the original mtime) would credit an unresumed writer's
whole run as downtime. An embedder that served for days would revive for days every lease it saw
expire. The accrual credits the gap before the writer and the gap after its last write, and not its
run.

**Why record at the migration, not read `.pre-table` later** (the lead's wording was "consult the
retired log's mtime"). `main` also migrates and retires to `{db}.branches.pre-table`
(`main:src/branch/table_catalog.rs:216`; READ). An unmarked catalog beside an old `.pre-table` may
have been served by `main` for months since. The retired log's mtime would credit all of it. The
migration is the only moment its mtime is known to be the last authority's.

Residuals, stated:
- A downgrade to `main` after an unresumed D198 start leaves a stale `[0x09]`. The next D198 start
  credits it on top of the time since `main`'s last write. That is an over-credit by the gap before
  the D198 start, the same class as a stale mark.
- A process that only reads writes nothing, so its uptime counts as downtime, as at `1ec2deb`.
- An embedder charges the outage before it at `D = 0` (C3), and a later resume credits that outage.
  A lease the embedder saw expire can come back by up to that outage.

**C7/C8.**
- The startup report's "The file time can only over-credit, never charge" is withdrawn (C7). The new
  text names the recorded part, the file time, and the two short-credit cases (a later writer, and a
  writer whose lease clock lagged).
- The stale docs are corrected: module `stored` (the door), `create`, `migrate_from`,
  `open_sidecar_at`'s mtime comment, `types.rs`'s `FirstStartFromFileTime`, and `AliveState::resume`.
- `LeaseResume::FirstStartFromFileTime` becomes
  `{ now_millis, file_mtime: Option<u64>, recorded_millis, credited_millis }`. No test matches its
  fields (grep).

### Corrections to earlier amendments

- **M24c reads 3000, not 1500** (review 3, C9). The raw-decoded `v = 1500` goes through `to_stored`:
  `1500 − 3000` saturates to 0, which reads `0 + 3000`.
- Amendment 9 (5): "an unreadable mtime gives a plain `FirstStart`" was false at `1ec2deb` on the
  migration branch (C5). It is true after this fix.
- Amendment 9 (5) inherited the lead's retracted "can only over-credit" argument. It is withdrawn, and
  replaced by the derivation above.
- **R7 persists** (review 3, C11). The C2 refusal tells a cluster member to start a `LeaseThread`,
  which on a member resumes nothing (`Clustered`). Recorded, not fixed, as in amendment 9 (8).

### Stated fragilities

- The two `b5bf3a3` FirstStart tests keep an exact lower bound, because of the `max`. Their upper bound
  (+1000 ms) now needs `W(resume) − L(now) <= 1 s`, that is, no host sleep of about a second since the
  test binary anchored its lease clock (INFERRED). They are not edited.
- The C5 test needs the filesystem to accept a pre-1970 mtime. If it does not, the test panics at
  `set_modified(..).unwrap()`: a fixture failure, not a verdict.
- The C1 and accrual tests place wall times with `wall_step`, which is thread-local, and with
  `age_file`. A fix that read `SystemTime::now()` directly, bypassing `local_wall_millis`, would see the
  real clock and fail both. That is the one-reader rule, enforced by these two tests.

### New mutants

| mutant | must fail |
|---|---|
| M35 credit's time term is `now − m` only (`1ec2deb`) | C1 wall-lag test |
| M36 credit's time term is `W(now) − m` only (no `max`) | backward-step test |
| M37 magic switch in the SAME durable as the record | **SURVIVES** every test (needs a crash inside `durable`). The torn state it allows is repaired at open (M40's test), and `main` reads it correctly, since no deadline has been stored at `D > 0`. Registered survivor. |
| M38 switch the magic even at `D = 0` | `…never_leaves_offset_zero_keeps_the_magic_main_opens` |
| M39 `publish_root` writes the old constant | root-split test |
| M40 no open-time switch | torn-flush test |
| M41 `open_from_header` refuses the new magic | crash-after-switch test; the reopen in the downgrade, root-split and D198 reopen tests |
| M41b the credit reads `SystemTime::now()` directly | C1 and accrual tests |
| M42 no resume-time switch (open-time only) | downgrade test (it asserts on disk after the drop, before any reopen) |
| M43 the `stage` rider removed (the key is never written by an unresumed writer) | C4 failed-start test, accrual test |
| M44 frozen evidence (the key holds the original mtime, not an accrual) | accrual test: credit ≈ 7 h > `hi` |
| M45 the key ignored at open | C4 failed-start, C4 migrated, accrual, and C6 with-key tests |
| M46 the migration writes no key | C4 migrated test |
| M47 the migration branch falls back to the own mtime (`1ec2deb`'s `.or`) | C5 test |
| M48 a switchover with no key prefers the legacy log (`1ec2deb`) | C6 no-key test |
| M49 a switchover WITH a key adds the legacy's age too (double counting) | **SURVIVES**: an over-credit, and the C6 with-key test asserts survival only. Registered. |

### Counts, per-target

- `f1_lease_grace` → **33** (+8 at `f8d8225`, +5 at `ba77cbd`). `tree_keys::tests` → +1 with the fix.
- **base + 50** (amendment 9's +36, then +13, then +1).
- The lib filter, extended by one name:
  `cargo test --lib -- f1_lease_grace lease_thread::tests::f1_ cluster::tests::f2_ the_alive_key_is_its_own_group an_over_long_lease_from_now the_first_start_key_is_its_own_group`
  → **44** (33 + 5 + 3 + 1 + 1 + 1).

## Amendment 11 — the soft mark (lead's decision after review 3: "the soft mark is yours to take"). Written before its fix.

The lead ruled that `b5bf3a3` is this lane's own unlanded test, not a test on `main`, so the ⚖7 of
amendment 10 is the lead's decision, not Ryan's. The decision: write the soft mark, then rebuild the
fixtures honestly. Nothing here has been run (quiet mode).

### Committed so far, with predictions

| commit | test | at that commit (`bf15efb` source) | after the fix | kills |
|---|---|---|---|---|
| `960cc02` | `f1_lease_grace::an_unmarked_writer_whose_lease_clock_lagged_keeps_its_leases_across_the_next_start` (review 3's E1) | **FAIL**: a fresh catalog records no `[0x09]`, so the credit is the file's 10 s and a lease with 840 s left is expired ("…blind to the 7200000 ms that clock lagged…") | PASS: credit `now − s ≈ S + 10 s`, the lease keeps `840 s − (s − t)` | M51, M52 |
| `960cc02` | `f1_lease_grace::the_soft_mark_is_the_last_commit_of_an_unresumed_writer_not_its_first` | **FAIL**: credit 0 (the file's time is in the future), below `now − after_last >= 50` | PASS: credit `now − s_last`, within the bounds by program order | M50, M52 |

### The fix that follows

**The soft mark.** `[0x09]` becomes 16 bytes, `(mark, accrued)`, both BE `u64`:
- `mark` is the writer's LEASE-clock reading at its commit.
- `accrued` is the downtime owed before that writer opened the catalog.

**Who writes it.** Every commit of an unmarked catalog writes it, riding `stage` with no extra fsync.
It is not written:
- once the catalog is marked;
- once this process's resume or heartbeat has superseded it;
- on a cluster member;
- when the lease clock cannot be read.

The migration writes `(L(now), age(source))` into the tmp catalog before the rename, as before. The
first-commit-only rider of `bf15efb` is gone.

**The credit.** For an unmarked catalog, from the evidence taken at open:

    credit(now) = accrued + max( now − mark , file_age(file time, now) )

- `now − mark` is 0 without a soft mark. It is the mark's own semantics: cross-process lease-clock
  subtraction, which is exactly what a lease on the writer's clock is owed.
- `file_age` is amendment 10's `max(W − m, L − m)`.
- The file time is:
  - own mtime (`Own`);
  - with a soft mark present, own mtime; without one, `min(own, legacy)` (`OwnOrLegacy`);
  - the source mtime (`Source`).
- The migrating process ignores its own soft mark. It uses `(no soft, source)`, as `bf15efb` did.
- **Why the file term stays in the `max`.** Every soft-mark write is a file write, so the file's age is
  at most the wall time since the last soft mark. It can exceed the soft credit only by
  `lag_new − lag_old`: an over-credit, in the accepted direction. And it keeps a pre-D198 catalog,
  which has no soft mark, on amendment 10's rule.
- **What this process's soft marks carry.** Their `accrued` is the credit evaluated at open, if the
  catalog held a live lease then, and 0 otherwise. So the writer's own run is never credited.

**E1 is closed** for every D198 writer that commits. The residual is the over-credit between its last
commit and its death, the accepted direction, the same as a mark's.

**`LeaseResume::FirstStartFromFileTime`** gains `writer_mark: Option<u64>`, and the report names it.

**Cost (UNMEASURED).** Every commit of an unmarked catalog pays one extra upsert and one lease-clock
read. A marked catalog pays nothing, and the binaries mark at their first start. `[0x09]` sorts after
`[0x07]` (`HEADER`), which every fork rewrites, so it usually shares that leaf.

### The fixtures rebuilt (a test change; why it is not a weakened assertion)

With the soft mark, every catalog a D198 build creates carries `[0x09]`. Six of this lane's own
unlanded tests use a D198-written catalog to stand for one that no D198 build wrote:
- `b5bf3a3` `a_pre_d198_catalog_credits_the_downtime_since_its_file_was_last_written` and
  `a_pre_d198_catalog_with_no_live_branch_does_not_move_the_offset`;
- `f8d8225` `a_first_start_credits_the_files_wall_clock_age_even_when_the_lease_clock_lags_it` and
  `the_first_start_evidence_survives_a_start_that_wrote_and_failed_before_resuming` (review 3's C4
  scenario is a pre-D198 catalog followed by a failed D198 start);
- `ba77cbd` `a_switchover_with_no_recorded_evidence_credits_from_the_earlier_file` and
  `the_downtime_before_an_unresumed_writer_is_kept_and_its_own_run_is_not_credited` (its "last
  authority before the writer" is placed on the wall clock, which a D198 writer's soft mark would
  contradict).

**The change.** Each fixture's writer ends with a new `#[cfg(test)]` helper,
`TableBranchCatalog::as_written_before_d198`. It removes `[0x08]` and `[0x09]` and makes that durable
WITHOUT `stage`, so no soft mark is rewritten. After the reopen, each test ASSERTS the absence of
both keys as a premise, before anything else (new premise assertions; additions).

**Why it is not weakened.**
- Every existing assertion, and every expected value, is byte-identical.
- The rebuild REMOVES state that made the fixture not what it names. It narrows what can pass: the
  premise assertion fails on any fixture that still carries a D198 key.
- Predicted: five of the six would PASS without the rebuild, through the `max` (the file term
  dominates the soft mark's milliseconds). So the rebuild is not what turns them green.
- The sixth, the C6 no-key test, would pass VACUOUSLY: with the fixture's soft mark present, the
  legacy log is ignored, and M48 would survive it.
- The soft-mark path is covered by the two new tests above, not by these.

Unchanged, because their fixtures are honestly D198-written:
- the backward-step pin;
- the C4 migrated test and the C6 with-key test, which are D198 migrations. C6 with a key passes
  through the own-mtime term of the `max`, since its mtime is aged 2 h;
- the `D = 0` magic control and the crash-after-switch test.

### New mutants

| mutant | must fail |
|---|---|
| M50 the soft mark written only on a process's FIRST commit (`bf15efb`'s rider) | last-commit test (credit ≥ `now − before_last + 200`) |
| M51 the soft mark ignored at open (file time only) | E1 test |
| M52 no soft mark written by `stage` | E1 test, last-commit test |
| M53 the soft mark's `accrued` written as 0 | C4 failed-start test (rebuilt), accrual test |
| M54 the soft mark written on a MARKED catalog | `a_marked_catalog_writes_no_soft_mark` (added with the fix, green-only: it reads the new `soft_mark()`) |
| M55 the heartbeat does not supersede the soft mark | the same test (the first `record_lease_alive` would write one) |
| M56 `as_written_before_d198` commits through `stage` (the strip would rewrite a soft mark) | the premise assertions of the six rebuilt tests |
| M57 `OwnOrLegacy` with a soft mark also takes the legacy's age (double counting) | **SURVIVES** (an over-credit; C6 with a key asserts survival only). Registered, replacing M49. |

Amendment 10's M43/M44 are superseded by M50–M53: the rider they mutate no longer exists in that form.

### Counts, per-target

- `f1_lease_grace` → **36** (+2 at `960cc02`, +1 `a_marked_catalog_writes_no_soft_mark` with the fix).
- **base + 53**.
- The lib filter of amendment 10 → **47** (36 + 5 + 3 + 1 + 1 + 1).

## Amendment 12 — review 4 (`frontier/lease_review4.md` @ `64b519b`) and the lead's decisions on it. Written before their fixes.

Nothing here has been run (quiet mode). `012f65c` is cited, so everything is appended. `L_p` is process
`p`'s lease clock, `W` the wall clock, `m` an OS mtime (wall), `lag_p = W − L_p`.

### Committed so far, with predictions

| commit | test | at that commit (`012f65c` source) | after the fixes | kills |
|---|---|---|---|---|
| `78ed41c` | `f1_lease_grace::an_unresumed_writer_whose_lease_clock_lagged_at_open_carries_no_lag_into_the_next_credit` (C1) | **FAIL**: `accrued = max(W − m, L − m) = 1 h + S` at the writer's open, so credited ≈ `now − m + S` > `now − m + 2000` | PASS: `accrued = L_w(open) − m`, credited ≈ `now − m` | M59 |
| `1d64b36` | `f1_lease_grace::a_migration_by_a_process_whose_lease_clock_lagged_carries_no_lag_into_the_next_credit` (C1, the migration) | **FAIL**: the migration's `accrued` is the wall age, `1 h + S` | PASS: `accrued = L_mig − m` | M60 |
| `1d64b36` | `f1_lease_grace::the_pre_d198_fixture_helper_refuses_a_catalog_whose_offset_left_zero` (C7a, `#[should_panic]`) | **FAIL**: the helper strips a `D = 3000` catalog without complaint | PASS: it panics with "only a catalog still at D = 0" | M61 |

### The fixes that follow

**C1 — one clock per expression.** `first_start_owed`, the `accrued` that this process's soft marks
carry, is computed from LEASE-clock terms only:

    owed_at_open = accrued_prev + max( L_w(open) − s_prev.mark , L_w(open) − m )

- The migration's `accrued` is `L_mig − m_src`.
- The resume keeps the lead's review-3 wall half in its file term:
  `credit(now) = accrued + max(now − s.mark, W(now) − m, now − m)`.
- There, the wall half over-credits by the resumer's own lag, and only there.

**The relations the bound relies on** (these go into the doc beside the formula):
- **R1.** `L_p(x) = W(x) − lag_p(x)`, where `lag_p ≥ 0` never decreases within `p`. The exception is
  a BACKWARD wall step after `p`'s anchor, for which the file term takes the lease half.
- **R2.** A pre-D198 writer, or a legacy log's writer, has `lag = 0`, because its lease clock IS the
  wall clock (READ `main:src/cluster/mod.rs:335`).
- **R3.** `m ≤ W(last moment its writer was alive)`. Every soft mark is written in a commit, so the
  file is never older, on the wall, than its last soft mark.
- **R4.** `s.mark ≤ L_w(last moment w was alive)`, because a soft mark is stamped at a commit.

**The derivation** (INFERRED):
- A lease written on writer `w`'s clock is owed `L_new(now) − L_w(last_alive)`.
  - By R4 that is ≤ `L_new(now) − s.mark`: an over-credit by the idle tail only.
  - For a pre-D198 predecessor, it is ≤ `L_new(now) − m`, by R2 and R3.
- With a soft mark present, the lease-scale file term `L − m` cannot exceed the soft term. By R3 and
  R1, `m ≥ W(s's commit) ≥ s.mark`. So at an intermediate writer's open the max IS the soft term:
  exact, with no lag counted twice.
- The chain is lease-minus-lease at every link. The only wall term left is at the final resume.
- `012f65c`'s `max(W − m, L − m)` at the writer's open added `lag_w(open)` to a quantity the next
  start ALSO reads on `w`'s clock. That is review 4's double count.

**C2.**
- **The E1 test is changed (a test edit on this lane's own unlanded test, per the lead).**
  - `an_unmarked_writer_whose_lease_clock_lagged_keeps_its_leases_across_the_next_start` now holds
    `wall_step::by(S)` across the writer's open and fork, as the C1 test does.
  - Its assertions and expected values are unchanged.
  - It can now tell a lease-clock soft mark from a wall-clock one: stamped with the wall, the mark
    reads `≈ t + S` and the credit is ≈ 10 s.
- **M58** stamps the soft mark with the wall: at `012f65c`'s API, `mark: crate::cluster::wall_millis_since(0)`.
  - The changed E1 test kills it.
  - After C3 that spelling does not compile, because no function returns a wall reading.
  - An inline `SystemTime::now()` is M41b's blind spot, since `wall_step` cannot step it.

**C3 — a wall reading made unrepresentable.** `cluster::wall_millis_since(u64)` is replaced by
`cluster::FileWallStamp`:
- `FileWallStamp::of(&Path) -> Option<FileWallStamp>` reads that file's mtime, and it is the ONLY
  constructor.
- `millis()` gives the stamp.
- `wall_age_millis()` gives `W(now) − stamp`, saturating.

A plain `fn(&Path) -> age` was not used: the evidence is the file's time AT OPEN, and its age is
needed at the resume, after this process may have written the file.

Stated hole: the age of a file whose mtime is the epoch IS a wall reading. Getting one means
creating such a file, so it is not reachable by accident.

Callers change as follows:
- `open_sidecar_at` (own stamp);
- `default_for_database` (legacy and source stamps);
- the evidence's resume-time wall half;
- `file_mtime_millis` becomes `#[cfg(test)]` (the C5 test's premise).

**Cost, second acquisition removed.** `record_soft_mark`, the open-time `owed`, and the migration read
`cluster::standalone_lease_millis() -> Option<u64>`:
- one acquisition of the process lock gives authority and reading together;
- it closes review 4's `is_clustered`/`try_now_millis` TOCTOU.

Per unmarked commit that leaves one lock acquisition, one `Instant` read and one upsert.

**C4 — over-credit is UNBOUNDED, and that is stated, not fixed** (the safe direction by design: Chubby
grace). Nothing in the policy caps any route. Each is bounded only by a physical interval:

| route | bounded by |
|---|---|
| a writer's idle tail after its last commit | that tail |
| the resumer's own lag, through the wall half at the resume | the resumer's host sleep since its anchor |
| leases GRANTED by an unresumed writer receive the whole `accrued` outage from before that writer existed | the pre-writer outage (can be months) |
| a stale soft mark after D198 → `main` → D198 | **`main`'s whole service period**: `now − stale_mark` spans it |
| a read-only opener's uptime | its run |

**Corrections to amendment 10.**
- Its "a stale `[0x09]` … over-credit by the gap before the D198 start" was true of `bf15efb`'s 8-byte
  record. It is STALE under the soft mark: the over-credit is `main`'s whole service period, as in the
  table.
- **M37 is not a double count and not an over-credit route.** It merges the magic switch and the
  record into one durable. Its torn state (`D > 0`, old magic) comes before any deadline is stored at
  that `D`, `main` reads it correctly, and the next D198 open switches it. So it is benign.
- **M57 is a mutant, not the code's behaviour.** `OwnOrLegacy` with a soft mark takes own only.
- **The one double count the code made was C1**, fixed here.

**C5.**
- **(a) Torn first commit, STATED residual.** Eviction can write a commit's pages before its
  `durable`, and the catalog tree has no WAL. So after a SIGKILL or a power loss, a FIRST commit over
  a catalog with no soft mark can leave the file time moved and no `[0x09]`. The pre-writer outage is
  then lost. That is never below `D = 0`, and the commit was never acknowledged.
- **(b) Fixed structurally.** `soft_mark_superseded` is REMOVED.
  - The mark's own commit, in `resume_leases` and `record_lease_alive`, goes through a new
    `stage_mark`, which writes no soft mark.
  - `marked` is set only after that commit's `durable`.
  - So a failed mark write leaves the catalog soft-marking, and there is no flag to forget to clear.
- **(c)** A cluster member, or no lease clock: stated, as before.
- **(d)** The migration fsyncs the tmp file before the rename (`disk_manager.sync()`). It fsyncs the
  directory after the two renames through the validated `storage::atomic_file::OsFileOps::sync_dir`
  and `parent_dir`, whose Windows no-op is stated there. This was pre-existing at `9aa6968`. Nothing
  can test it without a power cut, so **M63** (the directory fsync removed) is a registered survivor.

**C6 — the cost, to be MEASURED (FAN-QUEUE step, pre-registered here).** A new harness,
`examples/d198_soft_mark_cost.rs`, has two arms over fresh sidecars:
- `unmarked` writes a soft mark on every commit;
- `marked` is marked before the timed loop by `record_lease_alive` + `resume_leases` at the same
  reading, so `D = 0`, and writes none.

It is the same code path minus the soft mark, with no switch that disables anything.
- **Two commit kinds:** `fork_staged`, never awaited, isolates the serial section under `logical`;
  durable `set_root` includes the fsync.
- **Order:** arms interleaved `ABBA` over R rounds.
- **Output:** per arm and kind, the per-commit median and p90, and the INTEGER
  `key_rewrites` per commit.

**Predictions:**
- **I (integer, fixed by control flow):** `key_rewrites` per commit, unmarked minus marked, is
  **exactly 1**, in both kinds.
- **R-staged:** the ratio of median staged-fork time, unmarked over marked, is in **[1.00, 1.40]**.
- **R-durable:** the same ratio for durable `set_root` is in **[0.95, 1.10]**.

**Falsifiers:**
- I ≠ 1 means the arms do not differ by exactly the soft mark: a harness defect, and no ratio is
  quoted.
- A ratio outside its interval is reported as the finding, not re-run until it fits.

**Baselines (stated in the FAN-QUEUE row).** 24 of the 27 example files that open a catalog never
resume (review 4, MEASURED by grep), so they pay the soft mark on every commit. Bench numbers for
those examples from before `8d0606e` are NOT comparable to runs after it.

**C7 — fixtures.**
- **(a)** `as_written_before_d198` asserts `D = 0` and the old magic before it strips. Otherwise it
  panics with "only a catalog still at D = 0". It is forced to fire by `1d64b36`'s test.
- **(b)** A comment only: review 3's C1 test pins arithmetic (it kills M35). Its lagging-writer story
  is not a schedule a pre-D198 writer can reach, and the comment now says so. No code in it changes.
- **(c) The accrual test is re-staged with consistent clocks** (a test edit on this lane's own
  unlanded test, per the lead).
  - **Why.** Its old lower bound `lapsed + 4h + (w0 − (t + 2h))` demanded 5 h, while its resumer's
    lease clock read `t` against a wall of `t + 3h`. The fifth hour was that 3 h resumer lag, the
    over-credit the design names, so the test required a defect-shaped term.
  - **New staging:**
    1. The pre-D198 authority wrote at `m_prev = t − 4h` (the fixture is stripped).
    2. Program-order readings: `t0` before the writer's reopen, then the writer opens and commits
       `set_root`, then `t1`.
    3. The next start resumes at the lease reading `now = t1 + 1h`, with `wall_step::by(1h)`, so its
       wall clock and lease clock agree (resumer lag ≈ 0).
  - **The owed credit**, lease terms only: `(L_w(open) − m_prev) + max(now − s, file term)`, with
    `L_w(open)` and `s` both in `[t0, t1]`.
  - **New bounds, on the lease:**
    - `lo = lapsed + (t0 − m_prev) + 1h`: `L_w(open) ≥ t0`, and `now − s ≥ 1h`.
    - `hi = lapsed + (t1 − m_prev) + 1h + (t_after − t0) + 2000`: `t_after` is read after the
      resume, and 2000 ms covers wall/lease skew and mtime granularity.
  - What it still pins: the four hours before the writer are carried (M53: `accrued` 0 → ≈ 1 h,
    below `lo`). The writer's own run is excluded by the last-commit test, not here.

**The queued doc fix.** `holds_a_live_lease` is NOT one descent. `RangeScanner::next` follows
`leaf.next` through every empty leaf (READ `src/storage/range_scan.rs:58–95`). The adversary
`deadline-leaf-adversary` is testing that `delete` never frees or merges a leaf. So the cost is one
descent plus every empty leaf at the head of the DEADLINE group. It has three call sites:
- `take_first_start_evidence`, at every open of an existing unmarked sidecar;
- the migration;
- `resume_leases`.

Nothing new is registered for it.

### New mutants

| mutant | must fail |
|---|---|
| M58 the soft mark stamped with the wall clock (`012f65c`'s API: `wall_millis_since(0)`) | the E1 test, changed as above. After C3: fails to compile |
| M59 `first_start_owed` computed with the wall half | `78ed41c`'s C1 test |
| M60 the migration's `accrued` taken as the wall age | `1d64b36`'s migration test |
| M61 `as_written_before_d198` without its `D = 0` guard | `1d64b36`'s `should_panic` test |
| M62 `stage_mark` replaced by `stage` in the mark's own commit (the old M55's route) | `a_marked_catalog_writes_no_soft_mark` |
| M63 the migration's directory fsync removed | **SURVIVES** (needs a power cut). Registered. |

### Counts, per-target

- `f1_lease_grace` → **39** (+1 at `78ed41c`, +2 at `1d64b36`).
- **base + 56**.
- The lib filter of amendment 10 → **50** (39 + 5 + 3 + 1 + 1 + 1).

## Amendment 13 — review 5 (`frontier/lease_review5.md` @ `adcce16`): the head did not compile. Written before the fix.

Nothing here has been run (quiet mode).

### B1, and what it says about amendments 11–12

`fdd2d23` changed `FirstStartEvidence::file` to `Option<FileWallStamp>` and left
`first_start_credit`'s `file_mtime: e.file` feeding `FirstStartCredit::file_mtime: Option<u64>`. That
is E0308 at `fdd2d23`, `a678a61` and `cbc51df`.

**Every GREEN prediction of amendment 12 (§7 7e, 7f, 8, 9, M58–M62) was void from the moment it was
written.** The lane's own row says a compile error on the clean tree fails the pre-registration, and
it does.

The fix below is `file_mtime: e.file.map(FileWallStamp::millis)`. Then comes a hunk-by-hunk,
field-by-field re-check of `git diff 012f65c` against the struct and function definitions. The record
of that check is appended below the fix, as an amendment-13 addendum.

### Committed so far, with predictions

| commit | test | at that commit | after the fix | kills |
|---|---|---|---|---|
| `1dad3a6` | `f1_lease_grace::a_lagging_writer_over_an_evidence_bearing_catalog_keeps_its_leases_across_the_next_start` | does not compile (B1) | PASS: soft mark on `L_w ≈ t`, credit `1 h + S + 10 s`, the lease keeps `LEFT + 1 h − ε` | M58b |
| `1dad3a6` | `f1_lease_grace::an_unresumed_writers_accrued_downtime_is_fixed_at_its_open_not_at_its_commit` | does not compile (B1) | PASS: credited in `[(a0 − m) + (now − b1), (a1 − m) + (now − b0)]` | M64 |

Both are mutant killers, not reds: the code already behaves as they assert.

### The fixes that follow

**B1.** `file_mtime: e.file.map(FileWallStamp::millis)`.

**C3, the raw stamp.**
- `FileWallStamp::millis()` stays crate-private, with exactly TWO consumers, both named in its doc:
  - `first_start_credit`, for the startup report's `file_mtime`;
  - the `#[cfg(test)]` `file_mtime_millis`, which is the C5 test's premise.
- Every arithmetic consumer goes through a new `lease_scale_age_millis(lease_now)`, whose doc carries
  R2, or through `wall_age_millis()`. That covers `owed_at_open`, `resume_credit`'s lease half, and the
  migration.
- The claim is scoped to "no single call returns a wall reading". `s.millis() + s.wall_age_millis()`
  IS one, for any stamp. So the guard is a TEST, not the type:
  - **M58b** is the re-spelled M58, stamping the soft mark with the wall clock through a stamp the
    catalog holds;
  - `1dad3a6`'s first test kills it.

**C1a, the wording.** "Lease terms only" (`FirstStartEvidence`'s doc, `owed_at_open`'s, the migration's
comment) becomes "lease-scale terms: the mtime is the lease reading of a writer whose lease clock was
the wall clock (R2)".

**C1b, a stated residual.** R2 is scoped to a writer whose lease clock was the wall clock: every
pre-D198 build, and so every `.branches` log a pre-D198 build wrote. A `.branches` log that a D198
build wrote (`LogBranchCatalog::open`, the documented embedder path) breaks R2.
- The migration's `L_mig − m`, and the migrating resume's wall half, are then short by that writer's
  lag at its last append. That is E1, for a writer that never soft-marks. It is never below `D = 0`.
- **No soft mark is added to `LogBranchCatalog`.** The reasons:
  1. `LogBranchCatalog` has no restart grace at all (`resume_leases` is the trait's `NoMark`). Every
     restart of a log-backed database already charges its whole outage, so the migration moment is
     the least of its under-credits.
  2. Its log format is length-prefixed `BranchRecord`s, and `main`'s `replay` refuses any other entry
     (READ `main:src/branch/catalog.rs:263–284`). An in-log mark would make every D198-written log
     unopenable by `main`.
  3. A sidecar mark file costs a second write per append, and cannot be emulated in-process: the OS
     stamps the log with the real wall clock, not `wall_step`'s. A red test would therefore need the
     sidecar itself.
- **No red test is added for C1b.** There is no fix for it to turn green. A pin that asserts the
  under-credit would enshrine the defect, so the residual is stated instead. This departs from the
  lead's "add a red test", and is reported as such.

**C1c.** R1 is corrected. `lag` is not non-decreasing on macOS: `Instant` is `CLOCK_UPTIME_RAW`, which
drifts against the NTP-disciplined wall by the oscillator's error, in either direction, with no step.
R1 is used only for `m ≥ s.mark`, so its failure over-credits only: the file half exceeds the soft
half by `|lag|`.

**C1d, stated.** A torn first commit (C5(a)) also loses up to the torn writer's own lag: the file
half is `L − m`, with `m` stamped by the wall while that writer's lease clock lagged. It is never
below `D = 0`.

**C1e, stated.** Open as a cluster member, then `leave()`: `standalone_lease_millis()` is `None` at
the open, so `first_start_owed = 0`, and the soft marks written after `leave()` carry nothing accrued.
- The pre-open outage is dropped. That is under the policy, never below `D = 0`.
- It is a change from `012f65c`, which computed the wall age there. It is embedder-only: members use
  `LogBranchCatalog`.

**C4, the over-credit routes, completed.** Amendment 12's "each is bounded only by a physical
interval" was false for route 1 below. All are over-credits:

| route | bounded by |
|---|---|
| a writer's idle tail after its last commit | that tail |
| the resumer's own lag (the wall half at the resume) | its host sleep since its anchor |
| leases GRANTED by an unresumed writer receive the whole `accrued` outage | the pre-writer outage |
| a stale soft mark after D198 → `main` → D198, or through a D198 cluster-member interlude (members write none) | that whole interlude |
| a read-only opener's uptime | its run |
| **a forward wall step, or a wrong wall clock, between processes or after the resumer's anchor** | **the step: not a physical interval (an RTC reset forward by a year credits a year)** |
| a resumed process that never heartbeats | its uptime |
| **an unresumed writer that later resumes ITSELF**: `resume_leases` credits from the evidence taken at its OPEN, so its own run between open and resume is credited, and leases it refused as expired at `D = 0` come back | that run |
| an embedder continuing after a failed mark `durable`: `[0x08]` stays in the tree, and a later commit persists it | the interval to that later commit |
| an mtime rewound by a restore that preserves mtimes | the rewind |

**Why the stale soft mark cannot be bounded.** `[0x09]` carries no age. A bound built from `m − s.mark`
would reopen E1: a writer whose lease clock lagged by `S` legitimately leaves `m ≈ s.mark + S`, which
is indistinguishable from a stale mark followed by a later writer that left no soft mark. The only
limiter is addendum 2's gate: the route exists only while `D = 0`.

**The doc sentence "nothing credits an unresumed writer's own run"** becomes "nothing credits an
unresumed writer's own run to the NEXT process". A writer that later resumes itself credits its own
run, as in the table.

The `SoftMark` doc's "once `[0x08]` exists this key is never written or read again" becomes "never
READ once `[0x08]` is on disk". A commit racing the mark's own `durable` may still write one, and the
next open ignores it (review 5 Q4).

**C5.** The migration syncs the directory after EACH rename, not once after both. One sync after
both does not order them: a filesystem that persisted the retirement and not the publish would leave
neither file, and the next open would create a fresh empty catalog (pre-existing at `9aa6968`). That
is one more fsync, once in a catalog's life. M63 (either sync removed) remains a registered survivor.

**C6, the ratio definition.** Amendment 12 said "the ratio of median staged-fork time". The harness
computed the upper median over rounds of the per-round ratio `median(unmarked)/median(marked)`. Both
are made to say the same thing:
- **the statistic is the median over rounds of the per-round ratio of medians.** Pairing within a
  round cancels drift between rounds.
- The harness's even-count median is changed from the upper middle element to the mean of the two
  middle elements.
- The predicted intervals are unchanged.

**T.** The accrual test's name ends `…and_its_own_run_is_not_credited`. Its doc now hands that clause,
scoped to the NEXT process, to two tests:
- `the_soft_mark_is_the_last_commit_…`: where the mark sits (M50);
- `1dad3a6`'s `…accrued_downtime_is_fixed_at_its_open_not_at_its_commit`: what it carries (M64).

The name is kept: renaming it would break every earlier amendment's reference.

### New mutants

| mutant | must fail |
|---|---|
| M58b the soft mark stamped with the wall clock via a stamp the catalog holds: `mark: self.first_start.and_then(\|e\| e.file).map_or(now, \|f\| f.millis() + f.wall_age_millis())` | `a_lagging_writer_over_an_evidence_bearing_catalog_…` (the lease expires) |
| M64 `accrued` recomputed at each commit from the open's evidence (`e.owed_at_open(now)` in `record_soft_mark`) | `…accrued_downtime_is_fixed_at_its_open_not_at_its_commit` (credit ≥ `hi + 1000 − commit time`) |
| M65 one directory sync after both renames (`fdd2d23`'s) | **SURVIVES** (a power cut, and a reordering filesystem). Registered, with M63. |

### Counts, per-target

- `f1_lease_grace` → **41** (+2 at `1dad3a6`).
- **base + 58**.
- The lib filter → **52** (41 + 5 + 3 + 1 + 1 + 1).

### Amendment 13, addendum: the type and visibility pass (review 5, B1), recorded after `d6d5fb5` and `b561b48`

This is a READ pass, not a build, over `git diff 012f65c..b561b48 -- src examples`. Each added
production line was checked against the definition it uses. The same pass was also run over the
test code added since `012f65c`.

**What was checked:**
- **Changed struct and variant definitions, and every constructor and consumer of each** (found with
  `grep -rn --include='*.rs' -F` over `src tests examples`):
  - `LeaseResume::FirstStartFromFileTime` (5 fields): 1 constructor (`AliveState::resume`), which
    names all 5 with matching types, and 1 matcher (`lease_thread.rs:939`), which binds all 5. No
    test names it.
  - `stored::FirstStartCredit` (4 fields): 1 constructor (`first_start_credit`). `file_mtime` is
    `e.file.map(FileWallStamp::millis)`, which is `Option<u64>`: this was B1. `writer_mark` is
    `Option<u64>`. `recorded_millis` and `credited_millis` are `u64`.
  - `SoftMark` (2 fields): 3 constructors (`decode`, the migration, `record_soft_mark`), all `u64`.
  - `FirstStartEvidence { soft: Option<SoftMark>, file: Option<FileWallStamp> }`: 1 constructor.
    `FileTimes`' three variants all carry `Option<FileWallStamp>`, fed from `FileWallStamp::of` (3
    sites).
  - `TableBranchCatalog`'s two struct literals (`create`, `open`) list `first_start`,
    `first_start_owed` and `header_magic`, and neither lists a removed field. `file_mtime`,
    `first_start_record`, `first_start_written` and `soft_mark_superseded` have 0 hits in
    `src tests examples`.
  - `AliveState::resume` has 1 caller and `open_sidecar_at` 3, all with the current signatures.
- **Visibility:**
  - `cluster` is `pub mod` (`lib.rs:23`) and `FileWallStamp`/`standalone_lease_millis` are
    `pub(crate)`.
  - `FileWallStamp`'s field is private, and is constructed only inside `cluster`.
  - `storage::atomic_file` is `pub mod`, with `pub trait FileOps`, `pub struct OsFileOps` and
    `pub fn parent_dir`.
  - `BufferPoolManager::disk_manager` is `pub` (`buffer_pool.rs:391`).
  - `stored::LeaseOffset::is_zero` and `FirstStartCredit`'s fields are `pub(super)`.
- **The example:**
  - `LeaseDeadline(pub u64)` (`types.rs:138`) and `BranchId::TRUNK` are public.
  - `branch::table_catalog` and `branch::types` are `pub mod`.
  - `fork_staged`, `set_root`, `resume_leases` and `record_lease_alive` are `BranchCatalog` trait
    methods (`branch/mod.rs:80`, `:228`, `:448`, `:464`), imported with the trait.
  - `key_rewrites` is `pub fn`.
- **Copy and moves:**
  - `Option<SoftMark>` and `Option<FileWallStamp>` are `Copy` (both derive `Copy`), so `self.soft` /
    `self.file` behind `&self` and the reuse of `source_mtime` after the `if let` tuple are legal.
  - `Authority` is `Copy`, since `authority()` already returns `lock().authority`.
- **CI's `-D dead_code`** (`.github/workflows/tests.yml:67`). Every new production item has a
  production use:
  - `FileWallStamp::{of, millis, lease_scale_age_millis, wall_age_millis}`;
  - `standalone_lease_millis`, `sync_dir_of`, `stage_mark`, `soft_mark`;
  - `SoftMark::{encode, decode}`, `FirstStartEvidence::{owed_at_open, resume_credit, accrued}`;
  - `FileTimes`' three variants, `LeaseOffset::is_zero`, `HEADER_PAGE_MAGIC_OFFSET`.
  
  Every `#[cfg(test)]` helper has a test use: `file_mtime_millis`, `as_written_before_d198`,
  `assert_no_d198_keys`, `legacy_log`, `MAIN_MAGIC`, `magic_on_disk`, `write_magic_on_disk`,
  `wall_now_millis`, `credited`.

**Found:** no second error. The first `cargo check --lib --tests --examples` settles it; a read cannot.
