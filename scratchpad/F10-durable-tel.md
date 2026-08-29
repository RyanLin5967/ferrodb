# F10 — a durable Typed Effect Log

Branch `F10-durable-tel`, worktree `/Users/idide/wt/ferrodb-F10-durable-tel`, branched from `af77d8a`.
**Nothing pushed.** 12 local commits.

---

## The row's exit criterion, clause by clause

LEDGER row F10 asks for: *"a durable `EffectLog` exists **and is the runtime's default**; frames
survive a restart; a merge computed after a restart agrees with one computed before it."*

| clause | state | proof |
|---|---|---|
| a durable `EffectLog` exists | **done** | `DurableEffectLog` in `src/tel/log.rs`; 29 tests in `tel::log` |
| frames survive a restart | **done** | `frames_survive_a_restart_with_every_field_intact`, and through the SQL surface in `tests/integration_durable_tel.rs` |
| a merge after a restart agrees with one before | **done** | `a_merge_computed_after_a_restart_agrees_with_one_computed_before_it` (both `MergeOutcome` and `Diff`), and `an_agent_tasks_frames_and_its_merge_survive_a_process_restart` |
| **is the runtime's default** | **NOT DONE — a lane blocker, not a judgment call** | see "The one thing I did not do" |

---

## What I built

### `src/tel/log.rs` — 212 → 1661 lines

Two `EffectLog`s and **one** re-append rule.

`classify()` is that rule as a single function, called by both stores, so it cannot be one thing in
memory and another on the disk. That is not tidiness: the durable store writes only what `classify`
calls new, so a disagreement would put a frame nobody accepted on the disk.

**The file format, and why growth is a delta rather than a whole frame.**

```
header:  magic(4) | version(4) | crc32(4)                    -- checksummed, unlike the WAL's
record:  total_len(4) | tag(1) | body | crc32(4)

tag 1  FrameOpen    key(20) | base(32) | seq(8) | schema_ver(4) | ops | guards | claims
tag 2  FrameExtend  key(20) | prior_ops(4) | prior_guards(4) | prior_claims(4) | ops | guards | claims
key = branch.id(8) | branch.generation(4) | txn_id(8)
```

The framing is `provenance/durable.rs`'s, which is `wal/log.rs`'s. What is new is that a
`FrameExtend` carries only the **new tail**. The naive alternative — re-append the whole frame and
let the reader collapse the copies — is quadratic in the file (a task of *n* statements writes
*n(n+1)/2* op copies) and, far worse, puts two copies of every early op on the disk and makes
correctness depend on a reader noticing. Deltas remove both:

* a **retry** writes *nothing at all* — measured in
  `a_retried_frame_does_not_double_its_add_across_a_restart`: the file is byte-identical after three
  appends of the same frame. So the Cassandra counter trap is not defended against, it is
  unrepresentable: there is no second copy to double.
* growth is linear — `growth_costs_the_delta_and_not_the_whole_frame_again` measures the 40th
  statement's cost against the 2nd.
* `prior_ops/guards/claims` make a record **missing or duplicated in the middle of the file visible
  by arithmetic**, which is the only check that can see the doubling that `dedup_by_txn` cannot:
  that de-dup keys on `TxnId`, so it collapses two frames wearing one id and is blind to *one* frame
  carrying an op twice.

**Reuse rather than reinvention, as the brief asked.** Strings go through
`crate::wal::log::write_str`/`take_str` (with the `u16` guard that function does not have).
`Value`'s tag numbers are `storage::index_page`'s, byte for byte, so the codebase has one tag
vocabulary for a `Value` on a disk. The `Storage` seam is `consensus/log.rs`'s, which is what lets a
crash be *aimed*. `MAX_APPEND_BYTES` is `consensus::log::MAX_ENTRY_BYTES` rather than a second
constant with the same derivation.

What is **not** reused, with the reason in the code: `Value::deserialize` indexes unchecked and
**panics** on a short slice, and `Value::serialize` writes `s.len() as u16` with a raw cast while
`Value::Decimal` documents that its digit text has no cap. Either would be a defect here, so encode
and decode live side by side in one file and are both exhaustive — the arrangement `paged_rows.rs`'s
own comment says the alternative cost: when the wide types were added to `index_page`, its separate
span table was not, and BIGINT/DECIMAL/TIMESTAMP cells were *write-only* on page-backed branches.

**`MemEffectLog` changed in one way**: on growth it extends the stored vectors instead of replacing
the whole frame. `Value`'s equality is *numeric* (`Integer(1) == Float(1.0)`,
`Decimal("1.50") == Decimal("1.5")`), so replacing let a re-append silently retype a value already
stored, while the durable store keeps the bytes it first accepted. Extending makes the two agree by
construction rather than by argument, and it is cheaper.

### `src/tel/tests_durable_log.rs` — 1560 lines, 24 tests

### `tests/integration_durable_tel.rs` — 337 lines, 3 tests

Drives `BEGIN AGENT SESSION` + real `UPDATE`s through the SQL surface so the frames come from
`stage_all` rather than a fixture, restarts with a new runtime over the log that survived, and
asserts the `Diff` a `SurfaceMerger` computes is the same `Diff`. Its anti-vacuity half runs the
identical script over `MemEffectLog` and asserts it comes back empty.

### `.gitignore`

`*.tel` and `*.provenance` added to the "rest of a database" section, which existed because
"running the README's own quickstart leaves four files in a clean clone". `*.provenance` was already
missing (E79's); both are one line.

---

## Defects found — three in my own code, by attacking it in a fresh context

Per the rule that self-review inherits the assumption that caused the defect, the finished store was
attacked by six read-only agents over a **pristine `git archive` export of the commit** (not the
working tree), each finding then put to three independent skeptics prompted to refute it. 18
findings, 3 confirmed. I then verified the mechanism of four of the 15 *refuted* ones myself and
found three of those real too. All are fixed.

**CRITICAL — a failed read was treated as end-of-file, and the heal then destroyed good records.**
`src/tel/log.rs` scan, both record reads: `if pread_all(..).is_err() { break offset; }`. Both bounds
are already proved against the length `Storage::len` reported, so **neither read can fail on
end-of-data** — the only way in is a real I/O error. So an unreadable sector was indistinguishable
from a torn tail, and the heal then `set_len`'d away every *undamaged* record after it while `open()`
returned `Ok`: a store short of effects it had reported durable, with the records gone from the
media. `consensus::log::scan_frames` propagates for exactly this reason; `provenance/durable.rs:221`
and `:229` carry the same swallow, which is a **defect there** rather than a precedent (not my file;
reported below). `storage::sim` cannot inject this — it declares reads unfaultable because "a failed
read leaves the durable image untouched", which is precisely the assumption a truncating reader
breaks — so the test brings its own `Storage`.

**A torn first header write bricked the log for ever.** Found by widening the crash sweep from one
append to the whole task: `open` refused any file shorter than its 12-byte header, so a crash
tearing the very first write left a file that could never be opened. The boundary is `<= HEADER_SIZE`
now and it is exact rather than generous — records begin at `HEADER_SIZE`, so a file no longer than
the header cannot hold a byte of any frame; one byte past it is refused, because that byte could be a
record's.

**A NaN delta turned a byte-identical retry into a contradiction.** `Delta` derives `PartialEq`, so
`Float(NaN) != Float(NaN)`; `Value` avoids that by comparing through `Ord`/`total_cmp`, but
`OpKind::Add` and `EscrowClaim::amount` carry a `Delta` and bypass `Value`. Since `stage_all`
re-appends the open frame once per statement, such a frame could then **never grow again** — every
later statement of that task would fail permanently. `Delta::compose` makes NaN out of `inf + -inf`,
so it is reachable without anyone writing `NAN`.

**A guards-or-claims tail was never tested.** Every growth in the suite grew `ops` only, so
`put_tail(.., &ops[n..], &[], &[])` kept it green — while `stage_all` produces exactly a non-empty
guards tail on the second WHERE-carrying statement of every agent task, and `frame.rs` says in its
own words that guards are not derivable from ops by anything. The production failure is quiet: the
merge engine re-checks guards as preconditions, so a guard that never replayed is never re-evaluated
and a merge that should `Conflict` comes back `Clean`.

**The depth refusal walked the tree it was refusing.** The message used
`Guard::violated_predicate()`, which falls back to a recursive `Display` over the whole expression —
so on the one input the cap exists to refuse, building the refusal would overflow the stack the
refusal protects.

**The read bound was a transport tuning knob.** `MAX_RECORD` served as both the write and the read
limit while being derived from `replication::MAX_FRAME_BYTES`. Lowering that knob would have made
records already on a disk exceed the scan's bound — and the scan treats an over-long length prefix as
a torn tail, so a *tuning change* would have truncated committed effect logs while reporting a
healthy heal. The read bound is a constant of the format now, with a `const` assertion keeping the
write bound inside it.

Plus: a boolean byte other than 0 or 1 is refused rather than read as `true` (one value with 255
encodings is the record-level defect the trailing-bytes check already refuses); `GUARD_MIN`
corrected from 3 to 4 (the shortest expression is `Literal(Null)` at two bytes, not one).

---

## Mutants: 23 of 23 killed

`scratchpad/mutants-f10.py`, one mutant per rule, each with the test that must notice. Raw logs
committed: `scratchpad/mutants-f10.log`, `-rest.log`, `-survivors.log`, `-run1-partial.log`,
`-run2-first12.log`.

| mutant | test that killed it |
|---|---|
| retry-writes-a-second-copy | `a_retried_frame_does_not_double_its_add_across_a_restart` |
| growth-rewrites-the-whole-frame | `a_growing_frame_replays_to_its_final_contents_and_not_to_the_sum_of_its_appends` |
| no-arithmetic-hole-check | `a_delta_whose_prior_counts_do_not_line_up_is_refused` |
| undeclared-extend-is-healed | `an_extend_for_a_frame_the_file_never_declared_is_refused` |
| second-open-overwrites | `a_second_open_for_one_key_is_refused` |
| trailing-bytes-ignored | `an_unknown_tag_and_a_record_with_trailing_bytes_are_both_refused` |
| torn-tail-not-healed | `a_partial_append_is_discarded_reported_and_then_written_over` |
| torn-tail-not-reported | same |
| header-crc-unchecked | `a_foreign_file_and_a_damaged_header_are_both_refused` |
| unguarded-string-length | `a_value_too_long_for_its_length_prefix_is_refused_rather_than_truncated` |
| no-guard-depth-cap-on-decode | `a_guard_nested_past_the_cap...` — **killed by a stack-overflow abort**, which is the detection |
| no-guard-depth-cap-on-encode | same test |
| depth-refusal-walks-the-tree-it-refuses | same test |
| index-accepts-before-the-record-lands | `a_failed_append_leaves_a_store_that_still_works_and_a_file_that_still_opens` |
| torn-first-header-bricks-the-log | the crash sweep **and** the header test |
| frames_for-loses-its-ordering | `frames_come_back_in_sequence_order_per_branch` |
| mem-replaces-instead-of-extending | `retyping_a_stored_value_under_growth_cannot_split_the_two_stores` |
| read-error-swallowed — length prefix | `a_read_error_mid_file_refuses_the_open_and_destroys_nothing` |
| read-error-swallowed — record body | same test |
| extend-drops-guards-and-claims | `a_growth_whose_tail_carries_guards_and_claims_replays_with_them` |
| nan-delta-breaks-a-retry | `a_nan_delta_does_not_turn_a_retry_into_a_contradiction` |
| non-canonical-boolean-accepted | `a_boolean_byte_that_is_neither_zero_nor_one_is_refused` |
| the-durable-arm-is-secretly-in-memory | both restart tests in `tests/integration_durable_tel.rs` |

**Four survived the first full sweep, and all four were gaps in the tests rather than in the code.**
Every one is now closed and killed:

* `frames_for-loses-its-ordering` survived because the **pre-existing** ordering test appended
  seq 5 then 2 and asserted `[2, 5]` — reversing a two-element append order happens to sort it. The
  fixture is asymmetric now (5, 2, 9), and it asserts the `txn_id` tie-break at equal `seq`, which is
  the one that matters: `AgentRuntime` pins `seq` to 0 for every frame it writes, so on the SQL path
  the whole ordering is by `txn_id` — and `SurfaceMerger::diff` does not re-sort.
* the read-error mutant survived on the **record-body** read: the scan reads the 4-byte length prefix
  and then the whole record *at the same offset*, so a fault keyed on the offset alone only ever
  reaches the first of the two. The fixture now picks which read to break, the test breaks both, and
  the mutant list has one entry per read site.
* the depth-refusal mutant survived because the too-deep guard carried source text, and
  `violated_predicate()` returns that without recursing. Added a synthesised guard — the only case
  where the accessor falls back to the recursive `Display` — and asserted the message does not
  contain the rendered expression.
* `nan-delta-breaks-a-retry` was a harness artifact (cargo target-lock contention), killed on re-run.

### The harness had the defect it exists to prevent

`shutil.move` restores the **backup's** mtime, which `shutil.copy` set *before* the mutated build
ran. The restored source was therefore older than the test binary, cargo skipped the rebuild, and
every later run measured the mutant while `git status` read clean. Measured: `src/tel/log.rs` at
17:59:23, the test binary at 17:59:29, and `cargo test --lib tel::log` reporting **28 passed /
1 failed against source git said matched HEAD**. Fixed with `os.utime` after every restore, and the
run now *refuses* if the tree does not pass afterwards. Two earlier classifier defects were also
found and fixed mid-run and are documented in the script: concatenating two cargo invocations'
output made a killed mutant read as "collected nothing", and a mutant that kills by aborting the test
binary prints no `test result:` line at all and read as a survivor.

---

## Measurements, with the instrument named

**Guard nesting depth** — `scratchpad/guard-depth-measurement.md`, committed. `MAX_GUARD_DEPTH = 256`
is not a guess: encoding, fsyncing and replaying a guard through this store round-trips at depth 512
and **aborts the process with a stack overflow at 768** in a debug build on a libtest thread, and the
pre-existing guard operations (`Guard::clone`, `Guard::check`, `GuardExpr`'s `Display`, and `Drop` —
all recursive) took the process down at 1024. That last fact is why a cap is the right mechanism
rather than a workaround: `GuardExpr` recurses in `Clone` and in `Drop`, so an iterative encoder and
decoder would not remove the limit, only move the abort to the moment the decoded frame is dropped.

**Aimed crashes** — 60 points: every faultable operation of a whole agent task (the header write and
its fsync, the `FrameOpen`, both `FrameExtend`s, a second transaction) × 3 write shapes × 2
durability models, on `storage::sim` so no disk is touched. The invariant is the promise: an append
that returned `Ok` is still there, and the interrupted append is wholly there or wholly absent —
never half of a statement's effects, because half of a statement's effects is a counter a merge gets
wrong and never notices. The sweep asserts it ran ≥ 40 points, so a sweep that collected nothing
cannot pass.

**Growth is linear** — the 40th statement of a task costs within 8 bytes of the 2nd, and at least 20
bytes (the anti-vacuity half: a store that wrote nothing would also pass the first check).

**macOS holds an aborted test binary for minutes.** Two mutants kill by aborting, and `ReportCrash` +
`spindump` held the corpse at 0.0% CPU in state SN for over three minutes at load 6. Noted in the
script so the next run is not surprised; per-test timeout sized from it.

---

## Evidence

### `cargo test`, per target, with a durable record — 86 targets, 1722 passed, 0 failed

`tools/verify-suite.sh` in per-target mode is the sanctioned instrument, and it **refused** thirteen
targets into a ~2-hour run because this worktree's `cargo test --test integration_base_backup` was
killed with `Terminated: 15` by a sibling agent's cleanup — a silent target is not a passing one, and
the guard did its job. It has no resume, though, so one SIGTERM costs the whole run. I wrote
`scratchpad/suite-resumable.sh`: one line per target, appended **only** after that target printed a
verdict, so a re-run skips what is recorded and a kill costs one target. A killed target leaves no
line at all, which is the point.

**The arithmetic cross-checks the baseline exactly.** 1695 at `af77d8a`, plus 24 tests in
`src/tel/tests_durable_log.rs` and 3 in `tests/integration_durable_tel.rs`, is 1722.

```
targets=86  passed=1722  failed=0        Go: ok  github.com/.../cdc-consumer  37.4s  (97 tests)
```

Three targets went red in the sweep and a fourth was SIGTERMed, all on a machine running four other
agents' builds at load 4–24 against hardlink-farmed `target/` dirs. Every one is a **multi-process
test whose own timeout expired**, and none of them references `EffectLog`:

| target | sweep verdict | why | standalone re-run |
|---|---|---|---|
| `integration_consensus_failover` | 0 passed / 1 failed | *"node 1 never said READY (timed out waiting on channel)"* | **1/0, twice** |
| `integration_cdc_go_consumer` | 3 / 1 | *"the server never published a listening address within 60s, and it was alive every time it was asked"* | **4/0, twice** |
| `integration_base_backup` | 4 / 1 | `ConnectionRefused`, with the primary already exited cleanly — the exact race that test's own doc comment records **two prior sightings of**, on this same fleet-loaded machine | **5/0, twice** |
| `integration_cdc_publication` | SILENT, rc=143 | SIGTERM from outside | **5/0, twice** |

Measured rather than argued: all four pass 2/2 standalone. Both sets of verdicts are kept on disk —
`suite-f10-results.tsv` is the sweep, `suite-f10-rerun.tsv` the re-runs, `suite-f10-final.tsv` the
reconciliation naming its source per target — in `~/wt/artie-research/build-G/` and mirrored into
`scratchpad/`.

The blast-radius argument behind that judgment, stated so it can be checked rather than trusted: the
whole diff outside this row's own new files is `src/tel/log.rs`, **one** re-export line in
`src/tel/mod.rs`, and `.gitignore`. The only behaviour that changes for an existing caller is
`MemEffectLog::append` extending instead of replacing. So the targets that can break are the ones
calling `EffectLog::append`/`frames_for`, and the runner puts them first:

```
lib 1116/0   agent_sql_surface 31/0   integration_effect_log 3/0   integration_durable_tel 3/0
integration_merge_agreement 6/0   prop_merge_outcomes 6/0   integration_simulate 24/0
integration_runtime_reattach 4/0   integration_runtime_concurrency 4/0   integration_branch_pages 10/0
integration_zero_copy_fork 6/0   integration_trunk_tree_authority 3/0   adv_f5_probe 5/0
guard_precondition_probe 5/0   adv_f6_dropped_capture 3/0   adv_f4_false_refusal 13/0
```

`integration_effect_log` is the one that matters most — it *is* the re-append contract, and it is
green.

### The CI gate

```
RUSTFLAGS="-D duplicate_macro_attributes -D dead_code" cargo build --lib --tests --examples
    Finished `dev` profile ... in 2m 20s          exit 0
```

Two warnings remain and both predate this branch, in files this row does not own
(`catalog_page.rs:440` `unused_mut`, `adv_f6_dropped_capture.rs:17` `unused_imports`) — neither is
one of the two denied lints.

---

## The one thing I did not do, and why it is a blocker rather than a judgment call

The row's second clause is *"and is the runtime's default"*. The durable store is built, tested and
**wiring-ready**, and `DurableEffectLog::default_for_database(db_path)` exists precisely so the last
mile carries no decision — it owns the `<db>.tel` naming convention and the choice of
implementation, mirroring what `provenance::durable` says about `with_durable_provenance`: *"a
constructor that takes page stores is never given the database's name, so the layer that owns the
path applies it."*

That layer is two files, and **both are held by live sibling agents in this same wave**
(`~/wt/artie-research/phase-f-wave-b.tsv`):

* `src/cli/cli.rs` — **F11's**, which is wiring a lease thread into `run_cli` and whose own
  `PROGRESS.md` shows it working in exactly the block that constructs the runtime (`cli.rs:88`
  onwards).
* `examples/pgserver.rs` — **F11's** too, same edit.
* `src/agent_sql/runtime.rs` — **F9's**, and F9's dispatch brief tells it in so many words: *"you may
  touch `src/agent_sql/runtime.rs` (E82 and E79b are merged, so no other agent holds it)."*

The shared preamble's rule is *"You own exactly that file plus a test file you create… Another agent
is editing files beside yours and a change there conflicts with all of them."* Editing F11's file
after F11 was told it owned it is the expensive, hard-to-undo thing that rule exists to prevent, so
I did not.

**The exact edit, so whoever merges the lanes does it in one step with nothing to decide:**

Verified against the files as they stand at `af77d8a` — four arguments, two per file:

```rust
// src/cli/cli.rs:119 and :125   (the enclosing fn is `run_cli(db_path: &str)` and uses `?`)
-                Arc::new(MemEffectLog::new()),
+                DurableEffectLog::default_for_database(db_path)?,

// examples/pgserver.rs:95 and :102   (the path variable is `db`, and this file uses `.expect`)
-            Arc::new(MemEffectLog::new()),
+            DurableEffectLog::default_for_database(&db).expect("durable effect log"),
```

`default_for_database` returns `Arc<dyn EffectLog>` already, so the argument type is unchanged; the
import becomes `use ferrodb::tel::DurableEffectLog;` (re-exported from `tel/mod.rs` on this branch;
`crate::tel::log::DurableEffectLog` also works). `tests/integration_durable_tel.rs` already builds an
`AgentRuntime` exactly that way and asserts the restart, so the line is proven before it is written.

One consequence to apply with it: **`DEMO.md:231`** says *"The effect log is `MemEffectLog`, which is
a memory implementation rather than a durable one."* That sentence is true today and becomes false
the moment those four arguments change. It is in F11's neighbourhood (F11's own exit criterion
touches `DEMO.md`'s "10/10 MET"), so I left it alone rather than racing them for the file.

---

## Findings about files I do not own

Each verified against the code, not inferred.

1. **`provenance/durable.rs:221` and `:229` carry the same critical read-swallow I fixed.**
   `if pread_all(..).is_err() { break offset; }`, with the heal at `:248-252` doing `set_len` +
   `sync_all` and `open` returning `Ok`. Same mechanism, same consequence: a bad sector under one
   `Stamp` record silently deletes every attribution after it. `consensus::log::scan_frames`
   (`log.rs:959,964`) already propagates. Worth a row.

2. **The doc comment on `AgentRuntime::log` is stale.** `src/agent_sql/runtime.rs:637-638` says
   *"`MERGE` and `DIFF` both read from here through the shared traits, so the log is on the live path
   rather than a side record."* `self.log` appears exactly **twice** in that file — the accessor at
   `:640` and one `append` inside `stage_all` at `:1831` — while `merge`, `evaluate_merge` and `diff`
   read `Workspace.frame`. The log is **write-only** on the runtime's own path. This matters for
   reading F10's exit criterion: the merge that can be computed after a restart is a `Merger` over the
   log (`SurfaceMerger::new(runtime.log())`, which `tests/agent_sql_surface.rs` already does), not
   `AgentRuntime::merge`. Recorded in the header of `tests/integration_durable_tel.rs`.

3. **`storage::sim` declares reads unfaultable, and its stated reason is now false.**
   `sim.rs:24-26`: *"a failed read leaves the durable image untouched, so it cannot produce the class
   of bug this harness hunts."* That was true of every reader in the tree until a reader started
   truncating on a read error — which both this module and `provenance::durable` did. Widening the
   fabric to fault reads would have caught my critical defect and would catch provenance's. Not my
   file; the test here brings its own `Storage` instead.

4. **`Value::deserialize` (`storage/index_page.rs:200`) panics on a short slice**, and
   `Value::serialize` truncates a string over 65535 bytes with a raw `as u16` while `Value::Decimal`
   is documented as having no digit cap. `agent_sql::paged_rows::value_span` exists to pre-validate
   the first problem and is private to its module. Fixing both in `index_page` would let this module's
   `Value` codec be deleted.

---

## Files, tests, commits

| file | lines | tests |
|---|---|---|
| `src/tel/log.rs` | 212 → 1661 | 29 in `tel::log` (5 pre-existing inline, 24 new) |
| `src/tel/tests_durable_log.rs` | 1560 (new) | 24 |
| `tests/integration_durable_tel.rs` | 337 (new) | 3 |
| `src/tel/mod.rs` | +1 re-export | — |
| `.gitignore` | +5 | — |

Test names, all green:

```
frames_survive_a_restart_with_every_field_intact
a_retried_frame_does_not_double_its_add_across_a_restart
a_growing_frame_replays_to_its_final_contents_and_not_to_the_sum_of_its_appends
growth_costs_the_delta_and_not_the_whole_frame_again
a_contradicting_reappend_is_refused_and_never_reaches_the_file
retyping_a_stored_value_under_growth_cannot_split_the_two_stores
the_same_txn_id_on_a_different_branch_is_a_different_frame_after_a_restart
a_growth_whose_tail_carries_guards_and_claims_replays_with_them
a_nan_delta_does_not_turn_a_retry_into_a_contradiction
a_boolean_byte_that_is_neither_zero_nor_one_is_refused
a_merge_computed_after_a_restart_agrees_with_one_computed_before_it
a_partial_append_is_discarded_reported_and_then_written_over
an_extend_for_a_frame_the_file_never_declared_is_refused
a_second_open_for_one_key_is_refused
a_delta_whose_prior_counts_do_not_line_up_is_refused
an_unknown_tag_and_a_record_with_trailing_bytes_are_both_refused
a_foreign_file_and_a_damaged_header_are_both_refused
a_value_too_long_for_its_length_prefix_is_refused_rather_than_truncated
a_guard_nested_past_the_cap_is_refused_on_the_way_in_and_on_the_way_out
a_crash_at_any_point_during_an_append_loses_nothing_already_acknowledged   (60 aimed points)
a_failed_append_leaves_a_store_that_still_works_and_a_file_that_still_opens
a_read_error_mid_file_refuses_the_open_and_destroys_nothing
concurrent_appends_all_reach_the_file_exactly_once                        (8 threads x 25 frames)
the_log_opens_on_a_real_file_and_recovers_from_it
frames_come_back_in_sequence_order_per_branch                             (strengthened)
-- tests/integration_durable_tel.rs --
an_agent_tasks_frames_and_its_merge_survive_a_process_restart
the_durable_store_captures_exactly_what_the_in_memory_one_captures
wide_typed_values_survive_the_restart_with_their_bytes_intact
```

18 commits, `af77d8a..HEAD`. Nothing pushed.

```
7727c2e test(F10): the suite, per target, with a durable record
08d2506 test(F10): close the four gaps the mutation sweep found
2b7af35 test(F10): the exit criterion through the SQL surface, not just the store
cbe6a4d fix(F10): a failed read is a fault, not the end of the file
4411a66 test(F10): the two stores hold the identical frame, not merely an equal one
52ef826 fix(F10): a torn first header write must not brick a log that never held anything
6c3d45d feat(F10): a durable Typed Effect Log, with growth appended as a delta
   ... plus harness and progress commits
```

Re-runnable evidence, all committed:

```
scratchpad/mutants-f10.py            23 mutants; `python3 scratchpad/mutants-f10.py [names...]`
scratchpad/suite-resumable.sh        the resumable per-target suite
scratchpad/rerun-reds.sh             standalone re-runs of the load-starved targets
scratchpad/guard-depth-measurement.md
scratchpad/mutants-f10*.log, scratchpad/suite-f10-*.tsv
scratchpad/recon/                    the five recon notes this row was built from
```

---

## What I would tell the next person in one line

The store is done and hard: 23/23 mutants killed, 60 aimed crash points, a critical read-error
defect found and fixed by attacking it in a fresh context, and the same defect still open in
`provenance/durable.rs`. The only thing between it and the ledger's exit criterion is four arguments
in two files a live sibling owns, and `default_for_database` exists so that edit carries no decision.
