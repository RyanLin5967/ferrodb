# D138 — the effect-log position index, measured as a SLOPE

`reduce_slope.py` reads the two raw files in this directory and prints `slope_verdict.txt`.
Re-run it with `python3 bench/d138_index_slope/reduce_slope.py`; it exits non-zero on anything
that is not a clean integer-slope line.

## The verdict

    scan slope 1 -> 0 : +1.0000 -> +0.0000 (hit), +1.0000 -> +0.0000 (miss)
    shadow of the removed scan, inside the indexed binary: +1.0000 (hit), +1.0000 (miss)
    PASS — slope 1 -> 0 at all 8 rows, identical workload, removed scan still slope 1.

D137's law was `scan = parked_sessions + 125.5` (hit) / `+ 124.5` (miss), slope exactly 1.
The unindexed arm here **reproduces D137 row for row** — 125.5 / 375.5 / 625.5 / 875.5 / 1125.5 /
1375.5 / 1625.5 / 1875.5, identical to `bench/d129_run_raw.txt` banked at `cf5308b` — which is
what makes the comparison a comparison. The indexed arm reads **0.0 at all 8 rows**.

⭐ **It is a slope fitted at every row, not an endpoint ratio.** `reduce_slope.py` requires all
seven consecutive differences to agree before it will report a slope at all, and it names the
specific failure D138 predicted: **~0.5 means one of the three keyed sites was missed.**

## Why the zero is a reading and not an assertion

Two arms of a before/after are **two different binaries**, so a zero from a counter that was
accidentally disconnected reads exactly like a zero from a scan that no longer happens. Three
things close that, and all three are in the raw files:

1. **Fire-check (0), inside whichever binary is running.** `frames_for` still scans linearly in
   both arms; the counter is required to report `64` for a 64-frame scan. PASS in both. ⇒ a zero
   at another site is that site's, not the counter's.
2. **The SHADOW.** The indexed arm runs the pre-D138 linear scan *as well*, on the same call, and
   records what it walks at `SITE_APPEND_SHADOW`. It reads **125.5 → 1875.5, slope 1** — the
   removed cost and the actual cost are two columns of one table taken at one moment.
3. **Identical workload, asserted per row.** Both arms show `log_len 250 → 2000` and
   `500 hit / 250 miss` per block. The scan was removed; the work was not. A site that had simply
   stopped being called would show zero *calls*, which is no result rather than a zero.

Plus: **index-vs-scan disagreements over the axis: 0**, an equality checked on *every one* of the
6000 appends, over pgwire, not sampled.

## Scope — what these numbers do and do not cover

| site | in this harness | result |
|---|---|---|
| `MemEffectLog::append` | ✅ PARK axis, 6000 calls | 1875.5 → **0.0** |
| `classify_append` | ✅ durable addendum, 1000 calls | 499.5 → **0.0** |
| `DurableEffectLog` replay at open | ✅ addendum, K=1000 | 499500 elements → **0** |
| `frame()` | ⛔ **ZERO calls in both arms** | **unreachable from production — see below** |
| `frames_for()` | linear in both arms, by design | unchanged (it is the control) |

⛔ **`frame()` reports zero calls because NOTHING IN PRODUCTION CALLS IT — not because the harness
misses it.** Checked at HEAD: `frame` is not on the `EffectLog` trait (which declares only `append`
and `frames_for`), so it can only be called on a concrete store, and `git grep '\.frame('` over
`src`, `tests`, `examples`, `bench` and `tools` finds only this file, its test children, and
`consensus/log.rs` — a different type with its own `frame(Round)`.

⇒ ⭐ **So D138's "all three production scan sites" is one more than is true, and it is the exact
mirror of the same document's claim that `frames_for` has no production caller.** Both come from
reading call sites off a workload's counter rather than off the code: the D129 run labelled
`frames_for` "[no prod caller]" because its axes never merge, and `frame()` looked like a site
because it is `pub` and sits beside two that are. **Corrected in both directions:** the production
keyed scan sites are `append` and `classify_append` — two, not three — and `frames_for` has two
production callers on the merge path. `frame()` is still routed through the index (one index for
every keyed lookup beats a second shape) but carries none of D138's load. Its coverage is the unit
test `the_position_index_and_the_frames_agree_per_key`. **Do not quote this artifact about
`frame()`.**

⚠ **CHURN died at session 495 in BOTH arms**, on D136's 255-entry per-page provenance cap. That is
unchanged by D138 and is why both arms exit rc=1. It is not a regression signal.

⛔ **The addendum's own guard fires on a genuine zero in the indexed arm.** Its criterion is
`reopened == k && walked > 0`, i.e. "a zero means the counter did not fire" — which is correct for
the arm it was written for and **non-discriminating for this one**, because here `walked == 0` is
the result. The line `⛔ the durable addendum did not replay what it wrote, or counted nothing.`
is left in the raw file **unedited**; fire-check (0) is the instrument that separates the two
causes, and it passed. The harness was not changed to make this pass.

## The quarantined instrumentation — NOT on this branch, deliberately

The counters and the harness are measurement scaffolding and do not land. They are committed, and
auditable, on two branches whose only difference is the code under test:

    d138-measure-old   59b928d   main 29ad1f5, UNINDEXED     -> d138_old_raw.txt
    d138-measure-new   7087f74   dfe2c70, INDEXED (D138)     -> d138_new_raw.txt

Checked, not asserted: the harness file is **byte-identical** across the two branches
(`git diff d138-measure-old d138-measure-new -- examples/d129_effect_log_scan_count.rs` → 0 lines)
and so is the 186-line `scan_count` module (diffed directly). `src/pgwire/mod.rs` was **not** taken
from `D129-effect-log-scan-count`: that branch's copy is *behind* `main` by a landed correction to
`begin_read`'s comment.

⚠ Both raw files carry the harness's own label `frames_for() [no prod caller]`, which is **wrong**
— see the correction below. It is left as-run rather than edited, because an artifact is not a
document.

## Correction to D138's text in SCALE-DESIGN.md

D138 says: *"A fourth, `frames_for()`, has no production caller — the run labels it so."*

**It has two**, both outside `#[cfg(test)]`:

    src/tel/engine.rs                impl Merger for ThreeWayMerger :: diff
    src/agent_sql/merge_engine.rs    impl Merger for SurfaceMerger  :: diff

⛔ **AMENDED — the first cut of this very paragraph named both of them WRONG.** It said
`SurfaceMerger::diff` for the `tel/engine.rs` site and `RuntimeMerger::diff` for the `agent_sql`
one. The types are **swapped** (the `tel/engine.rs` call is inside `impl Merger for
ThreeWayMerger`; the `agent_sql` call is inside `impl Merger for SurfaceMerger`), and
**`RuntimeMerger` does not exist in this repo** — `git grep` found it only in the comment that
invented it. The COUNT of two production callers was correct; both names were not. Recorded rather
than quietly corrected, because of where it landed: a paragraph whose entire job is to fix a wrong
citation is the worst place to put one — the next reader greps the invented name, gets zero, and
cannot tell whether the finding itself was real. Found by a fresh-context adversarial read of the
diff, then verified at source before being believed.

The D129 harness observed zero calls because **neither of its axes merges**, not because the
callers do not exist — an absence in one workload read as an absence in the code. The weaker,
sufficient, and true statement is that `frames_for` is on the **merge/diff path, not the append
write path**, so it is not on the axis D137's slope-1 law measures. It is left linear on purpose:
it selects on `branch` alone and returns every match, which a `(branch, txn_id)` map cannot answer
at all. Pricing it needs a merging workload, which D136's cap currently blocks.

## Pre-registration — written before the suite was run

Baseline is `bench/land98a44ef_certified/SUMMARY.txt`: **per-target 2476, Go 97** at `98a44ef`.
`main` has moved to `29ad1f5` since, over three commits, none of which adds a test
(`21ae398` is a comment-only change to `src/branch/table_catalog.rs`; the other two are bench
files). This branch adds exactly **two** tests, both in `src/tel/`:

  * `tel::log::tests::the_position_index_and_the_frames_agree_per_key`
  * `tel::log::tests_durable_log::the_position_index_survives_a_replay_intact`

⇒ **Expected: per-target 2478 passed, 0 failed. Go 97 passed, 0 failed.**
Anything else is a finding, not a number to be explained away afterwards.

### AMENDMENT 1 — the base moved under the pre-registration (append-only; nothing above is edited)

Written before the suite ran, and before any count existed. While the measurement was in flight,
`main` moved **29ad1f5 → 18e63ba** (merges of D136, D130 and d85-rederive). `main` was merged into
this branch at `1db568b`, cleanly. The pre-registration above was derived from a base that no
longer exists, so it is re-derived here against the one that does.

⛔ **A broken instrument was caught doing this, and it is worth recording.** The first count used
`git grep -E '^\s*#\[test\]'`. **`git grep -E` does not honour `\s`** — under POSIX ERE it is not
an escape, so the pattern degenerated and matched only `#[test]` at column 0. It returned a
plausible `1281` and a plausible per-file diff showing ONE changed file, while the true answer was
2452 and two changed files. It failed silently, in the direction that looks like a smaller, calmer
result. **Use `[[:space:]]`, not `\s`, with `git grep`.**

Re-derived, with `^[[:space:]]*#\[(test|tokio::test)\]`:

| | `#[test]` lines in `src` + `tests` |
|---|---|
| `98a44ef` — the tree the banked **2476** certifies | 2452 |
| `main` `18e63ba` — the tree this branch now merges | 2452 |
| this branch, merged (`1db568b`) | 2454 |

**`main`'s test set is unchanged since the 2476 certification** — the per-file diff between
`98a44ef` and `18e63ba` is empty, and so is every other input the count depends on:
`#[ignore]` 3 → 3, `reaper_suite!` instantiations 3 → 3, `tests/*.rs` targets 132 → 132.
(That last set is checked because D139's addendum showed a `#[test]` inside a multiply-instantiated
macro counts once per instantiation. **Neither of this branch's two tests is inside a macro** —
both are plain `#[test]`s, one in `src/tel/log.rs`, one in `src/tel/tests_durable_log.rs`.)

⇒ **Expected, unchanged: per-target 2478 passed, 0 failed. Go 97 passed, 0 failed.**
Mode is `per-target`, matching the baseline — `whole` and `per-target` totals are not comparable.

### AMENDMENT 2 — three tests, not two (append-only; nothing above is edited)

Written before the suite ran. A fresh-context adversarial review of the diff landed three changes
after Amendment 1, one of which adds a **third** test:
`tel::log::tests::pushing_a_key_the_index_already_holds_is_refused_not_overwritten`. It forces
`Frames::push`'s duplicate-key refusal to fire — that refusal replaced a `debug_assert`, which is
compiled out of release, where the failure would have been silent frame-orphaning rather than a
wrong number. Fire-checked: reverting the guard to a plain `HashMap::insert` makes the test fail.

Re-derived with the same instrument as Amendment 1 (`^[[:space:]]*#\[(test|tokio::test)\]`):

| | `#[test]` lines in `src` + `tests` |
|---|---|
| `98a44ef` — the tree the banked **2476** certifies | 2452 |
| `main` `18e63ba` | 2452 |
| this branch at `bb10a8a` | 2455 |

Per file: `src/tel/log.rs` 5 → 7, `src/tel/tests_durable_log.rs` 24 → 25. All three are plain
`#[test]`s outside any macro, so D139's once-per-instantiation rule does not apply.

⇒ **Expected, revised: per-target 2479 passed, 0 failed. Go 97 passed, 0 failed.**
Amendment 1's 2478 is superseded by this line and is left above unedited.

### AMENDMENT 3 — re-derived at the commit that actually ships (append-only)

`d138_new_raw.txt` measured `dfe2c70`. Three review-pass commits followed it, so that artifact
named a tree that was no longer the one landing. Only one of those commits touched production
code at all — `Frames::push`, `debug_assert!` + `HashMap::insert` → an `Entry` match — and `push`
walks no frames in either version, so the measured quantity could not have moved. **That is an
argument, and a re-run is a measurement, so the re-run was done.**

`d138_ship_raw.txt` is the same instrument (harness `git diff` → 0 lines; `scan_count` module
diffed identical) over **`66e726c`, this branch's final production code**. Every commit after it
here touches `bench/` only — checked, not asserted: `git diff --name-only 66e726c HEAD` lists
exactly `bench/d138_index_slope/README.md` and `bench/d138_index_slope/d138_ship_raw.txt`.
(Stated that way rather than as "the commit that lands", which would stop being true the moment
banking this artifact added a commit — a citation has to survive its own landing.) Result, checked mechanically rather
than eyeballed — the extracted PARK tables diff **empty**:

    scan/hit 0.0 and scan/miss 0.0 at all 8 rows
    shadow/hit 125.5 -> 1875.5, shadow/miss 124.5 -> 1874.5, slope 1
    index-vs-scan disagreements: 0 over 6000 appends
    classify_append: 0.0 scanned over 1000 calls
    replay at open of a 1000-frame file: 0 elements walked (was 499500)

⇒ **The verdict at the top of this file holds for the shipping tree, not only for `dfe2c70`.**
`reduce_slope.py` still reads `d138_new_raw.txt`; `d138_ship_raw.txt` is the re-derivation beside
it, kept so the record shows the re-run happened rather than asserting it was unnecessary.

## D148 — what the removed scan was WORTH on the shipped durable path

`d148_durable_phase_raw.txt`. D148 asked: D137's slope-1 law is `MemEffectLog`'s, because pgwire
forces that store — but `src/cli/cli.rs:141` ships `DurableEffectLog`, which also encodes,
`pwrite`s and **`sync_data`s once per statement**. What share is the frame scan there? Precedent
said expect ~2% (D133's quadratic walk deflated to ≲1% behind `checkpoint_to`).

**The answer is that the question has no single number, and that is the finding.**

| | at 2000 frames | at 10⁶ frames (extrapolated) |
|---|---|---|
| the scan D138 removes | **0.073%** of an append | **~24–26%** of an append |
| `encode` + `pwrite` + `sync_data` | 99.9% | ~75% |

    sync_data       19904.664 ms of 20082.769 ms  = 99.1% of every durable append
    pwrite            160.365 ms
    encode              5.261 ms
    both lookups       11.182 ms                  = 0.056%  (indexed arm, flat in N)
    residual            +0.01%   -- the table closes

⇒ **Today it is swamped, and by far more than D148 guessed: 0.073%, not ~2%.** On the durable
path the lever is the fsync, which is 99.1% of the append — that is D81's territory, not D138's.
⇒ **But the scan grows at 1.09 ns per frame per append (least squares over all 8 rows; endpoints
give 1.20, and the gap between the two fits is the error bar) while encode+pwrite+sync is CONSTANT
in log length.** Crossover ~3.1M frames; at the 10⁶-branch objective the same scan is ~a quarter of
a durable append.

⇒ ⭐ **So "is D138 a 2% fix?" is unanswerable as posed — the percentage is a function of the axis,
which is exactly what a complexity-class change means.** It is 0.073% at 2000 frames and ~25% at
10⁶, from one unchanged line of code. **Quoting either number without its log length is the error.**

### Bounds on this, stated because they are easy to lose

⚠ **Two stores, two answers, and they must not be merged.** This drives `DurableEffectLog`
**directly**, which is what `cli.rs` ships. **pgwire forces `MemEffectLog`** (D137's own bound), and
there the scan is the whole lookup cost and D138 removes all of it. Neither result transfers.
⚠ **Wall-clock on a loaded box.** Mitigated, not eliminated: every phase is timed inside one
`append` so all phases carry the same load and the SHARE is a within-append ratio; every timer is
paired with a counter that must be non-zero; and the parts are summed against an independently
timed TOTAL, which closes to +0.01%.
⚠ **10⁶ is an EXTRAPOLATION from a fit over 250–2000 frames, not a measurement.** It is
conservative rather than optimistic: at 10⁶ the frame `Vec` is ~100 MB, so per-element cost would
rise with cache misses, steepening the slope.

### ⛔ The first run of this was WRONG, and the way it was wrong is the point

It probed session 0's key — **position 0** — so `position()` short-circuited after ONE comparison
and reported a 2000-frame scan as **1 ns**, i.e. "the scan is free". **A scan control that does not
scan manufactures exactly the conclusion the measurement exists to test.** Fixed by probing the
most recent session's key (D137: a recently-created txn's frame sits at the BACK, which is why a
hit costs about the whole Vec) **and** by counting what the scan actually walked, refusing the arm
below half the log. Every row now carries `scan control walked N of N frames — verified, not
assumed`.

⛔ **And the run before that was refused by its own clock check**: `Instant` ticks at **41 ns** here
and a single keyed lookup is tens of ns, so per-call timing measures the clock and truncates
toward zero — "unresolvable" reading as "free". Nothing here times a single lookup; the lookup is
measured by a batched probe of thousands of repetitions, both shapes over the same `Vec` under one
lock at one moment.
