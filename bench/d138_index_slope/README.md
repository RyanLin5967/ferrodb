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
| `frame()` | ⛔ **ZERO calls in both arms** | **not measured here** |
| `frames_for()` | linear in both arms, by design | unchanged (it is the control) |

⚠ **`frame()` is routed through the index in the code, and this harness never calls it.** Neither
axis nor any fire-check reaches it — both arms report `frame() calls 0`. Its coverage is the unit
test `the_position_index_and_the_frames_agree_per_key`, which asserts `frame()` returns the right
frame for *every* key. **Do not quote this artifact as evidence about `frame()`.**

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

    src/tel/engine.rs:461-462        SurfaceMerger::diff   (cfg(test) starts at :1149)
    src/agent_sql/merge_engine.rs:428  RuntimeMerger::diff (cfg(test) starts at :463)

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
