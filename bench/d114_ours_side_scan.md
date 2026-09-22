# D114 — CLOSED. The `ours`-side scan is real, and it is not the cost.

> ## ⛔ STOP — if you arrived here because you noticed `runtime.rs:4380`
>
> You read that `evaluate_merge` computes `ours` by scanning **every op the branch recorded**, once
> per changed cell, while the very next line computes `theirs` through `concurrent_op`, which D86
> already indexed to a `partition_point`. Two sides of the same comparison, one line apart, and only
> one of them fixed.
>
> **That reading is CORRECT. The scan is exactly `delta × ops`, confirmed to a ratio of 1.000 by an
> exact counter. It is also NOT the merge's cost, and indexing it buys nothing.**
>
> Measured before any fix was written: `examined` was driven up **128.5x** and merge latency did not
> move — it got slightly *faster*. The merge's cost tracks the **delta**, not the op log. A
> configuration that walks **32x more ops** merges **faster** than one that walks fewer but has a
> bigger delta. The numbers are below; the raw runs are in `bench/d114_before.txt` and
> `bench/d114_before_delta32_rerun.txt`.
>
> **Do not "fix" this.** It is a real O(delta · ops) scan that costs nothing measurable, and removing
> it would have been the third time a proposed mechanism for a slope on this exact code turned out
> to be the wrong mechanism (D68, D69-REOPEN, D114).

Measured at `1bf8abe` (`Merge D98-outer-lock`, the gate17 head) with the `ours_scan` counters added
and **no other change**. The fix was never written, deliberately.

---

## THE RESULT

### Instrument, and why the conclusion is load-independent

The headline is an **operation count**, not a duration. `ours_scan_counters()` returns
`(examined, matched)`, taken from inside the iterator that actually walks the ops. **Load cannot
move an integer**, so the conclusion — that `examined` rises by two orders of magnitude while merge
latency does not — does not depend on how quiet the box was. Every millisecond below is an upper
bound and carries the load at lock acquisition.

### Axis 1 — the pre-registered falsifier, at D86's own delta of 4

`bench/d114_before.txt`, 15/15 merges applied at every point, both directions, `load_at_acquire=6`.

`CHURN` is the arm where **only the scan grows** (the ops land on a column that is written back to
its base value, so they are examined and never folded):

| ops/branch | examined/merge | matched/merge | MERGE ms (asc) | MERGE ms (desc) |
|---|---|---|---|---|
| 4 | 32 | 4 | 4.856 | 3.888 |
| 16 | 80 | 4 | 3.818 | 3.812 |
| 64 | 272 | 4 | 4.077 | 4.121 |
| 256 | 1,040 | 4 | 4.274 | 4.044 |
| 1024 | 4,112 | 4 | 4.502 | 4.667 |

⇒ **256x the ops, 128.5x the `examined`, and merge latency goes 0.93x (ascending) / 1.20x
(descending).** The `REPEAT` arm, where the ops land *on* the delta cells so the fold grows too, is
the same: 256x `examined` for 1.22x / 1.13x.

**The pre-registered rule was "FLAT in ops per branch ⇒ the reading is wrong and the row closes."
It is flat. The row closes.**

### Axis 2 — the decisive control, at 8x the delta and 8x the ops

The scan costs `delta × ops`, so if it is ever the merge's cost it is at the largest delta and the
largest op count together. `bench/d114_before_delta32_rerun.txt`, delta fixed at **32**, ops to
**8192** — 263,168 ops walked per merge. 10/10 applied, both directions, `load_at_acquire=3`.

| ops/branch | examined/merge | MERGE ms (REPEAT asc) | MERGE ms (CHURN desc) |
|---|---|---|---|
| 32 | 1,024 / 2,048 | 10.734 | 9.814 |
| 128 | 4,096 / 5,120 | 9.815 | 8.020 |
| 512 | 16,384 / 17,408 | 8.877 | 8.677 |
| 2048 | 65,536 / 66,560 | 8.945 | 8.903 |
| 8192 | 262,144 / 263,168 | 11.096 | 10.777 |

⇒ **256x the ops for 1.03x–1.54x the latency**, depending on arm and direction. At the extreme
corner the scan is worth perhaps 1–2 ms of an ~11 ms merge, and nowhere near the delta of 4 that a
real agent workload produces.

### The comparison that names the mechanism

Same instrument, four configurations, from the delta arm and the ops arms of the same run:

| configuration | examined/merge | MERGE ms |
|---|---|---|
| delta=1, ops=8192 | 8,193 | **6.178** |
| delta=4, ops=8192 | 32,784 | **6.411** |
| delta=32, ops=32 | 1,024 | **8.8 – 10.7** |
| delta=32, ops=8192 | 263,168 | **10.0 – 12.9** |

⭐ **`delta=4, ops=8192` walks 32x MORE ops than `delta=32, ops=32` and merges FASTER** (6.411 ms
against 8.8–10.7 ms). Merge cost tracks the **delta**. It does not track `examined`. That is the
whole finding, and no wall-clock reading is needed to see it — the two `examined` columns differ by
32x in the direction opposite to the latency.

### Why it is flat, arithmetically

`examined = delta × ops` held at ratio **1.000** at every point of the delta arm, so the scan is
exactly what the source reading says it is. It is simply small: even the 263,168-op corner is a few
hundred microseconds to ~2 ms of a linear walk over a `Vec`, against a merge that is already doing
page writes, guard re-checks, fingerprinting and publication. The reading was right about the
*shape* and wrong about the *magnitude*, and only a measurement separates those two.

---

## WHAT THE ROW GOT WRONG (corrections, all verified from source at `1bf8abe`)

1. **"Quadratic in the branch's OWN write count"** — no. `snapshot.ops` is `ws.frame.ops`, and the
   frame is created fresh by `BEGIN AGENT SESSION` (`runtime.rs:1519`) and destroyed by the `seal`
   that `MERGE` performs (`:5434`). It does not outlive one session, so this is *not* D86's axis:
   it grows with one agent's task length, not with the age of the system.
2. **"Three sites, both production merge paths"** — one site, one path. Only `:4380`
   (`evaluate_merge`) is reachable from SQL. `:3792` and `:3978` are both inside `merge_into`, a
   `pub fn` whose only callers are in `tests/integration_sibling_merge.rs`. Same shape D110 already
   recorded when a brief called `merge_into` "the real merge path".
3. **The scan was never the merge's only linear term.** `evaluate_merge` clones the whole op Vec
   once per merge (`:4043`, and `:3659`/`:3667` on the sibling path), so the merge was **already**
   O(ops) before any scan. The scan took it from O(ops) to O(delta · ops) — a constant factor of
   the delta, never a new complexity class. Recorded as Amendment 3 *before* measuring, which is
   why the flat result was not a surprise.

## ⚠ A REAL QUADRATIC, NEXT DOOR, AND IT IS 200x LARGER

Found while confirming that an agent `UPDATE` reaches `ws.frame.ops`, and then measured by the same
harness. `stage_all` ends **every statement** with `ws.frame.clone()` + `self.log.append(&frame)`
(`runtime.rs:2924-2929`) — the whole frame, cloned and re-appended once per statement. A session
issuing W statements copies 1 + 2 + ... + W ops.

Writes phase per session, delta=32 `REPEAT` ascending, from `bench/d114_before_delta32_rerun.txt`:

| ops/session | writes ms | local exponent vs previous point |
|---|---|---|
| 32 | 0.450 | — |
| 128 | 2.056 | 1.10 |
| 512 | 11.990 | 1.27 |
| 2048 | 150.952 | **1.83** |
| 8192 | 2247.207 | **1.95** |

⇒ **Converges to quadratic** (exponent 1.95 across the top decade), exactly as the frame clone
predicts, with a linear statement cost dominating at small W. At 8192 ops that is **2.25 seconds of
writes per agent session against ~11 ms for the merge** — the thing I was sent to measure is ~200x
smaller than the thing sitting next to it.

The clone is deliberate (the comment explains that re-appending replaces rather than double-counts,
since `Add` is not idempotent), and `grep` of `SCALE-DESIGN.md` finds no row for it. **This is
evidence for a new row, not a fix I made** — it is outside D114's scope and the measurement above is
all of it that is established.

## Provenance of the numbers

* `bench/d114_before.txt` — delta=4, 15/15, both directions, `load_at_acquire=6`. **Clean.**
* `bench/d114_before_delta32.txt` — the first delta=32 control. **CONTAMINATED:** it took ~56 s of
  another agent's `cargo test --lib` at 20:47. Kept as the record rather than deleted. **The
  pre-registered ascending/descending control is what caught it** — the same point read 36.121 ms
  ascending and 9.798 ms descending. A single monotone pass would have reported the outlier as a
  slope.
* `bench/d114_before_delta32_rerun.txt` — the same control re-run on a quiet box,
  `load_at_acquire=3`. The outlier is gone (8.945 / 9.034 ms). This is the one quoted above.
* The conclusion rests on the **counts**, which load cannot move; the re-run removes the caveat from
  the durations as well.

## Verification of the code that shipped with this row

No fix was written, but the instrument is production code: the three identical scans in
`evaluate_merge`, `merge_into` and `sibling_op` were collapsed into one `ours_ops_on_cell`, and the
counters added. That is a behaviour-neutral claim, so it was measured rather than asserted:

    tools/verify-suite.sh D114-4bfd6c6
    D114-4bfd6c6: mode=whole rc=0 passed=2426 failed=0 build_errors=0 head=4bfd6c6
    D114-4bfd6c6: go    rc=0 passed=97   failed=0

`head=4bfd6c6` is the commit the green certifies. **This artifact lives one commit later**, because
a body cannot quote a summary that names its own sha; `git diff --name-only 4bfd6c6 <tip>` is
`bench/d114_ours_side_scan.md` and nothing else, so no compiled file moved after the green.
`tools/verify-impacted.sh` alone was **not** sufficient and said so: run against `HEAD` it saw only
the example file and selected **0 of 128** integration targets, because the `runtime.rs` change was
already committed. It had to be re-run as `--since 1bf8abe --binaries` to select the 96 targets
that actually reach this code.

## Two harness defects, found and fixed before any number was trusted

Both produced **n=1 out of 15** while printing a normal-looking row — a broken instrument wearing a
result's clothes, and the exact failure mode this project's own rules warn about.

1. A per-cycle agent name (`a{seq}`) is **refused** by the provenance slot, which is declared per
   branch: *"provenance slot prov1 is already declared as agent=a0 ... refusing to redeclare it as
   agent=a1"*. Every merge after the first errored. D68's harness reuses one name, which is why it
   never hit this.
2. The `REPEAT` arm's written value did not depend on `seq`, so cycle 2 re-wrote what cycle 1 had
   published, the delta was zero, and the merge applied nothing.

The harness now prints `⛔ only N of M merges APPLIED — this row is NOT a measurement` on the row
itself, because a partial `n` in a median column is indistinguishable from a result.

---

**Status at the time this section was written: NO NUMBERS EXIST.** Everything below the line
"PRE-REGISTRATION" was committed before the harness had ever been run, and the amendment section
is append-only. Anything added after the first measurement is marked as an amendment and dated.

Measured at, and only at: **`1bf8abe`** (`Merge D98-outer-lock`), the gate17 head.

---

## PRE-REGISTRATION

### The row, as recorded in `SCALE-DESIGN.md` D114

`runtime.rs` computes the two sides of one cell's three-way comparison on consecutive lines. At
`1bf8abe` the three sites are, unchanged from when the row was recorded:

| site | function | scans | per |
|---|---|---|---|
| `runtime.rs:4380` | `evaluate_merge` (the `MERGE;` path) | all of `snapshot.ops` | changed cell |
| `runtime.rs:3792` | `merge_into` (sibling path) | all of `src.ops` | changed cell |
| `runtime.rs:3978` | `sibling_op` | all of `tgt.ops` | changed cell |

`theirs`, one line below the first of them, goes through `concurrent_op`, which **D86 indexed** to
a `partition_point` over a per-cell position list. `ours` was left linear.

### The pre-registered falsifier, quoted from the row

> D86's own harness with the axis set to **OPS PER BRANCH** rather than merges — nobody has varied
> that one. Hold changed cells FIXED, grow the branch's recorded op count, and measure merge cost.
> * If cost is **FLAT** in ops per branch, the reading is wrong and this row closes as a misreading
>   of the source, exactly as D68 did.
> * If cost is **LINEAR** in ops per branch at fixed delta, the row is real and the fix is D86's
>   index.

### AMENDMENT 1 (pre-measurement, append-only): one axis cannot name the mechanism

Recorded **before the harness was built**, from reading D86's own write-up.

The pre-registered axis can come back linear for **two different reasons**, and an index only
removes one of them:

| mechanism | what grows | does an index fix it? |
|---|---|---|
| the scan WALKS ops belonging to other cells | `examined` | yes — an index never looks at them |
| `compose_ops` FOLDS this cell's own history | `matched` | **no** — the fold must still see them |

This is not hypothetical: it is the trap D86 fell into and reported rather than shipped. Its
attempt 1 indexed by `(tbl, row, col)` and the drift fell only 1.60x -> 1.25x, because *"an agent
workload writes the SAME cells over and over"*. **Indexing divides by the cell count; it does not
change a complexity class.**

So the harness runs the pre-registered arm and a second arm that differ only in WHERE the ops sit:

* **`REPEAT`** — the literal pre-registered arm. `W` updates round-robin over the same 4 cells that
  form the delta. `examined = 4W`, `matched = W`. Both mechanisms live; this arm alone **cannot**
  say which one it measured.
* **`CHURN`** — 4 changed cells as before, but the bulk of the ops land on a different COLUMN of
  the same 4 rows, written back to its base value at the end. A cell whose final image equals its
  base is skipped by the changed-column loop, so those ops are examined and never matched:
  `examined = 4W`, `matched = 4`. Only the scan is live, so a slope here is the scan and nothing
  else.

Pre-registered reading of the pair:

| REPEAT | CHURN | verdict |
|---|---|---|
| flat | flat | **the pre-registered falsifier FIRES.** Close the row as a misreading |
| linear | flat | the slope is the FOLD. An index would NOT have fixed it — report that, do not fix |
| linear | linear | the SCAN is real; D86's index is the right fix |
| flat | linear | impossible as written (`CHURN` cost ⊂ `REPEAT` cost). The harness is wrong |

### AMENDMENT 2 (pre-measurement, append-only): what the row got wrong about `ops`

Read from source at `1bf8abe`, before measuring. **The row's stated mechanism is wrong in one
respect and it changes what the axis means.**

The row says `ops` is *"quadratic in the branch's OWN write count"* because *"`ops` grows with
total writes"*. But `snapshot.ops` is `ws.frame.ops`, and `frame` is a `TxnFrame` created fresh by
`BEGIN AGENT SESSION` (`runtime.rs:1519`) and destroyed by the `seal` that `MERGE` performs
(`runtime.rs:5434`, via `remove_workspace`). `TxnFrame::push_op` is append-only with no prune, but
the Vec it appends to **does not outlive one session**.

⇒ So this is **not** D86's axis wearing different clothes. D86's `State::applied` is process-wide
and never pruned, so its cost grows with *merges already done*. `frame.ops` resets every merge, so
this cost grows with **ops one agent recorded before it merged** — the length of a single agent's
task, not the age of the system. That is a real axis (an agent doing sustained work issues many
writes before merging) but it is a **smaller** claim than the row makes, and the row's "quadratic
in the branch's own writes" should be read as "linear in one session's op count, per changed cell".

### Instrument

`ours_scan_counters() -> (examined, matched)`, integers taken from inside the iterator that
actually walks the ops — not computed as `ops.len()` from outside, which would make the instrument
an assertion about the code rather than a reading of it. Counted per changed cell (4 relaxed adds
per merge) so the instrument cannot create the slope it measures.

**The integers are the finding. Every millisecond in this file is an UPPER BOUND on a shared box**
and carries the load average at the time the measurement lock was acquired.

### Controls, pre-registered

1. **A fresh server per axis point.** `State::applied` grows with merges done; walking the axis on
   one server makes merges-done rise *with* the axis, a confound pointing in exactly the direction
   of the hypothesis. A fresh server puts every point at the same `applied` length.
2. **Each arm walked ASCENDING and DESCENDING.** A monotonically drifting machine fakes a slope in
   one direction and an anti-slope in the other; a real slope survives both. If the two passes
   disagree on the shape, neither pass is a result.
3. **The writes phase is timed separately** and reported as a machine control, as in D86.
4. **A merge that did not reach the target is not counted** (`applied_to_target`, not `is_ok()`),
   and a point that collected nothing prints as a refusal, not as a zero.

### Bounds, so nothing here is over-read

* The axis is the **DELTA side, not the table**. This does not disturb D110's retirement: the merge
  stays O(delta · log N) in table size and `MERGE;` reads zero branch-engine pages. **Nothing here
  revives `merge3`.**
* `tel::engine::compose_row`'s similar wart is **dead-code-only** (`ThreeWayMerger` has no
  production caller) and is deliberately left alone.

### AMENDMENT 3 (pre-measurement, append-only): the ops axis cannot tell a constant from a class

Read from source at `1bf8abe`, before measuring, and it is the single most important correction to
the row's framing.

**The merge ALREADY pays O(W) in ops-per-session before any scan exists.** `WorkspaceSnapshot`
clones `ws.frame.ops` wholesale, once per merge (`runtime.rs:4043`, `ops: ws.frame.ops.clone()`);
the sibling path does the same (`:3659`, `:3667`). So the `ours`-side scan does not take the merge
from O(1) to O(W). It takes it from O(W) to **O(delta · W)** — and at the fixed delta of 4 the
pre-registration holds, that is a **4x constant factor, not a new complexity class.**

⇒ The pre-registered axis therefore comes back LINEAR whether or not the scan exists, because the
snapshot clone is linear on its own. That is a fourth way to be fooled on this code, and it would
have been indistinguishable from a win. The only axis that moves the left factor is the delta, so:

* **`DELTA` arm** — ops per branch held FIXED, changed cells grown. `examined = delta · W` is an
  exact integer, and the ratio `examined / (delta · ops)` is printed so the product claim can be
  checked rather than asserted. **Read the counter, not the milliseconds:** growing the delta grows
  the merge's genuine work too (more cells resolved, more ops applied, more rows published), so
  wall-clock rises in this arm whether or not the scan exists.

Pre-registered prediction for the fix, stated now so the after-curve has something to disagree
with: grouping the ops by cell ONCE per merge turns `delta · W` into `W + delta`. That removes the
delta factor and leaves the snapshot clone's O(W) untouched, so **the honest ceiling on this fix is
a factor of `delta`, not a complexity class** — and the wall-clock win will be smaller than the
counter ratio, because the clone stays.

### AMENDMENT 4 (pre-measurement, append-only): the row overstates the blast radius

The row says *"Three sites, both production merge paths"*. Checked by call graph at `1bf8abe`:

| site | function | reachable from SQL? |
|---|---|---|
| `runtime.rs:4380` | `evaluate_merge` | **yes** — `MERGE;` → `dispatch.rs:444` → `runtime.merge` |
| `runtime.rs:3792` | `merge_into` | no — `grep -rn 'merge_into' .` finds callers only in `tests/integration_sibling_merge.rs` |
| `runtime.rs:3978` | `sibling_op` | no — called only from `merge_into` (`runtime.rs:3854`) |

So it is **one production site, not three, and one API path, not two**: the second and third sites
are both inside `merge_into`, which is a `pub fn` on `AgentRuntime` with no SQL surface and no
non-test caller. That does not close the row — `:4380` is the `MERGE;` path and it is the one that
matters — but the fix's blast radius is one reachable path plus a public API that only tests
currently exercise. This is the same shape D110 already recorded when a brief called `merge_into`
*"the real merge path"*.

### AMENDMENT 5 (pre-measurement, append-only): a larger quadratic sits next door, on the WRITE path

Noticed while confirming that an agent `UPDATE` reaches `ws.frame.ops`. `stage_all` ends each
statement with `ws.frame.clone()` and `self.log.append(&frame)` (`runtime.rs:2924-2929`) — **the
whole frame, cloned and re-appended once per statement.** A session issuing W statements therefore
copies 1 + 2 + ... + W ops, which is **O(W²) on the write path**, against the merge scan's
O(delta · W) once at the end.

It is deliberate — the comment explains that re-appending replaces rather than double-counts, since
`Add` is not idempotent — but the cost is quadratic and it is not a recorded row (`grep` of
`SCALE-DESIGN.md` for it returns nothing). This harness times the writes phase separately, so the
curve comes out of the same run. **Consequence for this measurement: the writes-phase column is NOT
a machine control across points on the ops axis, only within one point.** The machine control for
this run is the ascending/descending pair.
