# D114 — is the `ours`-side cell scan a real slope, or a misreading of the source?

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
