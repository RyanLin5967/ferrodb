# D115 — CLOSED. The frame clone is a real quadratic, and it is not the cost.

> ## ⛔ STOP — if you arrived here because you read `ws.frame.clone()` in `stage_all`
>
> You read that `runtime.rs` deep-copies the whole `TxnFrame` once per statement, that the frame
> accumulates every op the agent session has recorded, and that `W` statements therefore copy
> `W(W+1)/2` ops. **That reading is exactly right. An exact counter confirms it to the unit:
> 33,558,528 ops copied at W=8192, 536,887,296 at W=32768 — `W(W+1)/2` with no remainder.**
>
> **It is also 3.2% of the statement path on the configuration ferrodb ships, and the writes phase
> on that configuration is already LINEAR (exponent 1.01) before anything is changed.** The cost is
> one fsync per statement in `DurableEffectLog::append`, which is 92.6% of `stage_all` and which
> that store's own comment describes as the deliberate price of its guarantee.
>
> **Do not "fix" the clone.** Removing it buys ~6% at 8192 ops. On the shipped store the clone does
> not overtake the per-statement fsync until **~122,000 ops in a single agent session**.
>
> ⛔ **And the 2.25 s/session the row quotes is from a runtime with no page store and an in-memory
> effect log** — the D101 stub-runtime defect. The same session on the shipped configuration costs
> **36.0 s**, and 92.6% of it is somewhere else.

Measured at `8249c50` with the `tel::stage_probe` counters added and **no other change**. No fix
was written, deliberately. Raw runs, both directions, in `bench/d115_before.txt`,
`bench/d115_real_mem.txt`, `bench/d115_real_durable.txt`, `bench/d115_crossover.txt`;
`bench/d115_run4.sh` is the driver.

**Suite:** the instrument commit is `a19e881` on `D115-frame-clone`; `cargo test --release --lib`
there reports **`ok. 1599 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out`**, `rc=0`,
`head=a19e881` (`bench/d115_verify_full.sh`). Nothing here is filtered: the instrument adds
counters, makes the cloned frame's `drop` explicit, and splits `stage_all` into a timing wrapper
and a body — no behaviour change, and the whole suite says so at the commit that carries it.

---

## 1. THE VERDICT AGAINST THE PRE-REGISTERED FALSIFIER

The falsifier was:

> the exponent must fall from ~1.95 toward 1.0 on the same ops axis with delta fixed.
> **Flat or unchanged ⇒ the clone is not the cause, close the row and say so plainly.**

**On the configuration ferrodb ships, the exponent is 1.01 ASCENDING and 1.07 DESCENDING before
any fix exists.** It is already 1.0. The 1.95 the falsifier takes as its baseline occurs only in a
configuration with no page store and no durable effect log. **Flat ⇒ close the row.** Closed.

⚠ **And the falsifier could not have discriminated even where the 1.95 is real.** The exponent is
not a discriminating statistic here, because **three terms share one axis**: the clone, the `drop`
of the clone, and `extends()`'s prefix comparison. Removing any one of them divides the constant
and leaves the exponent where it was. A "fix" that removed `ws.frame.clone()` alone would have
scored *unchanged exponent* → *"the clone is not the cause"* → the right conclusion by an argument
that does not hold. Same family as D114's own amendment: *the pre-registered axis returns LINEAR
whether or not the scan exists.*

---

## 2. THE BEFORE-CURVE, THREE CONFIGURATIONS, BOTH DIRECTIONS

Delta fixed at 32, table 2000 rows, `load_at_acquire=2–4`. **Every millisecond is an UPPER BOUND**
on a box running a build fleet; the integer columns are the claim.

### A — the row's own configuration (D101 stub runtime: `storage: None`, `MemEffectLog`)

`bench/d115_before.txt`, 3 reps/point. This reproduces the row's numbers, which is the first thing
a before-curve owes.

| ops | writes ms ASC | exp | writes ms DESC | exp | row's quoted ms |
|---|---|---|---|---|---|
| 32 | 0.446 | — | 0.369 | — | 0.450 |
| 128 | 1.982 | 1.08 | 1.852 | 1.16 | 2.056 |
| 512 | 17.182 | 1.56 | 16.237 | 1.57 | 11.990 |
| 2048 | 163.357 | 1.62 | 163.082 | 1.66 | 150.952 |
| 8192 | 2353.566 | **1.92** | 2348.340 | **1.92** | 2247.207 |

Share of `stage_all` at 8192 ops: **clone 41.2% · drop 39.5% · append 19.1% · apply 0.1% ·
decide 0.0% · mirror 0.0%**. `mirror_rows = 0` — see §4.

### B — the runtime the harness actually configures (arena page store), `MemEffectLog`

`bench/d115_real_mem.txt`, 3 reps/point.

| ops | writes ms ASC | exp | mirror % | clone % | drop % | append % |
|---|---|---|---|---|---|---|
| 32 | 18.279 | — | **99.1** | 0.2 | 0.1 | 0.1 |
| 128 | 29.738 | 0.35 | 95.3 | 1.6 | 1.1 | 0.6 |
| 512 | 59.029 | 0.49 | 80.5 | 8.0 | 6.0 | 3.3 |
| 2048 | 259.820 | 1.07 | 40.3 | 24.7 | 22.9 | 10.4 |
| 8192 | 2678.147 | 1.68 | 12.5 | 36.6 | 34.9 | 15.4 |

Descending agrees within 1% at every point above 128 (18.3/24.4, 29.7/29.5, 59.0/58.2,
259.8/258.3, 2678/2690).

### C — ⭐ THE SHIPPED CONFIGURATION (arena page store + `DurableEffectLog`, `cli.rs:141`)

`bench/d115_real_durable.txt`, 1 rep/point.

| ops | writes ms ASC | exp | **append %** | clone % | drop % | mirror % |
|---|---|---|---|---|---|---|
| 32 | 174.965 | — | 84.0 | 0.0 | 0.0 | 15.8 |
| 128 | 556.230 | 0.83 | 94.2 | 0.1 | 0.1 | 5.4 |
| 512 | 2265.038 | **1.01** | 97.2 | 0.3 | 0.2 | 2.2 |
| 2048 | 8894.200 | **0.99** | 96.4 | 1.0 | 0.9 | 1.6 |
| 8192 | 36053.493 | **1.01** | **92.6** | 3.2 | 3.0 | 1.1 |

Descending: exponents 0.75 / 1.01 / 1.07 / 1.07; append 82.7 → 90.9%; clone 0.3 → 4.0%.

### D — past the crossover, `MemEffectLog`

`bench/d115_crossover.txt`. Once the linear term is left behind the quadratic is unmistakable.

| ops | writes ms ASC | exp | clone % | drop % | mirror % |
|---|---|---|---|---|---|
| 8192 | 2607.703 | — | 36.3 | 35.1 | 12.9 |
| 32768 | 37559.005 | **1.92** | 40.9 | 38.5 | **3.3** |

### The ASC/DESC contamination control

D114 saw one point read 36.121 ms ascending and 9.798 ms descending on this box. Nothing like it
here: the ratio is **1.00–1.01 at the two largest points of every configuration**. The only
disagreements are at 32 ops (0.75–1.21), on sub-millisecond readings where the clock floor and one
arena warm-up dominate. The curves are clean.

---

## 3. WHAT DOMINATES, AND WHY THE ROW NAMED THE WRONG THING

### First — the instrument was forced to fire, so a small reading means something

A phase timer that reports "the clone is 3.2%" is worthless unless it can be shown reading the
clone LARGE when the clone IS large. **The same binary, the same spans, the same run:**

| configuration | clone + drop, share of `stage_all` @8192 |
|---|---|
| A — stub runtime, `MemEffectLog` | **80.7%** |
| B — arena store, `MemEffectLog` | **71.5%** |
| C — **shipped**, arena + `DurableEffectLog` | **6.2%** |
| D — `MemEffectLog` @32768 ops | **79.4%** |

The detector pegs at 80% in three configurations and reads 6.2% in the fourth. The 6.2% is a fact
about the configuration, not a blind instrument. The phase table also has to close — `sum/stage`
reads **100.0%** at the top of every arm — so there is no unattributed remainder for the term to
be hiding in.

**On the shipped store: `DurableEffectLog::append`, at 4.057 ms per statement** — 33.24 s of a
36.05 s session at 8192 ops.

That number is I/O, not comparison, and the bound is derived from the measurements rather than
assumed. The durable store runs the same O(n) frame comparison **twice** per statement — once in
`classify_append` and once inside `self.mem.append` — and the counter confirms it exactly:
`extends_cmp` is 67,100,672 on the durable arm against 33,550,336 on the mem arm, precisely 2×.
Configuration B prices one such pass, with the position scan and the tail copy, at 399.9 ms per
session. So the comparisons account for **at most 800 ms of the 33,236 ms**, i.e. **≤2.4%**;
**≥97.6% is encode + `pwrite` + `sync_data`**. `write_record`'s own comment states the design:
*"Synchronous on every append, deliberately … one fsync per statement of an agent task, which is
the price of the guarantee rather than an oversight."*

**Second: the CoW mirror**, `put_row` → `set_root` → `TableBranchCatalog::durable` → `flush_all` +
`disk_manager.sync()`, and `DiskManager::sync` is `storage.sync_all()` — a real fsync per
statement. It is 99.1% of `stage_all` at 32 ops in configuration B, at **0.554 ms/statement**, and
falls to **0.040 ms/statement** by 8192.

⚠ **The reason for that 14x decay is NOT established here.** Group commit and page-cache warmth
are both plausible and neither was checked, so no mechanism is claimed for it. What the number
supports is only the magnitude: past a few hundred statements this term is tens of microseconds,
not hundreds.

**The clone, its drop, and `extends` are the third group**, together 6.2% (ASC) / 7.6% (DESC) of
`stage_all` at 8192 ops on the shipped store.

### Where the quadratic does win

From the measured constants — clone+drop at **66.2 ns per op copied**, the durable append at
**4.057 ms per statement** — the quadratic overtakes the fsync at

> **W ≈ 122,600 ops in one agent session.**

The same arithmetic on configuration B predicts a crossover at **W ≈ 1,198**, and the measured
curve crosses between 512 (mirror 80.5%) and 2048 (mirror 40.3%). **The prediction was made from
the 8192-point constants and checked against points it was not fitted to.**

---

## 4. ⛔ WHAT THE ROW'S FRAMING GOT WRONG — FOUR THINGS

**(1) The numbers were produced on a runtime the harness built and never used.** `Session::new()`
constructs its OWN `AgentRuntime::new()` — `storage: None`, its own in-memory branch catalog, its
own `MemEffectLog`. D114's harness (and mine, until this was caught) builds an `ArenaPageStore`, a
`TableBranchCatalog` sidecar and `AgentRuntime::with_storage`, hands them to a `ServerContext`,
and then runs every statement on `Session::new()`. The real server uses
`Session::with_runtime` (`pgwire/mod.rs:358`); so does the CLI (`cli.rs`).

⚠ **This is the project's OPEN ROW D101, not a new finding.** Its `agent_sql::designated` guard —
which refuses exactly this — exists on `D101-stub-runtime-guard` and **is not on main**, which is
why both harnesses slipped through. D101's own doc names D55, D56, D67 and D68 as the affected
benchmarks; **D114/D115 is a fifth, measured after the guard was written and before it landed.**

⭐ **The instrument that caught it was an integer, not a timer.** `mirror_rows` must be non-zero
when a page store is attached; it read 0 at every point of the axis. The timer showed `mirror` at
0.000 ms, which reads as *"the mirror is cheap"*. The harness now refuses a run that mirrors zero
rows.

⚠ **A null control confirms it, and the proof is an INTEGER rather than a timing argument.** An
arm configured with `DurableEffectLog` (`bench/d115_before_durable.txt`) reported
`extends_cmp = 33,550,336` at 8192 ops — **one** frame-comparison pass. The genuine durable path
provably runs **two** (67,100,672, measured in §3, because `classify_append` and `self.mem.append`
each run one). A store that is actually on the path cannot report the mem store's comparison
count. `mirror_rows = 0` in the same run says the same thing about the page store.

> ⛔ **Withdrawn, and logged here because this is where the claim was made.** An earlier draft of
> this paragraph argued instead that *"8192 fsyncs cannot happen in 3.6 s"*. **That argument does
> not hold and is retracted.** The CoW mirror's own `set_root` reaches a real `sync_all()` and
> still measures 0.040 ms/statement at 8192 ops on this box, so a sub-millisecond sync is exactly
> what this measurement cannot rule out. The integer above does not depend on it.

**(2) The clone is three terms, and the row names the one that is not even the largest pair.**
Every statement pays, with `n` = ops recorded so far:

| # | term | where | share of `stage_all` @8192, cfg B | in the row? |
|---|---|---|---|---|
| 1 | `ws.frame.clone()` | `runtime.rs` | 36.6% | ✅ |
| 2 | **dropping that clone** | `runtime.rs`, implicit | **34.9%** | ⛔ no |
| 3 | `extends()`'s ops prefix | `tel/log.rs` | inside append's 15.4% | ⛔ no |

Term 2 is not a subtlety: `Vec<Op>` runs a destructor per element, so a deep copy costs a deep
free, and the free lands after every span an obvious instrumentation would draw. **The first cut
of this probe timed five phases and left 40% of `stage_all` unattributed at every point.** The
`sum/stage` column exists so the phase table has to close; it now reads 100.0%.

**(3) A fourth O(n) term exists that no configuration here exercises, and it should be written
down before someone meets it.** `frame_eq()` compares `a.guards == b.guards` **before** it reaches
the op-count short-circuit, and `Vec::eq` only short-circuits on LENGTH. A statement that pushes a
guard costs nothing there; **a statement that pushes NONE compares every guard the session has
accumulated**, each a recursive `GuardExpr` walk. `eq_guard_cmp` is 0 in every run above because a
single-row `UPDATE ... WHERE id = ?` always captures a guard. An INSERT-heavy session mixed into a
guard-heavy one would light it up.

**(4) `position()` in `MemEffectLog::append` is linear in FRAMES, not ops — a different wall on
the 10⁶ axis.** `pos_scan` rose from 31/session to 63/session purely because configuration B
shares one runtime across cycles while the stub made a fresh one per cycle. In a long-lived
process the log holds one frame per session, and `DurableEffectLog` replays the whole file at open,
so this is O(sessions ever run) per statement. Not measured here — it is not the D115 axis — but it
is the term that scales with the objective rather than with task length.

---

## 5. WHAT SURVIVES OF THE ROW

The quadratic is **real, exact, and correctly located**. `clone_ops` is `W(W+1)/2` to the unit at
every point, in every configuration, in both directions. The comment at the call site is right
about why the frame is re-presented whole (`Add` is not idempotent, so re-appending must REPLACE),
and any future change must keep that.

What does not survive is the magnitude and the priority. On the shipped configuration it is 3.2%
of a path whose cost is two fsyncs per statement, it is already linear on the reachable axis, and
it does not become the leading term until an agent session runs ~122,000 statements.

⭐ **This is the fourth time a mechanism proposed for a slope in `runtime.rs` has been read
correctly off the source and turned out not to be the cost (D68, D69-REOPEN, D114, D115).** The
pattern across all four is the same: the shape is right, the magnitude is never checked, and only
a measurement separates them. The cost this time was one before-curve instead of a fix plus a
retraction.

⚠ **Residue worth keeping.** The clone/drop/extends trio is a genuine O(n²); the before-curve is
banked and re-runnable. If the per-statement fsyncs are ever batched — group commit across
statements rather than across writers, which is the obvious next thing anyone will try — the
linear term collapses and **this becomes the leading cost immediately**. Whoever does that work
should re-run `bench/d115_run4.sh` on the same day, not read this table.
